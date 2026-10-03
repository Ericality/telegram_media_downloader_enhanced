"""Download worker — queue consumers, producers and retry logic."""
import asyncio
import os
import shutil
import time
from typing import List, Optional, Tuple, Union

import pyrogram
from loguru import logger
from pyrogram.types import Audio, Document, Photo, Video, VideoNote, Voice

import core.context as ctx
from core.context import RETRY_TIME_OUT, app, queue_manager
from core.models import ChatDownloadConfig, DownloadStatus, TaskNode
from core.storage import load_failed_tasks, record_failed_task, remove_failed_task
from module.get_chat_history_v2 import get_chat_history_v2
from module.language import _t
from module.pyrogram_extension import get_extension, set_meta_data, upload_telegram_chat
from services.notifier import send_bark_notification
from utils.format import truncate_filename, validate_title
from utils.meta_data import MetaData
from workers.monitor import (
    CLOUD_SOURCE_CACHE,
    check_disk_space,
    cloud_space_check_enabled,
    disk_monitor,
    format_age,
    resolve_cloud_space,
    sleep_with_exit_check,
)

# 生产者调度配置的默认值（小时 / 分钟）——与 module/app.py 的 BASE_CONFIG 保持一致
SCHEDULER_CONFIG_DEFAULTS = {
    "chat_max_continuous_hours": 72.0,
    "chat_recheck_interval_minutes": 30.0,
}


def _config_seconds(key: str, multiplier: float) -> float:
    """读取生产者调度用的时间配置并换算成秒。

    ``<= 0`` 或缺失/非法一律返回 ``0.0``（调用方视为「关闭」）。
    """
    default = SCHEDULER_CONFIG_DEFAULTS.get(key, 0.0)
    raw = app.get_config(key, default)
    try:
        value = float(raw)
    except (TypeError, ValueError):
        logger.warning(f"配置 {key}={raw!r} 非法，改用默认值 {default}")
        value = default
    if value <= 0:
        return 0.0
    return value * multiplier


def _check_download_finish(media_size: int, download_path: str, ui_file_name: str):
    """Verify download completeness by comparing file sizes."""
    download_size = os.path.getsize(download_path)
    if media_size == download_size:
        logger.success(f"{_t('Successfully downloaded')} - {ui_file_name}")
    else:
        logger.warning(
            f"{_t('Media downloaded with wrong size')}: "
            f"{download_size}, {_t('actual')}: "
            f"{media_size}, {_t('file name')}: {ui_file_name}"
        )
        os.remove(download_path)
        raise pyrogram.errors.exceptions.bad_request_400.BadRequest()


def _move_to_download_path(temp_download_path: str, download_path: str):
    """Move file from temp path to final download path."""
    directory, _ = os.path.split(download_path)
    os.makedirs(directory, exist_ok=True)
    shutil.move(temp_download_path, download_path)


def _check_timeout(retry: int, _: int):
    """Check if download has exceeded retry limit."""
    return retry == 2


def _can_download(_type: str, file_formats: dict, file_format: Optional[str]) -> bool:
    """Check if a given file format is allowed for download."""
    if _type in ["audio", "document", "video"]:
        allowed_formats: list = file_formats[_type]
        if not file_format in allowed_formats and allowed_formats[0] != "all":
            return False
    return True


def _is_exist(file_path: str) -> bool:
    """Check if file exists and is not a directory."""
    return not os.path.isdir(file_path) and os.path.exists(file_path)


async def _get_media_meta(
    chat_id: Union[int, str],
    message: pyrogram.types.Message,
    media_obj: Union[Audio, Document, Photo, Video, VideoNote, Voice],
    _type: str,
) -> Tuple[str, str, Optional[str]]:
    """Extract filename and file ID from a media object."""
    if _type in ["audio", "document", "video"]:
        file_format: Optional[str] = media_obj.mime_type.split("/")[-1]
    else:
        file_format = None

    file_name = None
    temp_file_name = None
    dirname = validate_title(f"{chat_id}")
    if message.chat and message.chat.title:
        dirname = validate_title(f"{message.chat.title}")

    if message.date:
        datetime_dir_name = message.date.strftime(app.get_config("date_format"))
    else:
        datetime_dir_name = "0"

    if _type in ["voice", "video_note"]:
        file_format = media_obj.mime_type.split("/")[-1]
        file_save_path = app.get_file_save_path(_type, dirname, datetime_dir_name)
        file_name = "{} - {}_{}.{}".format(
            message.id,
            _type,
            media_obj.date.isoformat(),
            file_format,
        )
        file_name = validate_title(file_name)
        temp_file_name = os.path.join(
            app.get_config("temp_save_path"), dirname, file_name
        )
        file_name = os.path.join(file_save_path, file_name)
    else:
        file_name = getattr(media_obj, "file_name", None)
        caption = getattr(message, "caption", None)

        file_name_suffix = ".unknown"
        if not file_name:
            file_name_suffix = get_extension(
                media_obj.file_id, getattr(media_obj, "mime_type", "")
            )
        else:
            _, file_name_without_suffix = os.path.split(os.path.normpath(file_name))
            file_name, file_name_suffix = os.path.splitext(file_name_without_suffix)
            if not file_name_suffix:
                file_name_suffix = get_extension(
                    media_obj.file_id, getattr(media_obj, "mime_type", "")
                )

        if caption:
            caption = validate_title(caption)
            app.set_caption_name(chat_id, message.media_group_id, caption)
            app.set_caption_entities(
                chat_id, message.media_group_id, message.caption_entities
            )
        else:
            caption = app.get_caption_name(chat_id, message.media_group_id)

        if not file_name and message.photo:
            file_name = f"{message.photo.file_unique_id}"

        gen_file_name = (
            app.get_file_name(message.id, file_name, caption) + file_name_suffix
        )

        file_save_path = app.get_file_save_path(_type, dirname, datetime_dir_name)
        temp_file_name = os.path.join(
            app.get_config("temp_save_path"), dirname, gen_file_name
        )
        file_name = os.path.join(file_save_path, gen_file_name)

    return truncate_filename(file_name), truncate_filename(temp_file_name), file_format


async def add_download_task(
    message: pyrogram.types.Message,
    node: TaskNode,
    is_retry: bool = False,
) -> bool:
    """Add download task to queue — blocks until a free slot is available."""
    if message.empty:
        return False

    if getattr(app, "force_exit", False) or not getattr(app, "is_running", True):
        logger.debug(f"程序正在退出，跳过添加任务: message_id={message.id}")
        return False

    try:
        # Block on queue.put() until a worker takes a task (natural backpressure)
        put_start = time.time()
        await ctx.download_queue.put((message, node))
        wait_seconds = time.time() - put_start

        async with queue_manager.lock:
            node.download_status[message.id] = DownloadStatus.Downloading
            node.total_task += 1
            queue_manager.task_added += 1

            if not is_retry:
                chat_id_str = str(node.chat_id)
                chat_config = app.chat_download_config.get(
                    node.chat_id
                ) or app.chat_download_config.get(chat_id_str)
                if chat_config:
                    try:
                        message_id_int = int(message.id)
                        current_last_id = getattr(
                            chat_config, "last_read_message_id", 0
                        )
                        if current_last_id is None:
                            current_last_id = 0
                        current_last_id = int(current_last_id)
                        if message_id_int > current_last_id:
                            chat_config.last_read_message_id = message_id_int
                            logger.debug(
                                f"更新聊天 {node.chat_id} 的 last_read_message_id 到 {message_id_int}"
                            )
                            app.update_config(immediate=True)
                    except (ValueError, TypeError) as e:
                        logger.error(f"更新 last_read_message_id 时出错: {e}")

        await remove_failed_task(node.chat_id, message.id)

        if wait_seconds > 60:
            logger.warning(f"任务添加等待 {int(wait_seconds)} 秒: message_id={message.id}")
        logger.debug(
            f"[{'RETRY' if is_retry else 'NEW'}] 已添加下载任务: message_id={message.id}, 队列大小={ctx.download_queue.qsize()}"
        )
        return True

    except asyncio.CancelledError:
        logger.info(f"添加任务被取消: message_id={message.id}")
        await record_failed_task(node.chat_id, message.id, "添加任务被取消")
        return False
    except Exception as e:
        logger.error(f"添加下载任务异常: {e}")
        await record_failed_task(node.chat_id, message.id, f"添加异常: {e}")
        return False


async def retry_producer(client: pyrogram.Client):
    """Global retry producer: scans all chats for failed tasks and retries them."""
    retry_ratio = 4
    new_task_count = 0

    while getattr(app, "is_running", True) and not getattr(app, "force_exit", False):
        try:
            if ctx.download_queue.qsize() >= queue_manager.download_queue_size:
                await asyncio.sleep(1)
                continue

            if new_task_count < retry_ratio:
                new_task_count += 1
                await asyncio.sleep(0.5)
                continue

            # Round-robin through all chats to find a failed task
            retried = False
            for chat_id, chat_config in list(app.chat_download_config.items()):
                if not chat_config.node:
                    continue
                failed_tasks = await load_failed_tasks(chat_id)
                if not failed_tasks:
                    continue

                task = failed_tasks[0]
                msg_id = task["message_id"]
                try:
                    msg = await client.get_messages(chat_id, msg_id)
                    if msg is not None:
                        success = await add_download_task(
                            msg, chat_config.node, is_retry=True
                        )
                        if success:
                            await remove_failed_task(chat_id, msg_id)
                            logger.info(f"重试生产者: 为聊天 {chat_id} 添加重试任务 {msg_id}")
                            new_task_count = 0
                            retried = True
                            break
                        else:
                            logger.debug(f"重试生产者: 添加重试任务 {msg_id} 失败")
                    else:
                        await remove_failed_task(chat_id, msg_id)
                        logger.warning(f"重试生产者: 消息 {msg_id} 已不存在")
                except Exception as e:
                    logger.error(f"重试生产者: 获取消息 {msg_id} 失败: {e}")

            if not retried:
                new_task_count = 0
            await asyncio.sleep(1)
        except asyncio.CancelledError:
            logger.debug("重试生产者被取消")
            break
        except Exception as e:
            logger.error(f"重试生产者异常: {e}")
            await asyncio.sleep(5)
    logger.info("重试生产者退出")


async def add_download_task_batch(
    messages: List[pyrogram.types.Message],
    node: TaskNode,
) -> int:
    """Batch add download tasks sequentially, respecting queue capacity."""
    # Check if program is still running
    if not getattr(app, "is_running", True) or getattr(app, "force_exit", False):
        logger.debug("程序不在运行状态，跳过批量添加")
        for msg in messages:
            if msg:
                await record_failed_task(node.chat_id, msg.id, "程序退出，批量添加被跳过")
        return 0

    if not messages:
        return 0

    added_count = 0
    # Add tasks sequentially; add_download_task blocks until space available
    for msg in messages:
        if msg is None:
            continue
        try:
            success = await add_download_task(msg, node)
            if success:
                added_count += 1
        except Exception as e:
            logger.error(f"添加任务失败: message_id={msg.id}, 错误: {e}")
            await record_failed_task(node.chat_id, msg.id, f"批量添加异常: {e}")

    if added_count < len(messages):
        logger.warning(f"批量添加完成: 成功 {added_count} 个，失败 {len(messages) - added_count} 个")
    else:
        logger.info(f"批量添加完成: 成功添加 {added_count} 个任务")
    return added_count


async def download_worker(client: pyrogram.client.Client, worker_id: int):
    """Download task worker."""
    from services.downloader import download_task

    logger.debug(f"下载Worker {worker_id} 启动")

    while True:
        # Check forced exit signal
        if getattr(app, "force_exit", False) or not getattr(app, "is_running", True):
            logger.debug(f"下载Worker {worker_id} 收到退出信号，准备退出")
            break

        try:
            # Check cloud upload health before disk check
            if not getattr(app, "force_exit", False):
                if not ctx.cloud_upload_ok:
                    if worker_id not in disk_monitor.paused_workers:
                        logger.warning(f"下载Worker {worker_id}: 云端上传验证失败，暂停下载")
                        disk_monitor.paused_workers.add(worker_id)
                    if not getattr(app, "force_exit", False):
                        await asyncio.sleep(60)
                        continue
                    else:
                        break
                else:
                    if worker_id in disk_monitor.paused_workers:
                        logger.info(f"下载Worker {worker_id}: 云端连接恢复，继续下载")
                        disk_monitor.paused_workers.discard(worker_id)

            # Check cloud storage free space (rclone only; unknown -> fail-open, keep downloading)
            if not getattr(app, "force_exit", False):
                cloud_cfg = getattr(app, "cloud_drive_config", None)
                cloud_threshold = getattr(cloud_cfg, "cloud_space_threshold_gb", 10.0)
                cloud_check_enabled = cloud_space_check_enabled(cloud_cfg)
                logger.debug(
                    f"下载Worker {worker_id}: 云端空间检查启用={cloud_check_enabled} "
                    f"(enable_upload_file={getattr(cloud_cfg, 'enable_upload_file', None)!r}, "
                    f"adapter={getattr(cloud_cfg, 'upload_adapter', None)!r}, "
                    f"threshold={cloud_threshold}GB)"
                )
                if cloud_check_enabled:
                    # 查询失败时沿用上次成功结果（仅上传启用时参与判定）
                    (
                        cloud_ok,
                        cloud_free_gb,
                        _,
                        cloud_source,
                        cloud_age,
                    ) = await resolve_cloud_space(cloud_cfg, float(cloud_threshold))

                    if cloud_ok is False:
                        if worker_id not in disk_monitor.paused_workers:
                            source_hint = (
                                f"（查询失败，沿用 {format_age(cloud_age)}前的查询结果）"
                                if cloud_source == CLOUD_SOURCE_CACHE
                                else ""
                            )
                            logger.warning(
                                f"下载Worker {worker_id}: 云端空间不足 "
                                f"({cloud_free_gb}GB < {cloud_threshold}GB)"
                                f"{source_hint}，暂停下载"
                            )
                            disk_monitor.paused_workers.add(worker_id)

                            bark_config = getattr(app, "bark_notification", {})
                            events_to_notify = bark_config.get("events_to_notify", [])
                            if "task_paused" in events_to_notify:
                                message = (
                                    f"Worker {worker_id}: 因云端空间不足暂停下载\n"
                                    f"云端可用空间: {cloud_free_gb}GB{source_hint}"
                                )
                                await send_bark_notification("下载任务暂停", message)

                        if not getattr(app, "force_exit", False):
                            await asyncio.sleep(60)
                            continue
                        else:
                            break
                    elif cloud_ok is True:
                        if worker_id in disk_monitor.paused_workers:
                            logger.info(f"下载Worker {worker_id}: 云端空间恢复，继续下载")
                            disk_monitor.paused_workers.discard(worker_id)
                    else:
                        # cloud_ok is None: 查询失败且无历史结果，fail-open，不暂停
                        logger.warning(
                            f"下载Worker {worker_id}: 云端空间查询失败且无历史结果(fail-open)，"
                            f"本次不因云端空间暂停；将按本地磁盘空间继续判定"
                        )

            # Check disk space (skip if exiting)
            if not getattr(app, "force_exit", False):
                bark_config = getattr(app, "bark_notification", {})
                threshold_gb = bark_config.get("disk_space_threshold_gb", 10.0)

                has_space, available_gb, _ = await check_disk_space(threshold_gb)

                if not has_space:
                    if worker_id not in disk_monitor.paused_workers:
                        logger.warning(
                            f"下载Worker {worker_id}: 磁盘空间不足 ({available_gb}GB < {threshold_gb}GB)，暂停下载"
                        )
                        disk_monitor.paused_workers.add(worker_id)

                        events_to_notify = bark_config.get("events_to_notify", [])
                        if "task_paused" in events_to_notify:
                            message = f"Worker {worker_id}: 因磁盘空间不足暂停下载\n可用空间: {available_gb}GB"
                            await send_bark_notification("下载任务暂停", message)

                    # Keep paused state if program is exiting
                    if not getattr(app, "force_exit", False):
                        await asyncio.sleep(60)
                        continue
                    else:
                        # Exiting, break out of loop
                        break
                else:
                    if worker_id in disk_monitor.paused_workers:
                        logger.info(f"下载Worker {worker_id}: 磁盘空间恢复，继续下载")
                        disk_monitor.paused_workers.discard(worker_id)
        except Exception as e:
            logger.error(f"下载Worker {worker_id} 检查磁盘空间时异常: {e}")
            if not getattr(app, "force_exit", False):
                await asyncio.sleep(60)
            continue

        try:
            # Use timed get to avoid indefinite blocking
            try:
                message, node = await asyncio.wait_for(
                    ctx.download_queue.get(), timeout=1.0
                )
            except asyncio.TimeoutError:
                continue

            # Re-check exit signal before processing
            if getattr(app, "force_exit", False) or not getattr(
                app, "is_running", True
            ):
                logger.debug(f"下载Worker {worker_id} 收到退出信号，将任务放回队列")
                await ctx.download_queue.put((message, node))  # Return task to queue
                ctx.download_queue.task_done()
                break

            if node.is_stop_transmission:
                ctx.download_queue.task_done()
                continue

            # Log task start; individual step logging is suppressed
            logger.debug(f"下载Worker {worker_id} 处理消息 {message.id}")

            try:
                # Semaphore limits actual concurrent downloads to max_download_task
                async with ctx.download_semaphore:
                    if node.client:
                        await download_task(node.client, message, node)
                    else:
                        await download_task(client, message, node)

                # Task completed
                logger.debug(f"下载Worker {worker_id} 完成消息 {message.id}")
            except asyncio.CancelledError:
                logger.info(f"下载Worker {worker_id} 被取消，将消息 {message.id} 放回队列")
                await ctx.download_queue.put((message, node))  # Return task to queue
                raise
            except OSError as e:
                logger.error(f"下载Worker {worker_id}: 消息 {message.id} 网络连接错误: {e}")
                retry_count = await record_failed_task(
                    node.chat_id, message.id, f"Network error: {str(e)}"
                )
                logger.warning(
                    f"Message {message.id} network error, recorded to failed list (retry count: {retry_count})"
                )
            except Exception as e:
                logger.error(f"下载Worker {worker_id}: 消息 {message.id} 下载任务异常: {e}")
                retry_count = await record_failed_task(
                    node.chat_id, message.id, f"Download exception: {str(e)}"
                )
                logger.warning(
                    f"Message {message.id} download exception, recorded to failed list (retry count: {retry_count})"
                )
            finally:
                ctx.download_queue.task_done()

        except asyncio.CancelledError:
            logger.debug(f"下载Worker {worker_id} 被取消")
            break
        except Exception as e:
            logger.error(f"下载Worker {worker_id} 异常: {e}")
            await asyncio.sleep(1)

    logger.debug(f"下载Worker {worker_id} 退出")


async def download_chat_task(
    client: pyrogram.Client,
    chat_id: Union[int, str],
    chat_download_config: ChatDownloadConfig,
    node: TaskNode,
    max_seconds: float = None,
) -> bool:
    """Producer: feed new messages to download queue one-by-one.

    Uses add_download_task() which blocks on queue.put() when full,
    creating natural backpressure — producer waits for workers to free slots.

    Args:
        max_seconds: 单个会话连续下载的时间片上限（秒）。到点就停止本轮迭代、
            让位给其它会话；下一轮会从 ``last_read_message_id`` 续传（该值逐条
            即时落盘 ⇒ 不会漏也不会重）。``None``/``<=0`` = 不限制（旧行为）。

    Returns:
        ``True`` = 本轮被时间片截断（该会话还有剩余消息，下一轮要继续）；
        ``False`` = 已走到最新消息（本轮走完）。
    """
    timed_out = False
    started_at = time.time()
    added_before = node.total_task
    try:
        logger.info(
            f"开始处理聊天 {chat_id}，last_read_message_id={chat_download_config.last_read_message_id}"
            + (
                f"（时间片上限 {max_seconds / 3600:.2f} 小时）"
                if max_seconds and max_seconds > 0
                else ""
            )
        )

        messages_iter = get_chat_history_v2(
            client,
            chat_id,
            limit=node.limit,
            max_id=node.end_offset_id,
            offset_id=chat_download_config.last_read_message_id,
            reverse=True,
        )

        chat_download_config.node = node

        async for message in messages_iter:
            logger.debug(f"处理消息 {message.id}")

            # 时间片到点：停止本轮迭代，让位给其它会话（下一轮从此处续传）
            if (
                max_seconds
                and max_seconds > 0
                and time.time() - started_at >= max_seconds
            ):
                timed_out = True
                # 注意：这里必须打**实际耗时**，不能打时间片上限 ——
                # 队列背压会让"实际让位时刻"晚于上限（实测 36 秒的上限被拖到 4 分钟以上），
                # 只打上限会让人误以为逻辑算错（2026-10-03 实测踩到）。
                elapsed_seconds = time.time() - started_at
                logger.warning(
                    f"聊天 {chat_id} 本轮已连续处理 {elapsed_seconds / 3600:.2f} 小时"
                    f"（时间片上限 {max_seconds / 3600:.2f} 小时；"
                    f"本轮新增 {node.total_task - added_before} 个任务，"
                    f"last_read_message_id={chat_download_config.last_read_message_id}）"
                    f"⇒ 时间片到点，让位给其它会话，下一轮从此处续传"
                )
                break

            if getattr(app, "force_exit", False) or not getattr(
                app, "is_running", True
            ):
                logger.info(f"生产者收到退出信号，停止添加新任务")
                break

            if app.need_skip_message(chat_download_config, message.id):
                continue

            meta_data = MetaData()
            caption = message.caption
            if caption:
                caption = validate_title(caption)
                app.set_caption_name(chat_id, message.media_group_id, caption)
                app.set_caption_entities(
                    chat_id, message.media_group_id, message.caption_entities
                )
            else:
                caption = app.get_caption_name(chat_id, message.media_group_id)
            set_meta_data(meta_data, message, caption)

            if app.exec_filter(chat_download_config, meta_data):
                # Blocking add — waits for a free queue slot (backpressure)
                success = await add_download_task(message, node)
                if not success:
                    logger.debug(f"跳过添加消息 {message.id}（添加失败）")
                    continue

                if node.total_task % 100 == 0:
                    logger.info(f"聊天 {chat_id} 已添加 {node.total_task} 个新任务...")
            else:
                node.download_status[message.id] = DownloadStatus.SkipDownload
                if message.media_group_id:
                    await upload_telegram_chat(
                        client,
                        node.upload_user,
                        app,
                        node,
                        message,
                        DownloadStatus.SkipDownload,
                    )

        chat_download_config.need_check = True
        chat_download_config.total_task = node.total_task
        node.is_running = True

        added_this_round = node.total_task - added_before
        if timed_out:
            logger.info(
                f"聊天 {chat_id} 本轮让位结束：新增 {added_this_round} 个任务，"
                f"下一轮从 last_read_message_id={chat_download_config.last_read_message_id} 续传"
            )
        else:
            logger.info(f"聊天 {chat_id} 新消息处理完成，本轮添加 {added_this_round} 个新任务")
    except Exception as e:
        logger.exception(f"聊天 {chat_id} 下载任务处理异常: {e}")
        chat_download_config.need_check = True

    return timed_out


async def download_all_chat(client: pyrogram.Client):
    """生产者主循环：按会话轮转 + 时间片，一轮全无新内容时按间隔复查。

    - **时间片**（``chat_max_continuous_hours``）：单个会话最多连续处理这么久，
      到点让位给下一个会话 —— 长对话不会饿死其它会话。
    - **循环复查**（``chat_recheck_interval_minutes``）：一轮里所有会话都没新内容
      时等待该间隔再开始下一轮，这样下载完成后依然会自动发现新内容。
      设为 <= 0 则一轮跑完即结束（旧行为，需要重启才会再检查）。
    """
    for chat_id, value in app.chat_download_config.items():
        value.node = TaskNode(chat_id=chat_id)

    # Start one global retry producer (long-running background task)
    retry_task = app.loop.create_task(retry_producer(client))

    max_seconds = _config_seconds("chat_max_continuous_hours", 3600.0)
    recheck_seconds = _config_seconds("chat_recheck_interval_minutes", 60.0)
    logger.info(
        f"生产者调度: 会话时间片={'不限' if not max_seconds else f'{max_seconds / 3600:.2f} 小时'}，"
        f"空闲复查间隔={'关闭' if not recheck_seconds else f'{recheck_seconds / 60:.0f} 分钟'}"
    )

    round_no = 0
    while not getattr(app, "force_exit", False) and getattr(app, "is_running", True):
        round_no += 1
        round_start = time.time()
        yielded = []
        for chat_id, value in app.chat_download_config.items():
            if getattr(app, "force_exit", False) or not getattr(
                app, "is_running", True
            ):
                break
            try:
                still_pending = await download_chat_task(
                    client, chat_id, value, value.node, max_seconds=max_seconds
                )
            except Exception as e:
                logger.exception(f"聊天 {chat_id} 生产者异常: {e}")
                still_pending = False
            if still_pending:
                yielded.append(chat_id)

        if getattr(app, "force_exit", False) or not getattr(app, "is_running", True):
            break

        elapsed = time.time() - round_start
        if yielded:
            logger.info(
                f"第 {round_no} 轮完成（耗时 {elapsed / 60:.1f} 分钟）："
                f"{len(yielded)} 个会话因时间片让位（{yielded}）⇒ 立即开始下一轮"
            )
        elif recheck_seconds and recheck_seconds > 0:
            logger.info(
                f"第 {round_no} 轮完成（耗时 {elapsed / 60:.1f} 分钟）：所有会话均无新内容 ⇒ "
                f"{recheck_seconds / 60:.0f} 分钟后复查"
            )
            if not await sleep_with_exit_check(recheck_seconds):
                logger.info("生产者收到退出信号，停止复查")
                break
        else:
            logger.info(
                f"第 {round_no} 轮完成（耗时 {elapsed / 60:.1f} 分钟）：所有会话均无新内容，"
                f"且未启用空闲复查 ⇒ 生产者结束（重试生产者继续运行）"
            )
            break

    logger.info("所有新消息生产者已完成，重试生产者将继续运行")


async def retry_failed_tasks(
    client: pyrogram.Client,
    chat_id: Union[int, str],
    node: TaskNode,
    max_batch: int = None,
) -> Tuple[int, int]:
    """Retry failed tasks in batch."""
    if max_batch is None:
        max_batch = queue_manager.max_download_tasks  # Batch size = worker count

    failed_tasks = await load_failed_tasks(chat_id)
    if not failed_tasks:
        return 0, 0

    # Get message IDs to retry
    message_ids = [task["message_id"] for task in failed_tasks[:max_batch]]

    if not message_ids:
        return 0, 0

    try:
        messages = await client.get_messages(chat_id=chat_id, message_ids=message_ids)

        # Filter out None messages (may have been deleted)
        valid_messages = [msg for msg in messages if msg is not None]

        if not valid_messages:
            logger.warning(f"聊天 {chat_id} 的失败任务消息已不存在，清理失败列表")
            # Clean up non-existent messages from failed list
            for task in failed_tasks[:max_batch]:
                await remove_failed_task(chat_id, task["message_id"])
            return len(failed_tasks[:max_batch]), 0

        # Add to download queue
        added = await add_download_task_batch(valid_messages, node)

        if added > 0:
            logger.info(f"已为聊天 {chat_id} 重试 {added}/{len(valid_messages)} 个失败任务")
        else:
            logger.warning(f"聊天 {chat_id} 的失败任务重试添加失败")

        return len(failed_tasks[:max_batch]), added

    except Exception as e:
        logger.error(f"重试失败任务时出错: {e}")
        return len(failed_tasks[:max_batch]), 0
