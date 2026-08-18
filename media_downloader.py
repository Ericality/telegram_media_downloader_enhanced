"""Telegram Media Downloader

Downloads media from Telegram chats with:
- Multi-chat download management & progress tracking
- Bark & Synology Chat dual notification system
- Separate download queue & notification queue
- Disk space monitoring with auto pause/resume
- Infinite failed task retry mechanism
- Rclone / Aligo cloud storage upload
- Web admin panel
"""
import asyncio
import json
import logging
import os
import shutil
import signal
import sys
import time
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple, Union
from pathlib import Path

import aiohttp
import psutil
import pyrogram
from loguru import logger
from pyrogram.types import Audio, Document, Photo, Video, VideoNote, Voice
from rich.logging import RichHandler
from rich.console import Console
from rich.theme import Theme

import core.context as ctx
from core.config import _check_config, _load_config, check_config_consistency, print_config_summary
from core.context import CONFIG_NAME, RETRY_TIME_OUT, _media_seen, app, queue_manager
from core.queues import QueueManager
from core.storage import (
    _load_duplicate_count,
    _load_seen_media,
    _save_duplicate_count,
    _save_seen_media,
    load_failed_tasks,
    record_failed_task,
    remove_failed_task,
)
from module.app import ChatDownloadConfig, DownloadStatus, TaskNode
from services.notifier import (
    notification_manager,
    send_bark_notification,
    send_bark_notification_sync,
    send_synology_chat_notification,
    send_synology_chat_notification_sync,
)
from services.stats import (
    calculate_directory_size,
    collect_stats,
    collect_stats_async,
    get_storage_summary_text,
)
from workers.monitor import (
    check_disk_space,
    disk_monitor,
    disk_space_monitor_task,
    queue_monitor_task,
    stats_notification_task,
)
from module.bot import start_download_bot, stop_download_bot
from module.download_stat import update_download_status
from module.download_stat import get_download_result
from module.get_chat_history_v2 import get_chat_history_v2
from module.language import _t
from module.pyrogram_extension import (
    HookClient,
    fetch_message,
    get_extension,
    record_download_status,
    report_bot_download_status,
    set_max_concurrent_transmissions,
    set_meta_data,
    update_cloud_upload_stat,
    upload_telegram_chat,
)
from module.web import init_web
from utils.format import truncate_filename, validate_title
from module.cloud_drive import verify_rclone_remote
from utils.log import LogFilter
from utils.meta_data import MetaData

custom_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "red",
    "success": "green",
    "debug": "dim blue",
})
console = Console(theme=custom_theme)

# RichHandler initialized at INFO level; reconfigured after config loaded
rich_handler = RichHandler(
    console=console,
    rich_tracebacks=True,
    markup=True,
    show_time=True,
    show_path=False,
    tracebacks_show_locals=False,
    level=logging.INFO
)

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[rich_handler],
)


# Bark notification level mapping
BARK_LEVELS = {
    "active": "active",
    "timeSensitive": "timeSensitive",
    "passive": "passive"
}




async def notify_worker(worker_id: int):
    """Notification queue worker with delay monitoring."""
    logger.debug(f"通知Worker {worker_id} 启动")

    while True:
        # Check exit signal; continue processing if queue is not empty
        should_exit = getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True)

        try:
            # Exit only if queue is empty
            if should_exit and ctx.notify_queue.empty():
                logger.debug(f"通知Worker {worker_id} 队列已空，准备退出")
                break

            # Use timed get to avoid indefinite blocking
            try:
                task = await asyncio.wait_for(ctx.notify_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            task_type = task.get('type')
            create_time = task.get('create_time', '未知')
            queue_time = task.get('queue_time', time.time())

            # Calculate delay
            current_time = time.time()
            delay_seconds = current_time - queue_time

            # Log delay if significant
            if delay_seconds > 10:  # Warn if delay exceeds 10s
                logger.warning(f"通知Worker {worker_id}: 任务延迟 {delay_seconds:.1f} 秒, 创建时间={create_time}")
            elif delay_seconds > 60:  # Critical if delay exceeds 60s
                logger.error(f"通知Worker {worker_id}: 任务严重延迟 {delay_seconds:.1f} 秒, 创建时间={create_time}")

            if task_type == 'bark_notification':
                # Process Bark notification
                title = task.get('title')
                body = task.get('body')
                url = task.get('url')
                group = task.get('group')
                level = task.get('level')

                logger.debug(f"通知Worker {worker_id} 处理Bark通知: {title}, 延迟={delay_seconds:.1f}秒")

                try:
                    success = await send_bark_notification_sync(title, body, url, group, level)
                    if success:
                        logger.debug(f"通知Worker {worker_id}: {title} 发送成功, 总延迟={delay_seconds:.1f}秒")
                    else:
                        logger.warning(f"通知Worker {worker_id}: {title} 发送失败, 延迟={delay_seconds:.1f}秒")
                except Exception as e:
                    logger.error(f"通知Worker {worker_id} 发送Bark通知时出错: {e}, 延迟={delay_seconds:.1f}秒")
                finally:
                    ctx.notify_queue.task_done()

            elif task_type == 'synology_chat_notification':
                # Process Synology Chat notification
                title = task.get('title')
                message = task.get('message')
                level = task.get('level', 'info')
                webhook_url = task.get('webhook_url')
                bot_name = task.get('bot_name')
                bot_avatar = task.get('bot_avatar')
                mention_users = task.get('mention_users')
                mention_channels = task.get('mention_channels')

                logger.debug(f"通知Worker {worker_id} 处理群晖Chat通知: {title}, 延迟={delay_seconds:.1f}秒")

                try:
                    success = await send_synology_chat_notification_sync(
                        title, message, level, webhook_url, bot_name, bot_avatar,
                        mention_users, mention_channels
                    )
                    if success:
                        logger.debug(
                            f"通知Worker {worker_id}: 群晖Chat通知 {title} 发送成功, 延迟={delay_seconds:.1f}秒")
                    else:
                        logger.warning(
                            f"通知Worker {worker_id}: 群晖Chat通知 {title} 发送失败, 延迟={delay_seconds:.1f}秒")
                except Exception as e:
                    logger.error(f"通知Worker {worker_id} 发送群晖Chat通知时出错: {e}, 延迟={delay_seconds:.1f}秒")
                finally:
                    ctx.notify_queue.task_done()

            elif task_type == 'stats_notification':
                # Placeholder for future notification types
                pass

        except asyncio.CancelledError:
            logger.debug(f"通知Worker {worker_id} 被取消")
            break
        except Exception as e:
            logger.error(f"通知Worker {worker_id} 异常: {e}")
            try:
                ctx.notify_queue.task_done()
            except:
                pass
            await asyncio.sleep(1)

    logger.debug(f"通知Worker {worker_id} 退出")



def run_async_sync(coroutine, loop=None, timeout=10):
    """Run async coroutine synchronously."""
    if loop is None:
        loop = app.loop

    if loop and loop.is_running():
        # Use run_coroutine_threadsafe if loop is already running
        import asyncio as aio
        future = aio.run_coroutine_threadsafe(coroutine, loop)
        return future.result(timeout=timeout)
    else:
        # Otherwise use run_until_complete
        return loop.run_until_complete(coroutine)


def setup_exit_signal_handlers():
    """Set up graceful exit signal handlers."""

    def signal_handler(signum, frame):
        logger.info(f"接收到信号 {signum}，正在优雅退出...")

        if hasattr(app, 'is_running'):
            app.is_running = False

        if hasattr(app, 'force_exit'):
            app.force_exit = True

        if signum == signal.SIGINT:
            logger.info("正在停止所有任务，请稍候...")
        elif signum == signal.SIGTERM:
            logger.info("收到终止信号，正在停止...")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


async def graceful_shutdown():
    """Gracefully shut down all components."""
    logger.info("开始优雅关闭...")

    # 1. Stop adding new tasks
    app.is_running = False
    app.force_exit = True

    # 2. Send shutdown notification first
    try:
        if notification_manager.should_notify("shutdown"):
            shutdown_title = "程序停止"
            shutdown_message = (
                f"🛑 Telegram媒体下载器已停止\n"
                f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"运行时间: {datetime.now() - disk_monitor.stats_start_time}\n"
                f"完成任务: {app.total_download_task}"
            )

            # Give notification time to send
            notification_task = asyncio.create_task(
                notification_manager.send_event_notification("shutdown", shutdown_title, shutdown_message)
            )
            await asyncio.wait_for(notification_task, timeout=10)
            logger.info("关闭通知已发送")
    except Exception as e:
        logger.error(f"发送关闭通知失败: {e}")

    # 3. Brief wait for producers to stop
    await asyncio.sleep(1)

    # 4. Record in-flight and queued tasks to failed list
    pending_messages = []

    # Record all currently downloading tasks
    for chat_id, chat_config in app.chat_download_config.items():
        if chat_config.node and chat_config.node.download_status:
            for message_id, status in chat_config.node.download_status.items():
                if status == DownloadStatus.Downloading:
                    pending_messages.append((message_id, chat_id))
                    logger.debug(f"记录正在下载的任务: chat_id={chat_id}, message_id={message_id}")

    # Record all queued tasks
    try:
        while not ctx.download_queue.empty():
            try:
                message, node = ctx.download_queue.get_nowait()
                pending_messages.append((message.id, node.chat_id))
                ctx.download_queue.task_done()
                logger.debug(f"记录队列中的任务: chat_id={node.chat_id}, message_id={message.id}")
            except (asyncio.QueueEmpty, ValueError):
                break
    except Exception as e:
        logger.error(f"清空下载队列时出错: {e}")

    # Write to failed tasks file
    if pending_messages:
        logger.warning(f"有 {len(pending_messages)} 个未完成任务需要记录到失败列表")
        for message_id, chat_id in pending_messages:
            await record_failed_task(chat_id, message_id, "程序退出，任务未完成")

    # Final shutdown notification already sent above; skip duplicate

    logger.info("优雅关闭完成")


async def run_until_all_task_finish():
    """Main run loop: wait for new tasks to finish, then wait for retry producers or exit signal."""
    logger.info("开始主运行循环...")

    # Wait for new tasks to complete (producers have finished adding)
    while True:
        if getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True):
            logger.info("收到退出信号，准备退出...")
            break

        all_new_tasks_done = True
        for _, value in app.chat_download_config.items():
            if not value.need_check or value.total_task != value.finish_task:
                all_new_tasks_done = False
                break

        if all_new_tasks_done:
            logger.info("所有新任务已完成")
            break

        await asyncio.sleep(1)

    # After new tasks complete, keep running (retry producers still active) until exit signal
    while getattr(app, 'is_running', True) and not getattr(app, 'force_exit', False):
        # Periodic sleep; could add stats logging here
        await asyncio.sleep(10)

    logger.info("主运行循环结束")


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
        datetime_dir_name = message.date.strftime(app.date_format)
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
        temp_file_name = os.path.join(app.temp_save_path, dirname, file_name)
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
        temp_file_name = os.path.join(app.temp_save_path, dirname, gen_file_name)
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

    if getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True):
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
                chat_config = app.chat_download_config.get(node.chat_id) or app.chat_download_config.get(chat_id_str)
                if chat_config:
                    try:
                        message_id_int = int(message.id)
                        current_last_id = getattr(chat_config, 'last_read_message_id', 0)
                        if current_last_id is None:
                            current_last_id = 0
                        current_last_id = int(current_last_id)
                        if message_id_int > current_last_id:
                            chat_config.last_read_message_id = message_id_int
                            logger.debug(f"更新聊天 {node.chat_id} 的 last_read_message_id 到 {message_id_int}")
                            app.update_config(immediate=True)
                    except (ValueError, TypeError) as e:
                        logger.error(f"更新 last_read_message_id 时出错: {e}")

        await remove_failed_task(node.chat_id, message.id)

        if wait_seconds > 60:
            logger.warning(f"任务添加等待 {int(wait_seconds)} 秒: message_id={message.id}")
        logger.debug(f"[{'RETRY' if is_retry else 'NEW'}] 已添加下载任务: message_id={message.id}, 队列大小={ctx.download_queue.qsize()}")
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

    while getattr(app, 'is_running', True) and not getattr(app, 'force_exit', False):
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
                msg_id = task['message_id']
                try:
                    msg = await client.get_messages(chat_id, msg_id)
                    if msg is not None:
                        success = await add_download_task(msg, chat_config.node, is_retry=True)
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
    if not getattr(app, 'is_running', True) or getattr(app, 'force_exit', False):
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


async def save_msg_to_file(
        app, chat_id: Union[int, str], message: pyrogram.types.Message
):
    """Save message text or caption to a .txt file."""
    dirname = validate_title(
        message.chat.title if message.chat and message.chat.title else str(chat_id)
    )
    datetime_dir_name = message.date.strftime(app.date_format) if message.date else "0"

    file_save_path = app.get_file_save_path("msg", dirname, datetime_dir_name)
    file_name = os.path.join(
        app.temp_save_path,
        file_save_path,
        f"{app.get_file_name(message.id, None, None)}.txt",
    )

    os.makedirs(os.path.dirname(file_name), exist_ok=True)

    if _is_exist(file_name):
        return DownloadStatus.SkipDownload, None

    content = message.text or message.caption or ""
    with open(file_name, "w", encoding="utf-8") as f:
        f.write(content)

    return DownloadStatus.SuccessDownload, file_name


async def download_task(client, message, node):
    """Download and forward media from a message."""
    try:
        original_download_status, file_name = await download_media(
            client, message, app.media_types, app.file_formats, node
        )

        if original_download_status == DownloadStatus.SuccessDownload:
            removed = await remove_failed_task(node.chat_id, message.id)
            if removed:
                disk_monitor.retry_success_count += 1
                logger.info(f"[RETRY] 重试成功: chat_id={node.chat_id}, message_id={message.id}")

        if file_name and os.path.exists(file_name):
            try:
                file_size = os.path.getsize(file_name)
                disk_monitor.stats_since_last_notification["download_size"] += file_size
            except:
                pass

        # Clear download_result early so web UI doesn't count upload phase
        try:
            from module.download_stat import remove_download_record
            await remove_download_record(node.chat_id, message.id)
        except Exception as e:
            logger.error(f"清除下载记录失败: {e}")

        # Upload the main media file FIRST (mp4 / jpg / etc.)
        media_file_to_upload = original_download_status == DownloadStatus.SuccessDownload and file_name or None
        if media_file_to_upload and not node.upload_telegram_chat_id:
            logger.info(f"开始上传文件: {media_file_to_upload}")
            ui_file_name = media_file_to_upload
            if app.hide_file_name:
                ui_file_name = f"****{os.path.splitext(media_file_to_upload)[-1]}"
            upload_ok = await app.upload_file(
                media_file_to_upload, update_cloud_upload_stat, (node, message.id, ui_file_name)
            )
            logger.debug(f"[UPLOAD] download_task 媒体上传结果: ok={upload_ok}, file={media_file_to_upload}")
            if upload_ok:
                node.upload_success_count += 1

        # Save and upload .txt caption separately (do NOT replace file_name)
        download_status = original_download_status
        if app.enable_download_txt and (message.text or message.caption):
            txt_status, txt_path = await save_msg_to_file(app, node.chat_id, message)
            if txt_status == DownloadStatus.SuccessDownload and txt_path and not node.upload_telegram_chat_id:
                logger.info(f"开始上传文件: {txt_path}")
                upload_txt_ok = await app.upload_file(
                    txt_path, update_cloud_upload_stat, (node, message.id, txt_path)
                )
                logger.debug(f"[UPLOAD] download_task txt 上传结果: ok={upload_txt_ok}, file={txt_path}")

        if not node.bot:
            app.set_download_id(node, message.id, download_status)

        node.download_status[message.id] = download_status
        file_size = os.path.getsize(file_name) if file_name else 0

        await upload_telegram_chat(
            client,
            node.upload_user if node.upload_user else client,
            app,
            node,
            message,
            download_status,
            file_name,
        )

        await report_bot_download_status(
            node.bot,
            node,
            download_status,
            file_size,
        )

        queue_manager.task_processed += 1

    finally:
        # Remove from download_result to avoid stale entries in frontend
        try:
            from module.download_stat import remove_download_record
            await remove_download_record(node.chat_id, message.id)
        except Exception as e:
            logger.error(f"清除下载记录失败: {e}")

@record_download_status
async def download_media(
        client: pyrogram.client.Client,
        message: pyrogram.types.Message,
        media_types: List[str],
        file_formats: dict,
        node: TaskNode,
):
    """Download media from a Telegram message."""
    file_name: str = ""
    ui_file_name: str = ""
    task_start_time: float = time.time()
    media_size = 0
    _media = None
    temp_file_name = None

    # Check exit signal
    if getattr(app, 'force_exit', False):
        logger.debug(f"消息 {message.id}: 程序正在退出，跳过下载")
        return DownloadStatus.FailedDownload, None

    message = await fetch_message(client, message)

    # Check dedup BEFORE download (with configurable threshold)
    for _type in media_types:
        _media_check = getattr(message, _type, None)
        if _media_check is not None:
            media_uid = getattr(_media_check, 'file_unique_id', None)
            if media_uid:
                threshold = getattr(app, 'download_duplicate_threshold', 5)
                current_count = ctx._media_download_count.get(media_uid, 0)
                if threshold > 0 and current_count >= threshold:
                    logger.info(
                        f"[DEDUP] 消息 {message.id}: 媒体已下载 {current_count} 次（阈值={threshold}），跳过下载"
                    )
                    if current_count > 0:
                        ctx._media_download_count[media_uid] = current_count + 1
                        _save_duplicate_count(ctx._media_download_count)
                    return DownloadStatus.SkipDownload, None
                # Track count even for first download
                ctx._media_download_count[media_uid] = current_count + 1
                _save_duplicate_count(ctx._media_download_count)
            break

    logger.debug(f"开始下载消息 {message.id}...")

    try:
        for _type in media_types:
            _media = getattr(message, _type, None)
            if _media is None:
                continue
            file_name, temp_file_name, file_format = await _get_media_meta(
                node.chat_id, message, _media, _type
            )
            media_size = getattr(_media, "file_size", 0)

            ui_file_name = file_name
            if app.hide_file_name:
                ui_file_name = f"****{os.path.splitext(file_name)[-1]}"

            logger.debug(f"消息 {message.id}: 类型={_type}, 大小={media_size} bytes, 格式={file_format}")

            if _can_download(_type, file_formats, file_format):
                if _is_exist(file_name):
                    file_size = os.path.getsize(file_name)
                    if file_size or file_size == media_size:
                        logger.info(
                            f"id={message.id} {ui_file_name} "
                            f"{_t('already download,download skipped')}.\n"
                        )
                        return DownloadStatus.SkipDownload, None
            else:
                logger.info(f"消息 {message.id}: 文件格式 {file_format} 不在允许的下载列表中，跳过")
                return DownloadStatus.SkipDownload, None

            break
    except Exception as e:
        logger.error(
            f"Message[{message.id}]: "
            f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
            exc_info=True,
        )
        return DownloadStatus.FailedDownload, None

    if _media is None:
        logger.debug(f"消息 {message.id}: 没有媒体内容，跳过")
        return DownloadStatus.SkipDownload, None

    # Dedup already checked at fetch_message step — record after success
    message_id = message.id

    for retry in range(3):
        try:
            # Check exit signal
            if getattr(app, 'force_exit', False):
                logger.debug(f"消息 {message.id}: 程序正在退出，中止下载")
                # Clean up temp file
                if temp_file_name and os.path.exists(temp_file_name):
                    try:
                        os.remove(temp_file_name)
                        logger.debug(f"已删除临时文件: {temp_file_name}")
                    except:
                        pass
                return DownloadStatus.FailedDownload, None

            if retry > 0:
                logger.warning(f"消息 {message.id}: 第 {retry} 次重试下载")

            temp_download_path = await client.download_media(
                message,
                file_name=temp_file_name,
                progress=update_download_status,
                progress_args=(
                    message_id,
                    ui_file_name,
                    task_start_time,
                    node,
                    client,
                ),
            )

            if temp_download_path and isinstance(temp_download_path, str):
                _check_download_finish(media_size, temp_download_path, ui_file_name)
                await asyncio.sleep(0.5)
                _move_to_download_path(temp_download_path, file_name)

                # Mark media as seen for deduplication
                media_uid = getattr(_media, 'file_unique_id', None)
                if media_uid and media_uid not in _media_seen:
                    _media_seen.add(media_uid)
                    _save_seen_media(_media_seen)

                logger.success(f"消息 {message.id}: 下载成功 - {ui_file_name}")
                return DownloadStatus.SuccessDownload, file_name

        except OSError as e:
            logger.warning(f"网络连接错误: {e}，重试 {retry + 1}/3")
            await asyncio.sleep(RETRY_TIME_OUT * (retry + 1))
            if retry == 2:
                await record_failed_task(node.chat_id, message.id, f"Network error: {str(e)}")
                raise
        except asyncio.CancelledError:
            logger.info(f"消息 {message.id} 下载被取消")
            # Clean up temp file
            if temp_file_name and os.path.exists(temp_file_name):
                try:
                    os.remove(temp_file_name)
                    logger.debug(f"已删除临时文件: {temp_file_name}")
                except:
                    pass
            raise  # Re-raise for worker to handle
        except pyrogram.errors.exceptions.bad_request_400.BadRequest:
            logger.warning(
                f"Message[{message.id}]: {_t('file reference expired, refetching')}..."
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            message = await fetch_message(client, message)
            if _check_timeout(retry, message.id):
                logger.error(
                    f"Message[{message.id}]: "
                    f"{_t('file reference expired for 3 retries, download skipped.')}"
                )
        except pyrogram.errors.exceptions.flood_420.FloodWait as wait_err:
            await asyncio.sleep(wait_err.value)
            logger.warning("Message[{}]: FlowWait {}", message.id, wait_err.value)
            _check_timeout(retry, message.id)
        except TypeError:
            logger.warning(
                f"{_t('Timeout Error occurred when downloading Message')}[{message.id}], "
                f"{_t('retrying after')} {RETRY_TIME_OUT} {_t('seconds')}"
            )
            await asyncio.sleep(RETRY_TIME_OUT)
            if _check_timeout(retry, message.id):
                logger.error(
                    f"Message[{message.id}]: {_t('Timing out after 3 reties, download skipped.')}"
                )
        except Exception as e:
            logger.error(
                f"Message[{message.id}]: "
                f"{_t('could not be downloaded due to following exception')}:\n[{e}].",
                exc_info=True,
            )
            break

    logger.error(f"消息 {message.id}: 下载失败，已加入失败任务列表")
    return DownloadStatus.FailedDownload, None


async def download_worker(client: pyrogram.client.Client, worker_id: int):
    """Download task worker."""
    logger.debug(f"下载Worker {worker_id} 启动")

    while True:
        # Check forced exit signal
        if getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True):
            logger.debug(f"下载Worker {worker_id} 收到退出信号，准备退出")
            break

        try:
            # Check cloud upload health before disk check
            if not getattr(app, 'force_exit', False):
                if not ctx.cloud_upload_ok:
                    if worker_id not in disk_monitor.paused_workers:
                        logger.warning(f"下载Worker {worker_id}: 云端上传验证失败，暂停下载")
                        disk_monitor.paused_workers.add(worker_id)
                    if not getattr(app, 'force_exit', False):
                        await asyncio.sleep(60)
                        continue
                    else:
                        break
                else:
                    if worker_id in disk_monitor.paused_workers:
                        logger.info(f"下载Worker {worker_id}: 云端连接恢复，继续下载")
                        disk_monitor.paused_workers.discard(worker_id)

            # Check disk space (skip if exiting)
            if not getattr(app, 'force_exit', False):
                bark_config = getattr(app, 'bark_notification', {})
                threshold_gb = bark_config.get('disk_space_threshold_gb', 10.0)

                has_space, available_gb, _ = await check_disk_space(threshold_gb)

                if not has_space:
                    if worker_id not in disk_monitor.paused_workers:
                        logger.warning(
                            f"下载Worker {worker_id}: 磁盘空间不足 ({available_gb}GB < {threshold_gb}GB)，暂停下载")
                        disk_monitor.paused_workers.add(worker_id)

                        events_to_notify = bark_config.get('events_to_notify', [])
                        if 'task_paused' in events_to_notify:
                            message = f"Worker {worker_id}: 因磁盘空间不足暂停下载\n可用空间: {available_gb}GB"
                            await send_bark_notification("下载任务暂停", message)

                    # Keep paused state if program is exiting
                    if not getattr(app, 'force_exit', False):
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
            if not getattr(app, 'force_exit', False):
                await asyncio.sleep(60)
            continue

        try:
            # Use timed get to avoid indefinite blocking
            try:
                message, node = await asyncio.wait_for(ctx.download_queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                continue

            # Re-check exit signal before processing
            if getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True):
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
                retry_count = await record_failed_task(node.chat_id, message.id, f"Network error: {str(e)}")
                logger.warning(f"Message {message.id} network error, recorded to failed list (retry count: {retry_count})")
            except Exception as e:
                logger.error(f"下载Worker {worker_id}: 消息 {message.id} 下载任务异常: {e}")
                retry_count = await record_failed_task(node.chat_id, message.id, f"Download exception: {str(e)}")
                logger.warning(f"Message {message.id} download exception, recorded to failed list (retry count: {retry_count})")
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
):
    """Producer: feed new messages to download queue one-by-one.
    
    Uses add_download_task() which blocks on queue.put() when full,
    creating natural backpressure — producer waits for workers to free slots.
    """
    try:
        logger.info(f"开始处理聊天 {chat_id}，last_read_message_id={chat_download_config.last_read_message_id}")

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

            if getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True):
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

        logger.info(f"聊天 {chat_id} 新消息处理完成，共添加 {node.total_task} 个新任务")
    except Exception as e:
        logger.exception(f"聊天 {chat_id} 下载任务处理异常: {e}")
        chat_download_config.need_check = True

async def download_all_chat(client: pyrogram.Client):
    """Process chats sequentially; start one global retry producer in background."""
    for chat_id, value in app.chat_download_config.items():
        value.node = TaskNode(chat_id=chat_id)

    # Start one global retry producer (long-running background task)
    retry_task = app.loop.create_task(retry_producer(client))

    # Process chats sequentially — natural single-producer backpressure
    for chat_id, value in app.chat_download_config.items():
        await download_chat_task(client, chat_id, value, value.node)

    logger.info("所有新消息生产者已完成，重试生产者将继续运行")

async def retry_failed_tasks(
        client: pyrogram.Client,
        chat_id: Union[int, str],
        node: TaskNode,
        max_batch: int = None
) -> Tuple[int, int]:
    """Retry failed tasks in batch."""
    if max_batch is None:
        max_batch = queue_manager.max_download_tasks  # Batch size = worker count

    failed_tasks = await load_failed_tasks(chat_id)
    if not failed_tasks:
        return 0, 0

    # Get message IDs to retry
    message_ids = [task['message_id'] for task in failed_tasks[:max_batch]]

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
                await remove_failed_task(chat_id, task['message_id'])
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


async def start_server(client: pyrogram.Client):
    """Start Pyrogram client."""
    await client.start()


async def stop_server(client: pyrogram.Client):
    """Stop Pyrogram client."""
    await client.stop()


async def start_notify_workers():
    """Start notification workers."""
    notify_tasks = []

    for i in range(queue_manager.max_notify_tasks):
        task = app.loop.create_task(notify_worker(i + 1))
        notify_tasks.append(task)
        logger.debug(f"启动通知Worker {i + 1}/{queue_manager.max_notify_tasks}")

    return notify_tasks


async def start_download_workers(client: pyrogram.Client):
    """Start download workers."""
    download_tasks = []

    for i in range(queue_manager.max_download_tasks):
        task = app.loop.create_task(download_worker(client, i + 1))
        download_tasks.append(task)
        logger.debug(f"启动下载Worker {i + 1}/{queue_manager.max_download_tasks}")

    return download_tasks


async def wait_for_queues_to_empty():
    """Wait for queues to empty (with timeout fallback)."""
    logger.info("等待所有队列任务完成...")

    max_wait_time = 30
    start_time = time.time()

    # Try graceful wait first
    while time.time() - start_time < max_wait_time:
        try:
            # Prefer empty() over qsize() for accuracy
            download_queue_size = ctx.download_queue.qsize() if hasattr(ctx.download_queue, 'qsize') else 0
            notify_queue_size = ctx.notify_queue.qsize() if hasattr(ctx.notify_queue, 'qsize') else 0

            logger.debug(f"队列状态: 下载队列={download_queue_size}, 通知队列={notify_queue_size}")

            # More accurate emptiness check
            is_download_queue_empty = ctx.download_queue.empty() if hasattr(ctx.download_queue, 'empty') else (
                        download_queue_size == 0)
            is_notify_queue_empty = ctx.notify_queue.empty() if hasattr(ctx.notify_queue, 'empty') else (notify_queue_size == 0)

            if is_download_queue_empty and is_notify_queue_empty:
                # Check unfinished task counter
                unfinished_download_tasks = ctx.download_queue._unfinished_tasks if hasattr(ctx.download_queue,
                                                                                        '_unfinished_tasks') else 0
                unfinished_notify_tasks = ctx.notify_queue._unfinished_tasks if hasattr(ctx.notify_queue,
                                                                                    '_unfinished_tasks') else 0

                if unfinished_download_tasks == 0 and unfinished_notify_tasks == 0:
                    logger.info("所有队列已清空")
                    return True

                logger.debug(f"未完成任务: 下载={unfinished_download_tasks}, 通知={unfinished_notify_tasks}")

            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"检查队列状态时出错: {e}")
            break

    # Force clear on timeout
    logger.warning("等待队列清空超时，强制清理队列...")

    # Drain download queue
    try:
        while not ctx.download_queue.empty():
            try:
                ctx.download_queue.get_nowait()
                ctx.download_queue.task_done()
            except (asyncio.QueueEmpty, ValueError):
                break
    except Exception as e:
        logger.error(f"清空下载队列时出错: {e}")

    # Drain notification queue
    try:
        while not ctx.notify_queue.empty():
            try:
                ctx.notify_queue.get_nowait()
                ctx.notify_queue.task_done()
            except (asyncio.QueueEmpty, ValueError):
                break
    except Exception as e:
        logger.error(f"清空通知队列时出错: {e}")

    logger.warning("队列已强制清空")
    return False


def main():
    """Main entry point."""
    setup_exit_signal_handlers()

    # Task lists for cleanup
    notify_tasks = []
    download_tasks = []
    monitor_tasks = []
    chat_tasks = []

    client = None

    try:
        # Initialize application
        app.pre_run()
        init_web(app)

        ctx._media_download_count = _load_duplicate_count()

        # Print config summary
        print_config_summary(app)

        # Check config consistency
        issues = check_config_consistency(app)
        if issues:
            logger.warning("配置检查发现问题:")
            for i, issue in enumerate(issues, 1):
                logger.warning(f"  {i}. {issue}")
        else:
            logger.success("配置检查通过!")

        # Initialize Pyrogram client
        client = HookClient(
            "media_downloader",
            api_id=app.api_id,
            api_hash=app.api_hash,
            proxy=app.proxy,
            workdir=app.session_file_path,
            start_timeout=app.start_timeout,
        )

        # Update queue manager limits
        queue_manager.update_limits()

        # Re-initialize queues and semaphore with configured sizes
        ctx.download_queue = asyncio.Queue(maxsize=queue_manager.download_queue_size)
        ctx.notify_queue = asyncio.Queue(maxsize=100)
        ctx.download_semaphore = asyncio.Semaphore(queue_manager.max_download_tasks)

        logger.info(f"下载队列大小已设置为: {queue_manager.download_queue_size}")

        # Load notification manager config
        notification_manager.load_config()

        # Send startup notification (after notification system is initialized)
        async def send_startup_notification():
            if notification_manager.should_notify("startup"):
                startup_title = "程序启动"
                startup_message = (
                    f"🚀 Telegram媒体下载器已启动\n"
                    f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"版本: 1.0.0\n"
                    f"下载任务数: {len(app.chat_download_config)}\n"
                    f"通知系统: {'已启用' if notification_manager.bark_enabled or notification_manager.synology_chat_enabled else '未启用'}"
                )

                # Test notification delivery
                success = await notification_manager.send_event_notification(
                    "startup", startup_title, startup_message
                )
                if success:
                    logger.info("✅ 启动通知发送成功")
                else:
                    logger.warning("启动通知发送失败")

        # Verify cloud storage connectivity FIRST, before startup notification
        async def verify_cloud():
            if app.cloud_drive_config.enable_upload_file and app.cloud_drive_config.upload_adapter == "rclone":
                from module.cloud_drive import verify_rclone_remote
                cloud_ok, cloud_msg = await verify_rclone_remote(app.cloud_drive_config)
                if cloud_ok:
                    logger.success(f"☁️  {cloud_msg}")
                else:
                    ctx.cloud_upload_ok = False
                    logger.error(f"☁️  {cloud_msg}")
                    await notification_manager.send_event_notification("startup", "☁️ 云端写入验证失败，下载暂停", cloud_msg, "error")
                    logger.warning("☁️ 云端写入测试失败：download worker 已暂停，等待云端恢复后自动继续")

        app.loop.run_until_complete(verify_cloud())

        # Run startup notification
        app.loop.run_until_complete(send_startup_notification())

        # Set global exception handler
        def global_exception_handler(loop, context):
            exception = context.get('exception')
            if exception:
                logger.error(f"未处理的异常: {exception}")
            logger.error(f"异常上下文: {context}")

            if hasattr(app, 'force_exit') and app.force_exit:
                logger.info("强制退出程序中...")
                sys.exit(1)

        app.loop.set_exception_handler(global_exception_handler)
        set_max_concurrent_transmissions(client, app.max_concurrent_transmissions)

        # Start Pyrogram client
        app.loop.run_until_complete(start_server(client))
        logger.success(_t("Successfully started (Press Ctrl+C to stop)"))

        # Set running flags
        if not hasattr(app, 'force_exit'):
            app.force_exit = False
        if not hasattr(app, 'is_running'):
            app.is_running = True

        # Step 1: Start all workers
        notify_tasks = app.loop.run_until_complete(start_notify_workers())
        download_tasks = app.loop.run_until_complete(start_download_workers(client))

        # Step 2: Start monitor tasks
        if notification_manager.bark_enabled or notification_manager.synology_chat_enabled:
            # Start disk space monitor
            disk_monitor_task_obj = app.loop.create_task(disk_space_monitor_task())
            monitor_tasks.append(disk_monitor_task_obj)

            # Start stats notification
            stats_task_obj = app.loop.create_task(stats_notification_task())
            monitor_tasks.append(stats_task_obj)

            # Start queue monitor
            queue_monitor_obj = app.loop.create_task(queue_monitor_task())
            monitor_tasks.append(queue_monitor_obj)

            logger.info("通知系统已启用，监控任务已启动")
        else:
            logger.info("所有通知方式均未启用，跳过监控任务")

        # Step 3: Start chat download tasks (async)
        logger.info("启动聊天下载任务...")
        chat_task = app.loop.create_task(download_all_chat(client))
        chat_tasks.append(chat_task)

        # Give producers time to start
        app.loop.run_until_complete(asyncio.sleep(3))

        # Step 4: Start bot if configured
        if app.bot_token:
            logger.info("启动下载机器人...")
            bot_task = app.loop.create_task(
                start_download_bot(app, client, add_download_task, download_chat_task)
            )
            chat_tasks.append(bot_task)

        logger.info("=" * 60)
        logger.info("所有组件已启动，开始处理任务...")
        logger.info("失败任务将无限重试直到成功")
        logger.info("=" * 60)

        # Step 5: Enter main run loop
        app.loop.run_until_complete(run_until_all_task_finish())

    except KeyboardInterrupt:
        logger.info(_t("KeyboardInterrupt"))
        if hasattr(app, 'force_exit'):
            app.force_exit = True
    except Exception as e:
        logger.exception("{}", e)
    finally:
        # Set exit flags so all tasks know to exit
        app.is_running = False
        app.force_exit = True

        logger.info("=" * 60)
        logger.info("程序正在停止...")

        try:
            # Perform graceful shutdown first
            app.loop.run_until_complete(graceful_shutdown())
        except Exception as e:
            logger.error(f"优雅关闭过程中出错: {e}")

        # Cancel all tasks
        logger.info("取消所有任务...")
        all_tasks = []
        if 'chat_tasks' in locals():
            all_tasks.extend(chat_tasks)
        if 'monitor_tasks' in locals():
            all_tasks.extend(monitor_tasks)
        if 'download_tasks' in locals():
            all_tasks.extend(download_tasks)
        if 'notify_tasks' in locals():
            all_tasks.extend(notify_tasks)

        for task in all_tasks:
            if hasattr(task, 'done') and not task.done():
                try:
                    task.cancel()
                except:
                    pass

        # Brief wait for tasks to respond to cancellation
        try:
            app.loop.run_until_complete(asyncio.sleep(2))
        except:
            pass

        # Print final chat config state
        logger.info("当前聊天配置状态:")
        for chat_id, chat_config in app.chat_download_config.items():
            logger.info(
                f"  - 聊天 {chat_id}: last_read_message_id={getattr(chat_config, 'last_read_message_id', '未设置')}")

        logger.info(f"{_t('update config')}......")
        try:
            # Try to update config
            success = app.update_config()
            if success:
                logger.success(f"{_t('Updated last read message_id to config file')}")

                # Show updated config from app.config
                if hasattr(app, 'config') and 'chat' in app.config:
                    logger.info("更新后的聊天配置:")
                    for chat_item in app.config['chat']:
                        chat_id = chat_item.get('chat_id')
                        last_id = chat_item.get('last_read_message_id')
                        logger.info(f"  - chat_id: {chat_id}, last_read_message_id: {last_id}")
                else:
                    logger.warning("无法获取更新后的配置信息")
            else:
                logger.warning(f"配置更新可能失败，请检查日志")
        except Exception as e:
            logger.error(f"保存配置时出错: {e}")
            import traceback
            logger.error(f"堆栈信息: {traceback.format_exc()}")

        # Check config file size
        try:
            if os.path.exists(CONFIG_NAME):
                file_size = os.path.getsize(CONFIG_NAME)
                logger.info(f"配置文件大小: {file_size} 字节")
        except:
            pass

        if app.bot_token:
            try:
                app.loop.run_until_complete(stop_download_bot())
            except:
                pass

        try:
            if client:
                app.loop.run_until_complete(stop_server(client))
        except:
            pass

        logger.info(_t("Stopped!"))

        logger.info("=" * 60)
        logger.info("下载统计:")
        logger.success(
            f"{_t('total download')} {app.total_download_task}, "
            f"{_t('total upload file')} "
            f"{app.cloud_drive_config.total_upload_success_file_count}"
        )

        # Report remaining failed tasks
        try:
            async def get_final_failed_tasks():
                total = 0
                for chat_id, _ in app.chat_download_config.items():
                    failed_tasks = await load_failed_tasks(chat_id)
                    total += len(failed_tasks)
                return total

            total_failed_tasks = run_async_sync(get_final_failed_tasks(), timeout=30)
            if total_failed_tasks > 0:
                logger.warning(f"仍有 {total_failed_tasks} 个任务待重试，将在下次启动时继续重试")
        except Exception as e:
            logger.error(f"统计失败任务时出错: {e}")

        logger.info(f"队列管理器统计: 添加任务={queue_manager.task_added}, 处理任务={queue_manager.task_processed}")
        logger.info("=" * 60)


if __name__ == "__main__":
    if _check_config():
        main()