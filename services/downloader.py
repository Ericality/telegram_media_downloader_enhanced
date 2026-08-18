"""Download services — core download logic for messages."""
import asyncio
import os
import time
from typing import List, Optional, Union

import pyrogram
from loguru import logger

import core.context as ctx
from core.context import RETRY_TIME_OUT, _media_seen, app, queue_manager
from core.models import DownloadStatus, TaskNode
from core.storage import (
    _save_duplicate_count,
    _save_seen_media,
    record_failed_task,
    remove_failed_task,
)
from module.download_stat import update_download_status
from module.language import _t
from module.pyrogram_extension import (
    fetch_message,
    record_download_status,
    report_bot_download_status,
    update_cloud_upload_stat,
    upload_telegram_chat,
)
from utils.format import validate_title
from workers.monitor import disk_monitor


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

    from workers.download import _is_exist
    if _is_exist(file_name):
        return DownloadStatus.SkipDownload, None

    content = message.text or message.caption or ""
    with open(file_name, "w", encoding="utf-8") as f:
        f.write(content)

    return DownloadStatus.SuccessDownload, file_name


@record_download_status
async def download_media(
        client: pyrogram.client.Client,
        message: pyrogram.types.Message,
        media_types: List[str],
        file_formats: dict,
        node: TaskNode,
):
    """Download media from a Telegram message."""
    from workers.download import (
        _can_download,
        _check_download_finish,
        _check_timeout,
        _get_media_meta,
        _is_exist,
        _move_to_download_path,
    )

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

