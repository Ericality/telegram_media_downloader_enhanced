"""Tests for download_media core decision logic (dedup / format / existence / success)."""
import asyncio
import os
from datetime import datetime
from unittest import mock

import core.context as ctx
import media_downloader as md
from core.models import DownloadStatus, TaskNode
from module.pyrogram_extension import reset_download_cache
from services.downloader import download_media, download_task

from .test_common import MockMessage, MockVideo


async def _async_identity(client, message):
    return message


async def _async_noop(*args, **kwargs):
    return None


async def _meta_mp4(chat_id, message, media_obj, _type):
    return ("/final/sample.mp4", "/tmp/sample.mp4", "mp4")


async def _meta_avi(chat_id, message, media_obj, _type):
    return ("/final/sample.mp4", "/tmp/sample.mp4", "avi")


def _make_message(file_unique_id=None):
    video = MockVideo(file_name="sample.mp4", mime_type="video/mp4")
    video.file_unique_id = file_unique_id
    return MockMessage(
        id=5,
        media=True,
        chat_id=-123,
        chat_title="test_chat",
        date=datetime(2023, 1, 1, 12, 0, 0),
        video=video,
    )


def _reset_state():
    import tempfile

    reset_download_cache()
    md.app.hide_file_name = False
    md.app.download_duplicate_threshold = 5
    md.app.session_file_path = tempfile.mkdtemp()
    ctx._media_download_count.clear()
    ctx._media_seen.clear()


def test_download_media_success():
    _reset_state()
    message = _make_message()
    node = TaskNode(chat_id=-123)
    client = mock.AsyncMock()
    client.download_media = mock.AsyncMock(return_value="/tmp/sample.mp4")

    with mock.patch(
        "services.downloader.fetch_message", side_effect=_async_identity
    ), mock.patch(
        "workers.download._get_media_meta", side_effect=_meta_mp4
    ), mock.patch(
        "workers.download._is_exist", return_value=False
    ), mock.patch(
        "workers.download._check_download_finish"
    ), mock.patch(
        "workers.download._move_to_download_path"
    ), mock.patch(
        "services.downloader._save_duplicate_count"
    ), mock.patch(
        "services.downloader._save_seen_media"
    ), mock.patch(
        "services.downloader.asyncio.sleep", side_effect=_async_noop
    ):
        status, fname = asyncio.run(
            download_media(client, message, ["video"], {"video": ["all"]}, node)
        )

    assert status == DownloadStatus.SuccessDownload
    assert fname == "/final/sample.mp4"
    client.download_media.assert_awaited_once()


def test_download_media_skip_duplicate():
    _reset_state()
    ctx._media_download_count["UNIQUE_1"] = 5
    message = _make_message(file_unique_id="UNIQUE_1")
    node = TaskNode(chat_id=-123)
    client = mock.AsyncMock()

    with mock.patch(
        "services.downloader.fetch_message", side_effect=_async_identity
    ), mock.patch("services.downloader._save_duplicate_count"):
        status, fname = asyncio.run(
            download_media(client, message, ["video"], {"video": ["all"]}, node)
        )

    assert status == DownloadStatus.SkipDownload
    assert fname is None
    client.download_media.assert_not_called()


def test_download_media_skip_disallowed_format():
    _reset_state()
    message = _make_message()
    node = TaskNode(chat_id=-123)
    client = mock.AsyncMock()

    with mock.patch(
        "services.downloader.fetch_message", side_effect=_async_identity
    ), mock.patch(
        "workers.download._get_media_meta", side_effect=_meta_avi
    ), mock.patch(
        "workers.download._is_exist", return_value=False
    ):
        status, fname = asyncio.run(
            download_media(client, message, ["video"], {"video": ["mp4"]}, node)
        )

    assert status == DownloadStatus.SkipDownload
    assert fname is None
    client.download_media.assert_not_called()


def test_download_media_skip_existing_file():
    _reset_state()
    message = _make_message()
    node = TaskNode(chat_id=-123)
    client = mock.AsyncMock()

    with mock.patch(
        "services.downloader.fetch_message", side_effect=_async_identity
    ), mock.patch(
        "workers.download._get_media_meta", side_effect=_meta_mp4
    ), mock.patch(
        "workers.download._is_exist", return_value=True
    ), mock.patch(
        "media_downloader.os.path.getsize", return_value=1024
    ):
        status, fname = asyncio.run(
            download_media(client, message, ["video"], {"video": ["all"]}, node)
        )

    assert status == DownloadStatus.SkipDownload
    assert fname is None
    client.download_media.assert_not_called()


def test_download_media_skip_no_media():
    _reset_state()
    message = MockMessage(id=5, media=False, chat_id=-123, chat_title="test_chat")
    node = TaskNode(chat_id=-123)
    client = mock.AsyncMock()

    with mock.patch("services.downloader.fetch_message", side_effect=_async_identity):
        status, fname = asyncio.run(
            download_media(client, message, ["video"], {"video": ["all"]}, node)
        )

    assert status == DownloadStatus.SkipDownload
    assert fname is None
    client.download_media.assert_not_called()


def test_download_task_survives_file_removed_by_upload(tmp_path):
    """回归：上传把本地文件移走后，不得把这次成功记成失败任务。

    历史缺陷：`file_size` 在**上传之后**才 `getsize`，而 after_upload_file_delete=true 时
    上传走 `rclone move` 会移走本地文件 ⇒ FileNotFoundError 被 worker 的 `except OSError`
    当成"网络连接错误"并 `record_failed_task`（生产实测：失败列表 97% 是这种假失败）。
    """
    md.app.is_running = True
    md.app.force_exit = False
    md.app.media_types = ["video"]
    md.app.file_formats = {}
    md.app.enable_download_txt = False
    md.app.hide_file_name = False

    media_path = tmp_path / "sample.mp4"
    media_path.write_bytes(b"x" * 1234)
    node = TaskNode(chat_id=-123)
    message = MockMessage(id=5, media=True, chat_id=-123, chat_title="chat")

    async def fake_download_media(client, msg, media_types, file_formats, task_node):
        return DownloadStatus.SuccessDownload, str(media_path)

    async def fake_upload_file(path, callback, context_tuple):
        os.remove(path)  # 模拟 rclone move：上传成功后本地文件消失
        return True

    with mock.patch(
        "services.downloader.download_media", new=fake_download_media
    ), mock.patch(
        "services.downloader.remove_failed_task", new=mock.AsyncMock()
    ), mock.patch(
        "services.downloader.upload_telegram_chat", new=mock.AsyncMock()
    ), mock.patch(
        "services.downloader.report_bot_download_status", new=mock.AsyncMock()
    ) as mock_report, mock.patch.object(
        md.app, "upload_file", new=fake_upload_file
    ):
        asyncio.run(download_task(mock.MagicMock(), message, node))

    assert not media_path.exists(), "上传应当已把本地文件移走（测试前提）"
    mock_report.assert_awaited_once()
    # 第 4 个位置参数是 file_size —— 必须取到"上传前"的大小，而不是 0/异常
    assert mock_report.await_args.args[3] == 1234
    md.app.media_types = ["audio", "document", "photo", "video", "voice", "animation"]
