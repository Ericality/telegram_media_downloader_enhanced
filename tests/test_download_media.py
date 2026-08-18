"""Tests for download_media core decision logic (dedup / format / existence / success)."""
import asyncio
from datetime import datetime
from unittest import mock

import core.context as ctx
import media_downloader as md
from core.models import DownloadStatus, TaskNode
from services.downloader import download_media
from module.pyrogram_extension import reset_download_cache

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

    with mock.patch("services.downloader.fetch_message", side_effect=_async_identity), \
            mock.patch("workers.download._get_media_meta", side_effect=_meta_mp4), \
            mock.patch("workers.download._is_exist", return_value=False), \
            mock.patch("workers.download._check_download_finish"), \
            mock.patch("workers.download._move_to_download_path"), \
            mock.patch("services.downloader._save_duplicate_count"), \
            mock.patch("services.downloader._save_seen_media"), \
            mock.patch("services.downloader.asyncio.sleep", side_effect=_async_noop):
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

    with mock.patch("services.downloader.fetch_message", side_effect=_async_identity), \
            mock.patch("services.downloader._save_duplicate_count"):
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

    with mock.patch("services.downloader.fetch_message", side_effect=_async_identity), \
            mock.patch("workers.download._get_media_meta", side_effect=_meta_avi), \
            mock.patch("workers.download._is_exist", return_value=False):
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

    with mock.patch("services.downloader.fetch_message", side_effect=_async_identity), \
            mock.patch("workers.download._get_media_meta", side_effect=_meta_mp4), \
            mock.patch("workers.download._is_exist", return_value=True), \
            mock.patch("media_downloader.os.path.getsize", return_value=1024):
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
