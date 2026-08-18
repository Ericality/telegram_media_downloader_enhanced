"""Tests for add_download_task queueing logic."""
import asyncio
from unittest import mock

import core.context as ctx
import media_downloader as md
from media_downloader import DownloadStatus, TaskNode

from .test_common import MockMessage


def _reset():
    md.app.force_exit = False
    md.app.is_running = True
    md.app.chat_download_config = {}
    ctx.download_queue = asyncio.Queue()
    md.queue_manager.task_added = 0
    md.queue_manager.task_processed = 0


def test_add_download_task_empty_message():
    _reset()
    message = MockMessage(id=5, media=True, empty=True, chat_id=-123)
    node = TaskNode(chat_id=-123)
    with mock.patch("media_downloader.remove_failed_task", new=mock.AsyncMock()):
        ok = asyncio.run(md.add_download_task(message, node))
    assert ok is False


def test_add_download_task_exit_signal():
    _reset()
    md.app.force_exit = True
    message = MockMessage(id=5, media=True, chat_id=-123)
    node = TaskNode(chat_id=-123)
    ok = asyncio.run(md.add_download_task(message, node))
    assert ok is False


def test_add_download_task_success():
    _reset()
    message = MockMessage(id=5, media=True, chat_id=-123, chat_title="t")
    node = TaskNode(chat_id=-123)
    with mock.patch(
        "media_downloader.remove_failed_task", new=mock.AsyncMock(return_value=False)
    ):
        ok = asyncio.run(md.add_download_task(message, node))

    assert ok is True
    assert ctx.download_queue.qsize() == 1
    assert node.download_status[5] == DownloadStatus.Downloading
    assert node.total_task == 1
    assert md.queue_manager.task_added == 1
