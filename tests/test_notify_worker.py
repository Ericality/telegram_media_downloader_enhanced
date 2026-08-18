"""Tests for workers.notify.notify_worker."""
import asyncio
import time
from unittest import mock

import core.context as ctx
import media_downloader as md
from workers.notify import notify_worker


def _make_bark_task():
    return {
        "type": "bark_notification",
        "title": "title",
        "body": "body",
        "url": None,
        "group": None,
        "level": None,
        "create_time": "2026-01-01 00:00:00",
        "queue_time": time.time(),
    }


def _make_synology_task():
    return {
        "type": "synology_chat_notification",
        "title": "title",
        "message": "msg",
        "level": "info",
        "webhook_url": None,
        "bot_name": None,
        "bot_avatar": None,
        "mention_users": [],
        "mention_channels": [],
        "create_time": "2026-01-01 00:00:00",
        "queue_time": time.time(),
    }


def test_notify_worker_bark_task():
    md.app.is_running = False
    md.app.force_exit = False
    ctx.notify_queue = asyncio.Queue()
    ctx.notify_queue.put_nowait(_make_bark_task())
    with mock.patch(
        "workers.notify.send_bark_notification_sync", new=mock.AsyncMock(return_value=True)
    ) as mock_send:
        asyncio.run(notify_worker(1))
    mock_send.assert_awaited_once()
    md.app.is_running = True


def test_notify_worker_synology_task():
    md.app.is_running = False
    md.app.force_exit = False
    ctx.notify_queue = asyncio.Queue()
    ctx.notify_queue.put_nowait(_make_synology_task())
    with mock.patch(
        "workers.notify.send_synology_chat_notification_sync", new=mock.AsyncMock(return_value=True)
    ) as mock_send:
        asyncio.run(notify_worker(1))
    mock_send.assert_awaited_once()
    md.app.is_running = True


def test_notify_worker_exits_when_queue_empty():
    md.app.is_running = False
    md.app.force_exit = False
    ctx.notify_queue = asyncio.Queue()
    asyncio.run(notify_worker(1))  # 空队列 + 退出信号 → 立即退出
    md.app.is_running = True
