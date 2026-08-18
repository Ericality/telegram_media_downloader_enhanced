"""Tests for notification sending functions (boundary checks + enqueue + Bark send)."""
import asyncio
from unittest import mock

import core.context as ctx
import media_downloader as md


def test_bark_sync_disabled_returns_false():
    md.app.bark_notification = {}
    ok = asyncio.run(md.send_bark_notification_sync("title", "body"))
    assert ok is False


def test_bark_sync_no_url_returns_false():
    md.app.bark_notification = {"enabled": True, "url": ""}
    ok = asyncio.run(md.send_bark_notification_sync("title", "body"))
    assert ok is False


def test_bark_sync_success():
    md.app.bark_notification = {"enabled": True, "url": "https://api.day.app/key"}

    response = mock.MagicMock()
    response.status = 200
    response.__aenter__ = mock.AsyncMock(return_value=response)
    response.__aexit__ = mock.AsyncMock(return_value=False)

    session = mock.MagicMock()
    session.post = mock.MagicMock(return_value=response)
    session.__aenter__ = mock.AsyncMock(return_value=session)
    session.__aexit__ = mock.AsyncMock(return_value=False)

    with mock.patch("media_downloader.aiohttp.ClientSession", return_value=session), \
            mock.patch("media_downloader.aiohttp.ClientTimeout", return_value=mock.MagicMock()):
        ok = asyncio.run(md.send_bark_notification_sync("title", "body"))

    assert ok is True
    session.post.assert_called_once()


def test_synology_sync_disabled_returns_false():
    md.app.notifications = {"synology_chat": {"enabled": False}}
    ok = asyncio.run(md.send_synology_chat_notification_sync("title", "msg"))
    assert ok is False


def test_synology_sync_no_webhook_returns_false():
    md.app.notifications = {"synology_chat": {"enabled": True, "webhook_url": ""}}
    ok = asyncio.run(md.send_synology_chat_notification_sync("title", "msg"))
    assert ok is False


def test_bark_enqueue():
    ctx.notify_queue = asyncio.Queue()
    ok = asyncio.run(md.send_bark_notification("title", "body"))
    assert ok is True
    assert ctx.notify_queue.qsize() == 1


def test_synology_enqueue():
    ctx.notify_queue = asyncio.Queue()
    ok = asyncio.run(md.send_synology_chat_notification("title", "msg"))
    assert ok is True
    assert ctx.notify_queue.qsize() == 1
