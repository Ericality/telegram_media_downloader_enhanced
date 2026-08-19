"""Tests for notification sending functions (boundary checks + enqueue + Bark send)."""
import asyncio
from unittest import mock

import core.context as ctx
import media_downloader as md
from services.notifier import (send_bark_notification, send_bark_notification_sync, send_synology_chat_notification, send_synology_chat_notification_sync)


def test_bark_sync_disabled_returns_false():
    md.app.bark_notification = {}
    ok = asyncio.run(send_bark_notification_sync("title", "body"))
    assert ok is False


def test_bark_sync_no_url_returns_false():
    md.app.bark_notification = {"enabled": True, "url": ""}
    ok = asyncio.run(send_bark_notification_sync("title", "body"))
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

    with mock.patch("services.notifier.aiohttp.ClientSession", return_value=session), \
            mock.patch("services.notifier.aiohttp.ClientTimeout", return_value=mock.MagicMock()):
        ok = asyncio.run(send_bark_notification_sync("title", "body"))

    assert ok is True
    session.post.assert_called_once()


def test_synology_sync_disabled_returns_false():
    md.app.notifications = {"synology_chat": {"enabled": False}}
    ok = asyncio.run(send_synology_chat_notification_sync("title", "msg"))
    assert ok is False


def test_synology_sync_no_webhook_returns_false():
    md.app.notifications = {"synology_chat": {"enabled": True, "webhook_url": ""}}
    ok = asyncio.run(send_synology_chat_notification_sync("title", "msg"))
    assert ok is False


def test_bark_enqueue():
    ctx.notify_queue = asyncio.Queue()
    ok = asyncio.run(send_bark_notification("title", "body"))
    assert ok is True
    assert ctx.notify_queue.qsize() == 1


def test_synology_enqueue():
    ctx.notify_queue = asyncio.Queue()
    ok = asyncio.run(send_synology_chat_notification("title", "msg"))
    assert ok is True
    assert ctx.notify_queue.qsize() == 1


def _bark_session(statuses, text=""):
    """Build a mocked aiohttp session returning responses in sequence."""
    session = mock.MagicMock()
    responses = []
    for status in statuses:
        resp = mock.MagicMock()
        resp.status = status
        resp.text = mock.AsyncMock(return_value=text)
        resp.__aenter__ = mock.AsyncMock(return_value=resp)
        resp.__aexit__ = mock.AsyncMock(return_value=False)
        responses.append(resp)
    session.post = mock.MagicMock(side_effect=responses)
    session.__aenter__ = mock.AsyncMock(return_value=session)
    session.__aexit__ = mock.AsyncMock(return_value=False)
    return session


def _http_ctx(session):
    """Return an ExitStack patching aiohttp ClientSession/ClientTimeout."""
    from contextlib import ExitStack

    stack = ExitStack()
    stack.enter_context(mock.patch("services.notifier.aiohttp.ClientSession", return_value=session))
    stack.enter_context(mock.patch("services.notifier.aiohttp.ClientTimeout", return_value=mock.MagicMock()))
    return stack


def test_bark_sync_client_error_no_retry():
    md.app.bark_notification = {"enabled": True, "url": "https://api.day.app/key"}
    session = _bark_session([400])
    with _http_ctx(session):
        ok = asyncio.run(send_bark_notification_sync("t", "b"))
    assert ok is False


def test_bark_sync_server_error_retry_then_success():
    md.app.bark_notification = {"enabled": True, "url": "https://api.day.app/key"}
    session = _bark_session([500, 500, 200])
    with _http_ctx(session), mock.patch("services.notifier.asyncio.sleep", new=mock.AsyncMock()):
        ok = asyncio.run(send_bark_notification_sync("t", "b"))
    assert ok is True


def test_bark_sync_timeout_returns_false():
    md.app.bark_notification = {"enabled": True, "url": "https://api.day.app/key"}
    session = mock.MagicMock()
    resp = mock.MagicMock()
    resp.status = 500
    resp.text = mock.AsyncMock(side_effect=asyncio.TimeoutError)
    resp.__aenter__ = mock.AsyncMock(return_value=resp)
    resp.__aexit__ = mock.AsyncMock(return_value=False)
    session.post = mock.MagicMock(return_value=resp)
    session.__aenter__ = mock.AsyncMock(return_value=session)
    session.__aexit__ = mock.AsyncMock(return_value=False)
    with _http_ctx(session), mock.patch("services.notifier.asyncio.sleep", new=mock.AsyncMock()):
        ok = asyncio.run(send_bark_notification_sync("t", "b"))
    assert ok is False


def test_synology_sync_success_json():
    md.app.notifications = {"synology_chat": {"enabled": True, "webhook_url": "https://example.com/webhook"}}
    session = _bark_session([200], text='{"success": true}')
    with _http_ctx(session):
        ok = asyncio.run(send_synology_chat_notification_sync("t", "m"))
    assert ok is True


def test_synology_sync_success_non_json():
    md.app.notifications = {"synology_chat": {"enabled": True, "webhook_url": "https://example.com/webhook"}}
    session = _bark_session([200], text="not json at all")
    with _http_ctx(session):
        ok = asyncio.run(send_synology_chat_notification_sync("t", "m"))
    assert ok is True


def test_synology_sync_http_error_returns_false():
    md.app.notifications = {"synology_chat": {"enabled": True, "webhook_url": "https://example.com/webhook"}}
    session = _bark_session([500, 500, 500], text="error")
    with _http_ctx(session), mock.patch("services.notifier.asyncio.sleep", new=mock.AsyncMock()):
        ok = asyncio.run(send_synology_chat_notification_sync("t", "m"))
    assert ok is False


def test_bark_sync_uses_configured_sound():
    md.app.bark_notification = {
        "enabled": True, "url": "https://api.day.app/key", "sound": "silence"
    }
    session = _bark_session([200])
    with _http_ctx(session):
        ok = asyncio.run(send_bark_notification_sync("t", "b"))
    assert ok is True
    payload = session.post.call_args.kwargs["json"]
    assert payload["sound"] == "silence"


def test_bark_sync_default_sound_alarm():
    md.app.bark_notification = {"enabled": True, "url": "https://api.day.app/key"}
    session = _bark_session([200])
    with _http_ctx(session):
        asyncio.run(send_bark_notification_sync("t", "b"))
    payload = session.post.call_args.kwargs["json"]
    assert payload["sound"] == "alarm"
