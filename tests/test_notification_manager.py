"""Tests for NotificationManager."""
import media_downloader as md
from services.notifier import NotificationManager


def _notifications(bark_enabled=False, bark_events=None, syno_enabled=False, syno_events=None):
    return {
        "bark": {"enabled": bark_enabled, "events_to_notify": bark_events or []},
        "synology_chat": {"enabled": syno_enabled, "events_to_notify": syno_events or []},
        "global": {},
    }


def test_load_config_both_enabled():
    md.app.notifications = _notifications(True, ["startup"], True, ["shutdown"])
    nm = NotificationManager()
    nm.load_config()
    assert nm.bark_enabled is True
    assert nm.synology_chat_enabled is True
    assert nm.bark_config == {"enabled": True, "events_to_notify": ["startup"]}
    assert nm.synology_chat_config == {"enabled": True, "events_to_notify": ["shutdown"]}


def test_load_config_all_disabled():
    md.app.notifications = _notifications()
    nm = NotificationManager()
    nm.load_config()
    assert nm.bark_enabled is False
    assert nm.synology_chat_enabled is False


def test_should_notify_bark_enabled_event():
    md.app.notifications = _notifications(True, ["startup"])
    nm = NotificationManager()
    nm.load_config()
    assert nm.should_notify("startup", "bark") is True
    assert nm.should_notify("shutdown", "bark") is False


def test_should_notify_bark_disabled():
    md.app.notifications = _notifications(False, ["startup"])
    nm = NotificationManager()
    nm.load_config()
    assert nm.should_notify("startup", "bark") is False


def test_should_notify_synology():
    md.app.notifications = _notifications(syno_enabled=True, syno_events=["disk_space"])
    nm = NotificationManager()
    nm.load_config()
    assert nm.should_notify("disk_space", "synology_chat") is True
    assert nm.should_notify("startup", "synology_chat") is False


def test_should_notify_any_channel():
    md.app.notifications = _notifications(True, ["startup"], False, [])
    nm = NotificationManager()
    nm.load_config()
    assert nm.should_notify("startup") is True
    assert nm.should_notify("shutdown") is False


def test_should_notify_any_channel_no_match():
    md.app.notifications = _notifications(True, [], False, [])
    nm = NotificationManager()
    nm.load_config()
    assert nm.should_notify("startup") is False
