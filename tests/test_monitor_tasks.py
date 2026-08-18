"""Tests for workers.monitor — monitoring tasks."""
import asyncio
from unittest import mock

import media_downloader as md
from workers.monitor import (
    disk_space_monitor_task,
    queue_monitor_task,
    stats_notification_task,
)


def test_disk_space_monitor_task_disabled_returns():
    with mock.patch("workers.monitor.notification_manager") as nm:
        nm.bark_enabled = False
        nm.synology_chat_enabled = False
        asyncio.run(disk_space_monitor_task())


def test_stats_notification_task_disabled_returns():
    with mock.patch("workers.monitor.notification_manager") as nm:
        nm.should_notify.return_value = False
        asyncio.run(stats_notification_task())


def test_queue_monitor_task_disabled_returns():
    with mock.patch("workers.monitor.notification_manager") as nm:
        nm.should_notify.return_value = False
        asyncio.run(queue_monitor_task())


def test_disk_space_monitor_task_enabled_runs_once():
    async def stop_after_sleep(*args, **kwargs):
        md.app.is_running = False

    with mock.patch("workers.monitor.notification_manager") as nm:
        nm.bark_enabled = True
        nm.synology_chat_enabled = True
        nm.bark_config = {"disk_space_threshold_gb": 10.0, "space_check_interval": 300}
        nm.synology_chat_config = {"disk_space_threshold_gb": 10.0, "space_check_interval": 300}
        nm.send_disk_space_notification = mock.AsyncMock()

        with mock.patch(
            "workers.monitor.check_disk_space", new=mock.AsyncMock(return_value=(True, 20.0, 100.0))
        ), mock.patch("workers.monitor.asyncio.sleep", new=stop_after_sleep), \
                mock.patch("workers.monitor.disk_monitor") as dm:
            dm.space_low = False
            dm.last_notification_time = 0
            dm.paused_workers = set()
            asyncio.run(disk_space_monitor_task())

    assert nm.send_disk_space_notification.await_count >= 1
    md.app.is_running = True


def test_stats_notification_task_enabled_runs_once():
    async def stop_after_sleep(*args, **kwargs):
        md.app.is_running = False

    with mock.patch("workers.monitor.notification_manager") as nm:
        nm.should_notify.return_value = True
        nm.bark_config = {"stats_notification_interval": 3600}
        nm.global_config = {"stats_notification_interval": 3600}
        nm.send_stats_notification = mock.AsyncMock()

        with mock.patch(
            "workers.monitor.collect_stats_async",
            new=mock.AsyncMock(return_value={"uptime": "1s"}),
        ), mock.patch("workers.monitor.asyncio.sleep", new=stop_after_sleep), \
                mock.patch("workers.monitor.disk_monitor") as dm:
            dm.stats_since_last_notification = {}
            asyncio.run(stats_notification_task())

    nm.send_stats_notification.assert_awaited()
    md.app.is_running = True


def test_queue_monitor_task_enabled_runs_once():
    async def stop_after_sleep(*args, **kwargs):
        md.app.is_running = False

    with mock.patch("workers.monitor.notification_manager") as nm:
        nm.should_notify.side_effect = [True, True]
        nm.global_config = {"queue_monitor_interval": 300}
        nm.send_event_notification = mock.AsyncMock()

        with mock.patch("workers.monitor.ctx") as mock_ctx, \
                mock.patch("workers.monitor.queue_manager") as qm, \
                mock.patch("workers.monitor.asyncio.sleep", new=stop_after_sleep), \
                mock.patch("workers.monitor.disk_monitor") as dm:
            mock_ctx.download_queue.qsize.return_value = 10
            qm.download_queue_size = 10
            qm.max_download_tasks = 4
            dm.paused_workers = set()
            asyncio.run(queue_monitor_task())

    nm.send_event_notification.assert_awaited()
    md.app.is_running = True
