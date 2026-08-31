"""Tests for workers.monitor — monitoring tasks."""
import asyncio
from unittest import mock

import core.context as ctx
import media_downloader as md
from module.cloud_drive import CloudDriveConfig
from workers.monitor import (
    cloud_health_monitor_task,
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
        nm.synology_chat_config = {
            "disk_space_threshold_gb": 10.0,
            "space_check_interval": 300,
        }
        nm.send_disk_space_notification = mock.AsyncMock()

        with mock.patch(
            "workers.monitor.check_disk_space",
            new=mock.AsyncMock(return_value=(True, 20.0, 100.0)),
        ), mock.patch(
            "workers.monitor.asyncio.sleep", new=stop_after_sleep
        ), mock.patch(
            "workers.monitor.disk_monitor"
        ) as dm:
            dm.space_low = False
            dm.cloud_space_low = False
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
        ), mock.patch(
            "workers.monitor.asyncio.sleep", new=stop_after_sleep
        ), mock.patch(
            "workers.monitor.disk_monitor"
        ) as dm:
            dm.stats_since_last_notification = {}
            asyncio.run(stats_notification_task())

    nm.send_stats_notification.assert_awaited()
    md.app.is_running = True


def test_disk_space_monitor_task_cloud_space_low_sets_flag():
    async def stop_after_sleep(*args, **kwargs):
        md.app.is_running = False

    md.app.cloud_drive_config = CloudDriveConfig(
        enable_upload_file=True,
        upload_adapter="rclone",
        remote_dir="MyRemote:telegram",
        cloud_space_threshold_gb=10.0,
    )
    with mock.patch("workers.monitor.notification_manager") as nm:
        nm.bark_enabled = True
        nm.synology_chat_enabled = True
        nm.bark_config = {"disk_space_threshold_gb": 10.0, "space_check_interval": 300}
        nm.synology_chat_config = {
            "disk_space_threshold_gb": 10.0,
            "space_check_interval": 300,
        }
        nm.send_disk_space_notification = mock.AsyncMock()

        with mock.patch(
            "workers.monitor.check_disk_space",
            new=mock.AsyncMock(return_value=(True, 20.0, 100.0)),
        ), mock.patch(
            "module.cloud_drive.check_cloud_space",
            new=mock.AsyncMock(return_value=(False, 5.0, 100.0)),
        ), mock.patch(
            "workers.monitor.asyncio.sleep", new=stop_after_sleep
        ), mock.patch(
            "workers.monitor.disk_monitor"
        ) as dm:
            dm.space_low = False
            dm.cloud_space_low = False
            dm.last_notification_time = 0
            dm.paused_workers = set()
            asyncio.run(disk_space_monitor_task())

    assert dm.cloud_space_low is True  # 云端空间不足 → 置标志
    # 最后一次通知应携带云端信息
    call_args = nm.send_disk_space_notification.call_args
    assert call_args is not None and "云端空间" in call_args[0][4]
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()


def test_disk_space_monitor_task_recovers_when_local_and_cloud_ok():
    async def stop_after_sleep(*args, **kwargs):
        md.app.is_running = False

    md.app.cloud_drive_config = CloudDriveConfig(
        enable_upload_file=True,
        upload_adapter="rclone",
        remote_dir="MyRemote:telegram",
        cloud_space_threshold_gb=10.0,
    )
    with mock.patch("workers.monitor.notification_manager") as nm:
        nm.bark_enabled = True
        nm.synology_chat_enabled = True
        nm.bark_config = {"disk_space_threshold_gb": 10.0, "space_check_interval": 300}
        nm.synology_chat_config = {
            "disk_space_threshold_gb": 10.0,
            "space_check_interval": 300,
        }
        nm.send_disk_space_notification = mock.AsyncMock()

        with mock.patch(
            "workers.monitor.check_disk_space",
            new=mock.AsyncMock(return_value=(True, 20.0, 100.0)),
        ), mock.patch(
            "module.cloud_drive.check_cloud_space",
            new=mock.AsyncMock(return_value=(True, 50.0, 100.0)),
        ), mock.patch(
            "workers.monitor.asyncio.sleep", new=stop_after_sleep
        ), mock.patch(
            "workers.monitor.disk_monitor"
        ) as dm:
            dm.space_low = True
            dm.cloud_space_low = True
            dm.last_notification_time = 0
            dm.paused_workers = {1, 2}
            asyncio.run(disk_space_monitor_task())

    assert dm.space_low is False
    assert dm.cloud_space_low is False
    assert dm.paused_workers == set()  # 本地+云端都恢复 → 清空暂停
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()


def test_cloud_health_monitor_task_disabled_when_cloud_not_enabled():
    md.app.cloud_drive_config = CloudDriveConfig()  # enable_upload_file=False
    with mock.patch(
        "module.cloud_drive.verify_rclone_remote",
        new=mock.AsyncMock(return_value=(True, "ok")),
    ) as mock_verify:
        asyncio.run(cloud_health_monitor_task())
    mock_verify.assert_not_awaited()
    md.app.cloud_drive_config = CloudDriveConfig()


def test_cloud_health_monitor_task_recovers_after_verification_success():
    async def stop_after_sleep(*args, **kwargs):
        md.app.is_running = False

    md.app.cloud_drive_config = CloudDriveConfig(
        enable_upload_file=True,
        upload_adapter="rclone",
        remote_dir="MyRemote:telegram",
    )
    with mock.patch("workers.monitor.disk_monitor") as dm, mock.patch(
        "workers.monitor.asyncio.sleep", new=stop_after_sleep
    ), mock.patch(
        "module.cloud_drive.verify_rclone_remote",
        new=mock.AsyncMock(return_value=(True, "恢复成功")),
    ) as mock_verify, mock.patch(
        "workers.monitor.notification_manager"
    ) as nm, mock.patch.object(
        ctx, "cloud_upload_ok", False
    ):
        dm.last_cloud_recheck_time = 0
        dm.cloud_recheck_interval = 300
        nm.send_event_notification = mock.AsyncMock()
        asyncio.run(cloud_health_monitor_task())

        assert ctx.cloud_upload_ok is True  # 重验成功 → 标志恢复
        mock_verify.assert_awaited_once()
        nm.send_event_notification.assert_awaited_once()
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()


def test_cloud_health_monitor_task_stays_down_when_verification_fails():
    async def stop_after_sleep(*args, **kwargs):
        md.app.is_running = False

    md.app.cloud_drive_config = CloudDriveConfig(
        enable_upload_file=True,
        upload_adapter="rclone",
        remote_dir="MyRemote:telegram",
    )
    with mock.patch("workers.monitor.disk_monitor") as dm, mock.patch(
        "workers.monitor.asyncio.sleep", new=stop_after_sleep
    ), mock.patch(
        "module.cloud_drive.verify_rclone_remote",
        new=mock.AsyncMock(return_value=(False, "仍不可用")),
    ) as mock_verify, mock.patch(
        "workers.monitor.notification_manager"
    ) as nm, mock.patch.object(
        ctx, "cloud_upload_ok", False
    ):
        dm.last_cloud_recheck_time = 0
        dm.cloud_recheck_interval = 300
        nm.send_event_notification = mock.AsyncMock()
        asyncio.run(cloud_health_monitor_task())

        assert ctx.cloud_upload_ok is False  # 仍未恢复
        mock_verify.assert_awaited_once()
        nm.send_event_notification.assert_not_awaited()  # 恢复通知不应发送
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()


def test_queue_monitor_task_enabled_runs_once():
    async def stop_after_sleep(*args, **kwargs):
        md.app.is_running = False

    with mock.patch("workers.monitor.notification_manager") as nm:
        nm.should_notify.side_effect = [True, True]
        nm.global_config = {"queue_monitor_interval": 300}
        nm.send_event_notification = mock.AsyncMock()

        with mock.patch("workers.monitor.ctx") as mock_ctx, mock.patch(
            "workers.monitor.queue_manager"
        ) as qm, mock.patch(
            "workers.monitor.asyncio.sleep", new=stop_after_sleep
        ), mock.patch(
            "workers.monitor.disk_monitor"
        ) as dm:
            mock_ctx.download_queue.qsize.return_value = 10
            qm.download_queue_size = 10
            qm.max_download_tasks = 4
            dm.paused_workers = set()
            asyncio.run(queue_monitor_task())

    nm.send_event_notification.assert_awaited()
    md.app.is_running = True
