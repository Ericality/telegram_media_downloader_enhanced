"""Tests for workers.monitor — monitoring tasks."""
import asyncio
import time
from unittest import mock

import core.context as ctx
import media_downloader as md
from module.cloud_drive import CloudDriveConfig
from workers.monitor import (
    cloud_health_monitor_task,
    cloud_space_check_enabled,
    disk_space_monitor_task,
    queue_monitor_task,
    resolve_cloud_space,
    stats_notification_task,
)


def _cloud_config(enable=True, adapter="rclone", threshold=10.0):
    return CloudDriveConfig(
        enable_upload_file=enable,
        upload_adapter=adapter,
        remote_dir="MyRemote:telegram",
        cloud_space_threshold_gb=threshold,
    )


def _stop_after_checks(n):
    """构造 check_disk_space 桩：第 n 次调用时置退出信号（前 n-1 次只返回结果）。

    监控循环现在是"分片睡眠"（保持退出响应），所以"在 sleep 里置退出标志"会让本轮
    判定被整轮跳过。统一的替代做法 = 以"第 n 次检查完成"作为退出触发点
    （n=2 ⇒ 启动检查 1 次 + 循环体 1 次）。
    """
    counter = {"n": 0}

    async def _fake(threshold_gb=10.0):
        counter["n"] += 1
        if counter["n"] >= n:
            md.app.is_running = False
        return (True, 20.0, 100.0)

    return _fake


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
            "workers.monitor.check_disk_space", new=_stop_after_checks(2)
        ), mock.patch(
            "workers.monitor.asyncio.sleep", new=mock.AsyncMock()
        ), mock.patch(
            "workers.monitor.disk_monitor"
        ) as dm:
            dm.space_low = False
            dm.cloud_space_low = False
            dm.space_low_first_notified = False
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
            "workers.monitor.check_disk_space", new=_stop_after_checks(2)
        ), mock.patch(
            "module.cloud_drive.check_cloud_space",
            new=mock.AsyncMock(return_value=(False, 5.0, 100.0)),
        ), mock.patch(
            "workers.monitor.asyncio.sleep", new=mock.AsyncMock()
        ), mock.patch(
            "workers.monitor.disk_monitor"
        ) as dm:
            dm.space_low = False
            dm.cloud_space_low = False
            dm.space_low_first_notified = False
            dm.last_notification_time = 0
            dm.paused_workers = set()
            asyncio.run(disk_space_monitor_task())

    assert dm.cloud_space_low is True  # 云端空间不足 → 置标志
    # 首条不足通知应响铃(bark_level=None)且携带"云端空间不足"提示
    low_calls = [
        c
        for c in nm.send_disk_space_notification.call_args_list
        if "bark_level" in c.kwargs
    ]
    assert len(low_calls) >= 1
    assert low_calls[-1].kwargs["bark_level"] is None
    assert "云端空间不足" in low_calls[-1].args[4]
    assert dm.space_low_first_notified is True
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()


def test_disk_space_monitor_task_recovers_when_local_and_cloud_ok():
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
            "workers.monitor.check_disk_space", new=_stop_after_checks(2)
        ), mock.patch(
            "module.cloud_drive.check_cloud_space",
            new=mock.AsyncMock(return_value=(True, 50.0, 100.0)),
        ), mock.patch(
            "workers.monitor.asyncio.sleep", new=mock.AsyncMock()
        ), mock.patch(
            "workers.monitor.disk_monitor"
        ) as dm:
            dm.space_low = True
            dm.cloud_space_low = True
            dm.space_low_first_notified = True
            dm.last_notification_time = 0
            dm.paused_workers = {1, 2}
            asyncio.run(disk_space_monitor_task())

    assert dm.space_low is False
    assert dm.cloud_space_low is False
    assert dm.space_low_first_notified is False  # 周期结束，首条标记重置
    assert dm.paused_workers == set()  # 本地+云端都恢复 → 清空暂停
    # 恢复通知应响铃(bark_level=None)
    rec_calls = [
        c
        for c in nm.send_disk_space_notification.call_args_list
        if "bark_level" in c.kwargs
    ]
    assert rec_calls and rec_calls[-1].kwargs["bark_level"] is None
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


def test_disk_space_monitor_task_low_notification_rings_then_passive():
    # 不足持续两轮：首条通知响铃(bark_level=None)，冷却期后的重复通知静音(passive)
    rounds = {"n": 0}
    state = {"dm": None}
    md.app.cloud_drive_config = CloudDriveConfig(
        enable_upload_file=True,
        upload_adapter="rclone",
        remote_dir="MyRemote:telegram",
        cloud_space_threshold_gb=10.0,
    )

    async def fake_disk_check(threshold_gb=10.0):
        rounds["n"] += 1
        if rounds["n"] >= 3:  # 启动检查 1 次 + 循环体 2 次
            md.app.is_running = False
        if rounds["n"] >= 2:
            state["dm"].last_notification_time = 0  # 模拟已到冷却期外
        return (True, 20.0, 100.0)

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
            "workers.monitor.check_disk_space", new=fake_disk_check
        ), mock.patch(
            "module.cloud_drive.check_cloud_space",
            new=mock.AsyncMock(return_value=(False, 5.0, 100.0)),
        ), mock.patch(
            "workers.monitor.asyncio.sleep", new=mock.AsyncMock()
        ), mock.patch(
            "workers.monitor.disk_monitor"
        ) as dm:
            state["dm"] = dm
            dm.space_low = False
            dm.cloud_space_low = False
            dm.space_low_first_notified = False
            dm.last_notification_time = 0
            dm.paused_workers = set()
            asyncio.run(disk_space_monitor_task())

    low_calls = [
        c
        for c in nm.send_disk_space_notification.call_args_list
        if "bark_level" in c.kwargs
    ]
    assert len(low_calls) == 2  # 两轮不足通知（启动检查无 bark_level 不计入）
    assert low_calls[0].kwargs["bark_level"] is None  # 首条响铃
    assert low_calls[1].kwargs["bark_level"] == "passive"  # 持续期间静音
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()


# --------------------------------------------------------------------------
# 云端空间: 查询失败沿用上次成功结果 / 未启用上传不得拦截
# --------------------------------------------------------------------------


def test_cloud_space_check_enabled_matrix():
    """只有 上传开启 + rclone + 阈值>0 才算启用云端空间检查。"""
    assert cloud_space_check_enabled(_cloud_config()) is True
    assert cloud_space_check_enabled(_cloud_config(enable=False)) is False
    assert cloud_space_check_enabled(_cloud_config(adapter="aligo")) is False
    assert cloud_space_check_enabled(_cloud_config(threshold=0)) is False
    assert cloud_space_check_enabled(None) is False


def test_resolve_cloud_space_success_updates_cache():
    from workers.monitor import disk_monitor

    cfg = _cloud_config()
    with mock.patch(
        "module.cloud_drive.check_cloud_space",
        new=mock.AsyncMock(return_value=(True, 50.0, 100.0)),
    ):
        ok, free_gb, total_gb, source, age = asyncio.run(resolve_cloud_space(cfg, 10.0))

    assert (ok, free_gb, total_gb, source, age) == (True, 50.0, 100.0, "live", 0.0)
    cached = disk_monitor.cloud_space_cache
    assert isinstance(cached, tuple) and cached[0] == 50.0 and cached[1] == 100.0


def test_resolve_cloud_space_failure_reuses_last_successful_result():
    """查询失败 ⇒ 沿用上次成功结果（含用当前阈值重新判定）。"""
    from workers.monitor import disk_monitor

    cfg = _cloud_config()
    disk_monitor.cloud_space_cache = (50.0, 100.0, time.time())

    with mock.patch(
        "module.cloud_drive.check_cloud_space",
        new=mock.AsyncMock(return_value=(None, None, None)),
    ):
        ok, free_gb, total_gb, source, age = asyncio.run(resolve_cloud_space(cfg, 10.0))

    assert (ok, free_gb, total_gb, source) == (True, 50.0, 100.0, "cache")
    assert age >= 0

    # 同一份缓存 + 更高的阈值 ⇒ 判定为"不足"（阈值按当前配置重算）
    with mock.patch(
        "module.cloud_drive.check_cloud_space",
        new=mock.AsyncMock(return_value=(None, None, None)),
    ):
        ok, _free, _total, source, _age = asyncio.run(resolve_cloud_space(cfg, 80.0))
    assert (ok, source) == (False, "cache")


def test_resolve_cloud_space_failure_without_cache_fails_open():
    """既查不到又无历史结果 ⇒ None（调用方 fail-open，不暂停下载）。"""
    with mock.patch(
        "module.cloud_drive.check_cloud_space",
        new=mock.AsyncMock(return_value=(None, None, None)),
    ):
        ok, free_gb, total_gb, source, age = asyncio.run(
            resolve_cloud_space(_cloud_config(), 10.0)
        )

    assert (ok, free_gb, total_gb, source, age) == (None, None, None, "unknown", 0.0)


def test_disk_space_monitor_task_keeps_low_state_when_cloud_query_fails():
    """回归：查询失败不得被当成"充足"而误发恢复通知、误清不足标志。

    场景 = 上次成功结果"云端不足" + 本次查询失败。
    期望 = 沿用上次结果判定为不足：cloud_space_low 保持 True、不发恢复通知。
    """

    md.app.cloud_drive_config = _cloud_config()
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
            "workers.monitor.check_disk_space", new=_stop_after_checks(2)
        ), mock.patch(
            "module.cloud_drive.check_cloud_space",
            new=mock.AsyncMock(return_value=(None, None, None)),
        ), mock.patch(
            "workers.monitor.asyncio.sleep", new=mock.AsyncMock()
        ), mock.patch(
            "workers.monitor.disk_monitor"
        ) as dm:
            dm.space_low = False
            dm.cloud_space_low = True  # 上一轮已判定云端不足
            dm.space_low_first_notified = True
            dm.last_notification_time = 0
            dm.paused_workers = set()
            dm.cloud_space_cache = (5.0, 100.0, time.time())  # 上次成功结果: 5GB
            asyncio.run(disk_space_monitor_task())

    assert dm.cloud_space_low is True  # 仍是"不足"，没有被误判为恢复
    assert dm.space_low is True
    recovery_calls = [
        c
        for c in nm.send_disk_space_notification.call_args_list
        if "bark_level" in c.kwargs and c.args[0] is True
    ]
    assert recovery_calls == []  # 不得发"存储空间充足/恢复"通知
    low_calls = [
        c
        for c in nm.send_disk_space_notification.call_args_list
        if "bark_level" in c.kwargs and c.args[0] is False
    ]
    assert low_calls, "沿用上次不足结果时应继续发不足通知"
    assert "沿用" in low_calls[-1].args[4]  # 通知里标明是沿用的旧结果
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()


def test_disk_space_monitor_task_fails_open_without_cloud_cache():
    """查询失败且无历史结果 ⇒ 不因云端暂停（旧行为保留：不发不足通知）。"""

    md.app.cloud_drive_config = _cloud_config()
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
            "workers.monitor.check_disk_space", new=_stop_after_checks(2)
        ), mock.patch(
            "module.cloud_drive.check_cloud_space",
            new=mock.AsyncMock(return_value=(None, None, None)),
        ), mock.patch(
            "workers.monitor.asyncio.sleep", new=mock.AsyncMock()
        ), mock.patch(
            "workers.monitor.disk_monitor"
        ) as dm:
            dm.space_low = False
            dm.cloud_space_low = False
            dm.space_low_first_notified = False
            dm.last_notification_time = 0
            dm.paused_workers = set()
            dm.cloud_space_cache = None
            asyncio.run(disk_space_monitor_task())

    assert dm.space_low is False
    assert dm.cloud_space_low is False
    assert [
        c
        for c in nm.send_disk_space_notification.call_args_list
        if "bark_level" in c.kwargs
    ] == []  # 无历史结果 ⇒ 不因云端判不足
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()


def test_disk_space_monitor_task_ignores_cloud_when_upload_disabled():
    """未启用上传 ⇒ 完全不做云端判定（历史缓存也不得拦截下载）。"""

    md.app.cloud_drive_config = _cloud_config(enable=False)
    with mock.patch("workers.monitor.notification_manager") as nm:
        nm.bark_enabled = True
        nm.synology_chat_enabled = True
        nm.bark_config = {"disk_space_threshold_gb": 10.0, "space_check_interval": 300}
        nm.synology_chat_config = {
            "disk_space_threshold_gb": 10.0,
            "space_check_interval": 300,
        }
        nm.send_disk_space_notification = mock.AsyncMock()
        mock_cloud = mock.AsyncMock(return_value=(None, None, None))

        with mock.patch(
            "workers.monitor.check_disk_space", new=_stop_after_checks(2)
        ), mock.patch(
            "module.cloud_drive.check_cloud_space", new=mock_cloud
        ), mock.patch(
            "workers.monitor.asyncio.sleep", new=mock.AsyncMock()
        ), mock.patch(
            "workers.monitor.disk_monitor"
        ) as dm:
            dm.space_low = False
            dm.cloud_space_low = False
            dm.space_low_first_notified = False
            dm.last_notification_time = 0
            dm.paused_workers = set()
            dm.cloud_space_cache = (0.5, 100.0, time.time())  # 极低的历史值
            asyncio.run(disk_space_monitor_task())

    mock_cloud.assert_not_awaited()  # 未启用上传 ⇒ 连查询都不做
    assert dm.space_low is False  # 不得因云端拦截
    assert dm.cloud_space_low is False
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()


def test_disk_space_monitor_task_uses_configured_interval():
    """回归：轮询间隔必须按配置(300s)，不得被压成固定 5 秒。

    判据 = 累计睡满 6 秒期间，本地/云端检查各只跑"启动那一次"，
    且没有任何单片睡眠超过 1 秒。
    """
    slept = []

    async def fake_sleep(seconds):
        slept.append(seconds)
        if sum(slept) >= 6:
            md.app.is_running = False

    md.app.cloud_drive_config = _cloud_config()
    with mock.patch("workers.monitor.notification_manager") as nm:
        nm.bark_enabled = True
        nm.synology_chat_enabled = True
        nm.bark_config = {"disk_space_threshold_gb": 10.0, "space_check_interval": 300}
        nm.synology_chat_config = {
            "disk_space_threshold_gb": 10.0,
            "space_check_interval": 300,
        }
        nm.send_disk_space_notification = mock.AsyncMock()
        mock_cloud = mock.AsyncMock(return_value=(True, 50.0, 100.0))
        mock_disk = mock.AsyncMock(return_value=(True, 20.0, 100.0))

        with mock.patch("workers.monitor.check_disk_space", new=mock_disk), mock.patch(
            "module.cloud_drive.check_cloud_space", new=mock_cloud
        ), mock.patch("workers.monitor.asyncio.sleep", new=fake_sleep), mock.patch(
            "workers.monitor.disk_monitor"
        ) as dm:
            dm.space_low = False
            dm.cloud_space_low = False
            dm.space_low_first_notified = False
            dm.last_notification_time = 0
            dm.paused_workers = set()
            dm.cloud_space_cache = None
            asyncio.run(disk_space_monitor_task())

    assert slept, "监控循环应当睡眠"
    assert max(slept) <= 1.0  # 分片睡眠，保持退出响应
    assert mock_disk.await_count == 1  # 只有启动那次检查
    assert mock_cloud.await_count == 0  # 300 秒间隔内不得再次查询 rclone
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()
