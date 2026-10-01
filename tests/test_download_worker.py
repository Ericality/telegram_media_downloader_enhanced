"""Tests for workers.download — download_worker and retry_producer."""
import asyncio
import time
from unittest import mock

import core.context as ctx
import media_downloader as md
from core.models import ChatDownloadConfig, DownloadStatus, TaskNode
from module.cloud_drive import CloudDriveConfig
from workers.download import download_chat_task, download_worker, retry_producer

from .test_common import MockMessage


async def _fake_download_task(client, message, node):
    md.app.is_running = False  # 处理完成后触发 worker 退出


def test_download_worker_exits_on_stop():
    md.app.is_running = False
    md.app.force_exit = False
    asyncio.run(download_worker(mock.MagicMock(), 1))
    md.app.is_running = True


def test_download_worker_processes_task():
    md.app.is_running = True
    md.app.force_exit = False
    md.app.bark_notification = {}
    ctx.cloud_upload_ok = True
    ctx.download_semaphore = asyncio.Semaphore(1)
    ctx.download_queue = asyncio.Queue()

    node = TaskNode(chat_id=-123)
    message = MockMessage(id=5, media=True, chat_id=-123)
    ctx.download_queue.put_nowait((message, node))

    client = mock.MagicMock()
    with mock.patch(
        "workers.download.check_disk_space",
        new=mock.AsyncMock(return_value=(True, 20.0, 100.0)),
    ), mock.patch("workers.download.disk_monitor") as dm, mock.patch(
        "services.downloader.download_task", new=_fake_download_task
    ):
        dm.paused_workers = set()
        asyncio.run(download_worker(client, 1))

    assert ctx.download_queue.empty() is True  # 任务已被消费
    md.app.is_running = True


def test_download_worker_pauses_when_cloud_down():
    md.app.is_running = True
    md.app.force_exit = False
    md.app.bark_notification = {}
    ctx.cloud_upload_ok = False
    ctx.download_semaphore = asyncio.Semaphore(1)
    ctx.download_queue = asyncio.Queue()

    async def stop_after_sleep(*args, **kwargs):
        md.app.is_running = False  # 暂停 sleep 后触发退出

    with mock.patch("workers.download.disk_monitor") as dm, mock.patch(
        "workers.download.asyncio.sleep", new=stop_after_sleep
    ):
        dm.paused_workers = set()
        asyncio.run(download_worker(mock.MagicMock(), 1))

    assert 1 in dm.paused_workers  # 云上传失败 → worker 被暂停
    md.app.is_running = True
    ctx.cloud_upload_ok = True


def test_download_worker_pauses_when_cloud_space_low():
    md.app.is_running = True
    md.app.force_exit = False
    md.app.bark_notification = {}
    ctx.cloud_upload_ok = True
    ctx.download_semaphore = asyncio.Semaphore(1)
    ctx.download_queue = asyncio.Queue()
    md.app.cloud_drive_config = CloudDriveConfig(
        enable_upload_file=True,
        upload_adapter="rclone",
        remote_dir="MyRemote:telegram",
        cloud_space_threshold_gb=10.0,
    )

    async def stop_after_sleep(*args, **kwargs):
        md.app.is_running = False

    with mock.patch("workers.download.disk_monitor") as dm, mock.patch(
        "workers.download.asyncio.sleep", new=stop_after_sleep
    ), mock.patch(
        "workers.download.check_disk_space",
        new=mock.AsyncMock(return_value=(True, 20.0, 100.0)),
    ), mock.patch(
        "module.cloud_drive.check_cloud_space",
        new=mock.AsyncMock(return_value=(False, 5.0, 100.0)),
    ):
        dm.paused_workers = set()
        asyncio.run(download_worker(mock.MagicMock(), 1))

    assert 1 in dm.paused_workers  # 云端空间不足 → worker 被暂停
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()


def test_download_worker_continues_when_cloud_space_unknown():
    # 云端空间查询失败 → fail-open，不暂停，正常消费队列
    md.app.is_running = True
    md.app.force_exit = False
    md.app.bark_notification = {}
    ctx.cloud_upload_ok = True
    ctx.download_semaphore = asyncio.Semaphore(1)
    ctx.download_queue = asyncio.Queue()
    md.app.cloud_drive_config = CloudDriveConfig(
        enable_upload_file=True,
        upload_adapter="rclone",
        remote_dir="MyRemote:telegram",
        cloud_space_threshold_gb=10.0,
    )

    node = TaskNode(chat_id=-123)
    message = MockMessage(id=5, media=True, chat_id=-123)
    ctx.download_queue.put_nowait((message, node))

    client = mock.MagicMock()
    with mock.patch(
        "workers.download.check_disk_space",
        new=mock.AsyncMock(return_value=(True, 20.0, 100.0)),
    ), mock.patch(
        "module.cloud_drive.check_cloud_space",
        new=mock.AsyncMock(return_value=(None, None, None)),
    ), mock.patch(
        "workers.download.disk_monitor"
    ) as dm, mock.patch(
        "services.downloader.download_task", new=_fake_download_task
    ):
        dm.paused_workers = set()
        asyncio.run(download_worker(client, 1))

    assert 1 not in dm.paused_workers  # 查询失败不暂停
    assert ctx.download_queue.empty() is True  # 任务被正常消费
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()


def test_download_worker_resumes_when_cloud_space_recovers():
    md.app.is_running = True
    md.app.force_exit = False
    md.app.bark_notification = {}
    ctx.cloud_upload_ok = True
    ctx.download_semaphore = asyncio.Semaphore(1)
    ctx.download_queue = asyncio.Queue()
    md.app.cloud_drive_config = CloudDriveConfig(
        enable_upload_file=True,
        upload_adapter="rclone",
        remote_dir="MyRemote:telegram",
        cloud_space_threshold_gb=10.0,
    )

    node = TaskNode(chat_id=-123)
    message = MockMessage(id=5, media=True, chat_id=-123)
    ctx.download_queue.put_nowait((message, node))

    client = mock.MagicMock()
    with mock.patch(
        "workers.download.check_disk_space",
        new=mock.AsyncMock(return_value=(True, 20.0, 100.0)),
    ), mock.patch(
        "module.cloud_drive.check_cloud_space",
        new=mock.AsyncMock(return_value=(True, 50.0, 100.0)),
    ), mock.patch(
        "workers.download.disk_monitor"
    ) as dm, mock.patch(
        "services.downloader.download_task", new=_fake_download_task
    ):
        dm.paused_workers = {1}  # 之前因云端空间不足被暂停
        asyncio.run(download_worker(client, 1))

    assert 1 not in dm.paused_workers  # 云端空间恢复 → 自动继续
    assert ctx.download_queue.empty() is True
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()


def test_download_worker_pauses_with_stale_low_cloud_cache():
    """回归：云端查询失败但上次成功结果是"不足" ⇒ 仍应暂停（不得当成充足继续下载）。"""
    from workers.monitor import disk_monitor as monitor_state

    md.app.is_running = True
    md.app.force_exit = False
    md.app.bark_notification = {}
    ctx.cloud_upload_ok = True
    ctx.download_semaphore = asyncio.Semaphore(1)
    ctx.download_queue = asyncio.Queue()
    md.app.cloud_drive_config = CloudDriveConfig(
        enable_upload_file=True,
        upload_adapter="rclone",
        remote_dir="MyRemote:telegram",
        cloud_space_threshold_gb=10.0,
    )
    # 上次成功查到的结果：只剩 5GB（< 阈值 10GB）
    monitor_state.cloud_space_cache = (5.0, 100.0, time.time())

    async def stop_after_sleep(*args, **kwargs):
        md.app.is_running = False

    with mock.patch("workers.download.disk_monitor") as dm, mock.patch(
        "workers.download.asyncio.sleep", new=stop_after_sleep
    ), mock.patch(
        "workers.download.check_disk_space",
        new=mock.AsyncMock(return_value=(True, 20.0, 100.0)),
    ), mock.patch(
        "module.cloud_drive.check_cloud_space",
        new=mock.AsyncMock(return_value=(None, None, None)),
    ):
        dm.paused_workers = set()
        asyncio.run(download_worker(mock.MagicMock(), 1))

    assert 1 in dm.paused_workers  # 沿用上次"不足"结果 → 暂停
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()
    monitor_state.cloud_space_cache = None


def test_download_worker_ignores_cloud_when_upload_disabled():
    """未启用上传 ⇒ 即使有"云端不足"的历史缓存也不得拦截正常下载。"""
    from workers.monitor import disk_monitor as monitor_state

    md.app.is_running = True
    md.app.force_exit = False
    md.app.bark_notification = {}
    ctx.cloud_upload_ok = True
    ctx.download_semaphore = asyncio.Semaphore(1)
    ctx.download_queue = asyncio.Queue()
    md.app.cloud_drive_config = CloudDriveConfig()  # enable_upload_file=False
    monitor_state.cloud_space_cache = (0.5, 100.0, time.time())

    node = TaskNode(chat_id=-123)
    message = MockMessage(id=5, media=True, chat_id=-123)
    ctx.download_queue.put_nowait((message, node))

    mock_cloud = mock.AsyncMock(return_value=(False, 0.5, 100.0))
    with mock.patch("workers.download.disk_monitor") as dm, mock.patch(
        "workers.download.check_disk_space",
        new=mock.AsyncMock(return_value=(True, 20.0, 100.0)),
    ), mock.patch("module.cloud_drive.check_cloud_space", new=mock_cloud), mock.patch(
        "services.downloader.download_task", new=_fake_download_task
    ):
        dm.paused_workers = set()
        asyncio.run(download_worker(mock.MagicMock(), 1))

    mock_cloud.assert_not_awaited()  # 未启用上传 ⇒ 不做云端查询
    assert 1 not in dm.paused_workers  # 不拦截
    assert ctx.download_queue.empty() is True  # 任务被正常消费
    md.app.is_running = True
    md.app.cloud_drive_config = CloudDriveConfig()
    monitor_state.cloud_space_cache = None


def test_retry_producer_exits_when_stopped():
    md.app.is_running = False
    md.app.force_exit = False
    asyncio.run(retry_producer(mock.MagicMock()))
    md.app.is_running = True


def test_retry_producer_retries_failed_task():
    md.app.is_running = True
    md.app.force_exit = False
    md.queue_manager.download_queue_size = 10
    md.app.chat_download_config = {123: mock.MagicMock(node=mock.MagicMock())}
    ctx.download_queue = mock.MagicMock()
    ctx.download_queue.qsize.return_value = 0

    client = mock.MagicMock()
    client.get_messages = mock.AsyncMock(
        return_value=MockMessage(id=7, media=True, chat_id=123)
    )

    async def fake_add(msg_, node_, is_retry=False):
        md.app.is_running = False  # 重试后触发退出
        return True

    with mock.patch(
        "workers.download.load_failed_tasks",
        new=mock.AsyncMock(return_value=[{"message_id": 7}]),
    ), mock.patch("workers.download.add_download_task", new=fake_add), mock.patch(
        "workers.download.remove_failed_task",
        new=mock.AsyncMock(return_value=True),
    ) as mock_rm, mock.patch(
        "workers.download.asyncio.sleep", new=mock.AsyncMock()
    ):
        asyncio.run(retry_producer(client))

    mock_rm.assert_awaited_once_with(123, 7)
    md.app.is_running = True


def test_download_chat_task_adds_task():
    md.app.is_running = True
    md.app.force_exit = False
    md.app.chat_download_config = {}
    chat_cfg = ChatDownloadConfig()
    node = TaskNode(chat_id=123)

    async def fake_history(*args, **kwargs):
        yield MockMessage(
            id=1, media=True, chat_id=123, chat_title="chat", caption="cap"
        )

    with mock.patch(
        "workers.download.get_chat_history_v2", new=fake_history
    ), mock.patch(
        "workers.download.add_download_task", new=mock.AsyncMock(return_value=True)
    ) as mock_add:
        asyncio.run(download_chat_task(mock.MagicMock(), 123, chat_cfg, node))

    mock_add.assert_awaited_once()


def test_download_chat_task_skips_when_filter_fails():
    md.app.is_running = True
    md.app.force_exit = False
    md.app.chat_download_config = {}
    chat_cfg = ChatDownloadConfig()
    node = TaskNode(chat_id=123)

    async def fake_history(*args, **kwargs):
        yield MockMessage(id=1, media=True, chat_id=123, chat_title="chat")

    with mock.patch(
        "workers.download.get_chat_history_v2", new=fake_history
    ), mock.patch(
        "workers.download.add_download_task", new=mock.AsyncMock()
    ) as mock_add, mock.patch(
        "workers.download.app.exec_filter", return_value=False
    ):
        asyncio.run(download_chat_task(mock.MagicMock(), 123, chat_cfg, node))

    mock_add.assert_not_awaited()
    assert node.download_status[1] == DownloadStatus.SkipDownload
