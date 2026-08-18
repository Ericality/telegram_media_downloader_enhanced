"""Tests for QueueManager."""
import media_downloader as md
from core.queues import QueueManager


def test_queue_manager_defaults():
    qm = QueueManager()
    assert qm.max_download_tasks == 0
    assert qm.max_notify_tasks == 1
    assert qm.download_queue_size == 0
    assert qm.task_added == 0
    assert qm.task_processed == 0


def test_update_limits_reads_config():
    md.app.max_download_task = 8
    md.app.bark_notification = {"notify_worker_count": 3}
    qm = QueueManager()
    qm.update_limits()
    assert qm.max_download_tasks == 8
    assert qm.max_notify_tasks == 3
    assert qm.download_queue_size == 8


def test_update_limits_defaults_when_missing():
    md.app.max_download_task = 5
    md.app.bark_notification = {}
    qm = QueueManager()
    qm.update_limits()
    assert qm.max_download_tasks == 5
    assert qm.max_notify_tasks == 1
    assert qm.download_queue_size == 5
