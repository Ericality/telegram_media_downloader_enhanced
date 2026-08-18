"""Tests for services.stats."""
import asyncio
from datetime import datetime
from unittest import mock

import core.context as ctx
import media_downloader as md
from services.stats import calculate_directory_size, collect_stats_async, get_storage_summary_text


def test_calculate_directory_size(tmp_path):
    (tmp_path / "a.txt").write_text("hello")
    (tmp_path / "sub").mkdir()
    (tmp_path / "sub" / "b.txt").write_text("world")
    assert calculate_directory_size(str(tmp_path)) == 10


def test_calculate_directory_size_missing():
    assert calculate_directory_size("/nonexistent/path") == 0


def test_collect_stats_async():
    md.app.total_download_task = 5
    md.app.chat_download_config = {}
    md.app.save_path = ""
    ctx.download_queue = None

    with mock.patch(
        "workers.monitor.check_disk_space", new=mock.AsyncMock(return_value=(True, 20.0, 100.0))
    ), mock.patch("workers.monitor.disk_monitor") as dm:
        dm.stats_start_time = datetime(2026, 1, 1)
        dm.stats_since_last_notification = {"download_size": 1048576}
        dm.paused_workers = set()
        dm.space_low = False
        stats = asyncio.run(collect_stats_async())

    assert stats["tasks_completed"] == 5
    assert stats["disk_available_gb"] == 20.0
    assert stats["disk_total_gb"] == 100.0
    assert stats["space_low"] is False
    assert stats["download_size_mb"] == 1.0


def test_collect_stats_async_disk_error_safe_defaults():
    md.app.total_download_task = 0
    md.app.chat_download_config = {}
    md.app.save_path = ""
    ctx.download_queue = None

    with mock.patch(
        "workers.monitor.check_disk_space", new=mock.AsyncMock(side_effect=OSError("boom"))
    ), mock.patch("workers.monitor.disk_monitor") as dm:
        dm.stats_start_time = datetime(2026, 1, 1)
        dm.stats_since_last_notification = {"download_size": 0}
        dm.paused_workers = set()
        dm.space_low = False
        stats = asyncio.run(collect_stats_async())

    assert stats["disk_available_gb"] == 0
    assert stats["disk_total_gb"] == 0
    assert stats["space_low"] is False
