"""Tests for disk space monitoring helpers."""
import asyncio
from unittest import mock

import media_downloader as md


def _disk_usage(free_bytes, total_bytes):
    return type("DiskUsage", (), {"free": free_bytes, "total": total_bytes})()


def test_check_disk_space_enough():
    md.app.download_path = "/app/downloads"
    with mock.patch("media_downloader.os.path.exists", return_value=True), \
            mock.patch(
                "media_downloader.psutil.disk_usage",
                return_value=_disk_usage(20 * 1024 ** 3, 100 * 1024 ** 3),
            ):
        has_space, free_gb, total_gb = asyncio.run(md.check_disk_space(10.0))
        assert has_space is True
        assert free_gb == 20.0
        assert total_gb == 100.0


def test_check_disk_space_low():
    md.app.download_path = "/app/downloads"
    with mock.patch("media_downloader.os.path.exists", return_value=True), \
            mock.patch(
                "media_downloader.psutil.disk_usage",
                return_value=_disk_usage(5 * 1024 ** 3, 100 * 1024 ** 3),
            ):
        has_space, free_gb, total_gb = asyncio.run(md.check_disk_space(10.0))
        assert has_space is False
        assert free_gb == 5.0
        assert total_gb == 100.0


def test_check_disk_space_exception_returns_safe_defaults():
    md.app.download_path = "/app/downloads"
    with mock.patch("media_downloader.os.path.exists", return_value=True), \
            mock.patch("media_downloader.psutil.disk_usage", side_effect=OSError("boom")):
        has_space, free_gb, total_gb = asyncio.run(md.check_disk_space(10.0))
        assert has_space is False
        assert free_gb == 0
        assert total_gb == 0
