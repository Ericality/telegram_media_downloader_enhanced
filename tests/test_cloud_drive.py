"""Tests for rclone remote verification and cloud space checks."""
import asyncio
import json
from unittest import mock

from module.cloud_drive import (
    CloudDriveConfig,
    check_cloud_space,
    verify_rclone_remote,
)


class FakeProc:
    def __init__(self, returncode=0, stdout=b"", stderr=b""):
        self.returncode = returncode
        self._stdout = stdout
        self._stderr = stderr

    async def communicate(self):
        return self._stdout, self._stderr


def _make_config():
    return CloudDriveConfig(
        remote_dir="MyRemote:telegram/downloads",
        rclone_path="/usr/bin/rclone",
    )


def _subprocess_ok(cmd, **kwargs):
    if "cat" in cmd:
        return FakeProc(0, stdout=b"rclone verify test")
    return FakeProc(0)


def _subprocess_mismatch(cmd, **kwargs):
    if "cat" in cmd:
        return FakeProc(0, stdout=b"wrong content")
    return FakeProc(0)


def test_verify_rclone_remote_success():
    with mock.patch(
        "module.cloud_drive.asyncio.create_subprocess_shell",
        side_effect=_subprocess_ok,
    ), mock.patch("module.cloud_drive.os.remove"), mock.patch(
        "module.cloud_drive.os.path.exists", return_value=False
    ), mock.patch(
        "builtins.open", mock.mock_open(read_data="rclone verify test")
    ):
        ok, msg = asyncio.run(verify_rclone_remote(_make_config()))

    assert ok is True
    assert "MyRemote:" in msg


def test_verify_rclone_remote_content_mismatch():
    with mock.patch(
        "module.cloud_drive.asyncio.create_subprocess_shell",
        side_effect=_subprocess_mismatch,
    ), mock.patch("module.cloud_drive.os.remove"), mock.patch(
        "module.cloud_drive.os.path.exists", return_value=False
    ), mock.patch(
        "builtins.open", mock.mock_open(read_data="rclone verify test")
    ):
        ok, msg = asyncio.run(verify_rclone_remote(_make_config()))

    assert ok is False
    assert "内容验证失败" in msg


def test_verify_rclone_remote_timeout():
    with mock.patch(
        "module.cloud_drive.asyncio.create_subprocess_shell",
        side_effect=asyncio.TimeoutError,
    ), mock.patch("module.cloud_drive.os.remove"), mock.patch(
        "module.cloud_drive.os.path.exists", return_value=False
    ):
        ok, msg = asyncio.run(verify_rclone_remote(_make_config()))

    assert ok is False
    assert "超时" in msg


def _make_cloud_config(enable=True, threshold=10.0):
    return CloudDriveConfig(
        enable_upload_file=enable,
        upload_adapter="rclone",
        remote_dir="MyRemote:telegram/downloads",
        rclone_path="/usr/bin/rclone",
        cloud_space_threshold_gb=threshold,
    )


def _about_payload(total_gb, used_gb, free_gb=None, has_free=True):
    data = {
        "total": int(total_gb * 1024**3),
        "used": int(used_gb * 1024**3),
        "trashed": 0,
        "other": 0,
        "hasTotal": True,
        "hasUsed": True,
        "hasFree": has_free,
    }
    if free_gb is not None:
        data["free"] = int(free_gb * 1024**3)
    return json.dumps(data).encode()


def _about_ok(payload):
    return lambda cmd, **kwargs: FakeProc(0, stdout=payload)


def test_check_cloud_space_enough():
    with mock.patch(
        "module.cloud_drive.asyncio.create_subprocess_shell",
        side_effect=_about_ok(_about_payload(100, 80, free_gb=20)),
    ):
        has_space, free_gb, total_gb = asyncio.run(
            check_cloud_space(_make_cloud_config(), 10.0)
        )

    assert has_space is True
    assert free_gb == 20.0
    assert total_gb == 100.0


def test_check_cloud_space_low():
    with mock.patch(
        "module.cloud_drive.asyncio.create_subprocess_shell",
        side_effect=_about_ok(_about_payload(100, 95, free_gb=5)),
    ):
        has_space, free_gb, total_gb = asyncio.run(
            check_cloud_space(_make_cloud_config(), 10.0)
        )

    assert has_space is False
    assert free_gb == 5.0


def test_check_cloud_space_fallback_total_minus_used():
    # 部分后端不报告 free 字段 → 用 total - used 估算
    with mock.patch(
        "module.cloud_drive.asyncio.create_subprocess_shell",
        side_effect=_about_ok(_about_payload(100, 95, has_free=False)),
    ):
        has_space, free_gb, total_gb = asyncio.run(
            check_cloud_space(_make_cloud_config(), 10.0)
        )

    assert has_space is False
    assert free_gb == 5.0
    assert total_gb == 100.0


def test_check_cloud_space_query_failure_returns_unknown():
    with mock.patch(
        "module.cloud_drive.asyncio.create_subprocess_shell",
        side_effect=lambda cmd, **kwargs: FakeProc(1, stderr=b"timeout"),
    ):
        has_space, free_gb, total_gb = asyncio.run(
            check_cloud_space(_make_cloud_config(), 10.0)
        )

    assert has_space is None  # fail-open
    assert free_gb is None
    assert total_gb is None


def test_check_cloud_space_disabled_returns_unknown():
    has_space, free_gb, total_gb = asyncio.run(
        check_cloud_space(_make_cloud_config(enable=False), 10.0)
    )

    assert has_space is None
    assert free_gb is None
    assert total_gb is None


def test_check_cloud_space_onedrive_output_without_has_flags():
    # OneDrive 实测输出：只有 total/used/trashed/free，没有 hasFree/hasTotal 字段
    payload = json.dumps(
        {
            "total": 1104880336896,  # 1029.0 GB
            "used": 1099082131047,
            "trashed": 0,
            "free": 5798205849,  # ~5.4 GB（配额接近用满）
        }
    ).encode()
    with mock.patch(
        "module.cloud_drive.asyncio.create_subprocess_shell",
        side_effect=_about_ok(payload),
    ):
        has_space, free_gb, total_gb = asyncio.run(
            check_cloud_space(_make_cloud_config(threshold=50.0), 50.0)
        )

    assert has_space is False  # 5.4GB < 50GB → 应判定不足
    assert free_gb == 5.4
    assert total_gb == 1029.0


def test_check_cloud_space_quota_full_has_free_true():
    # 明确报告配额用满：hasFree:true, free:0
    payload = json.dumps(
        {
            "total": int(100 * 1024**3),
            "used": int(100 * 1024**3),
            "trashed": 0,
            "free": 0,
            "hasTotal": True,
            "hasUsed": True,
            "hasFree": True,
        }
    ).encode()
    with mock.patch(
        "module.cloud_drive.asyncio.create_subprocess_shell",
        side_effect=_about_ok(payload),
    ):
        has_space, free_gb, total_gb = asyncio.run(
            check_cloud_space(_make_cloud_config(), 10.0)
        )

    assert has_space is False
    assert free_gb == 0.0
    assert total_gb == 100.0


def test_check_cloud_space_unlimited_backend_has_free_false_zero():
    # 无配额后端：hasFree:false, free:0 → 走 total-used 兜底，视为充足
    payload = json.dumps(
        {
            "total": int(1000 * 1024**3),
            "used": int(10 * 1024**3),
            "trashed": 0,
            "free": 0,
            "hasTotal": True,
            "hasUsed": True,
            "hasFree": False,
        }
    ).encode()
    with mock.patch(
        "module.cloud_drive.asyncio.create_subprocess_shell",
        side_effect=_about_ok(payload),
    ):
        has_space, free_gb, total_gb = asyncio.run(
            check_cloud_space(_make_cloud_config(), 10.0)
        )

    assert has_space is True
    assert free_gb == 990.0
    assert total_gb == 1000.0
