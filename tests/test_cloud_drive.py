"""Tests for rclone remote verification."""
import asyncio
from unittest import mock

from module.cloud_drive import CloudDriveConfig, verify_rclone_remote


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
