"""Tests for rclone remote verification and cloud space checks."""
import asyncio
import json
import os
from unittest import mock

from module.cloud_drive import (
    CloudDrive,
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


# --------------------------------------------------------------------------
# 上传后校验：超时不得把"已上传成功"记成失败（2026-10-03 定因）
# --------------------------------------------------------------------------


class _AsyncLines:
    """既是异步可迭代（逐行读 stdout），又能 .read() 返回空。"""

    def __init__(self, lines=()):
        self._lines = [l.encode() if isinstance(l, str) else l for l in lines]

    def __aiter__(self):
        return self

    async def __anext__(self):
        if not self._lines:
            raise StopAsyncIteration
        return self._lines.pop(0)

    async def read(self):
        return b""


class _UploadProc:
    """模拟 rclone 子进程：可给 stdout 行、可在 wait() 时执行副作用（如 move 掉源文件）、可挂住。"""

    def __init__(self, lines=(), on_wait=None, hang=False):
        self.stdout = _AsyncLines(lines)
        self.stderr = _AsyncLines()
        self.returncode = 0
        self._on_wait = on_wait
        self._hang = hang
        self.killed = False
        self.communicate_called = False

    async def wait(self):
        if self._on_wait:
            self._on_wait()
        return self.returncode

    async def communicate(self):
        self.communicate_called = True
        if self._hang:
            await asyncio.sleep(3600)  # 永不返回 ⇒ 触发 wait_for 超时
        return b"", b""

    def kill(self):
        self.killed = True


class _AlwaysCached(dict):
    def get(self, key, default=None):
        return True


def _upload_case(tmp_path, remove_source_on_move):
    """造一次上传：rclone move 成功（stdout 有 100%），第二次子进程（rclone size）挂住。"""
    save_path = str(tmp_path)
    local = tmp_path / "sub" / "sample.mp4"
    local.parent.mkdir(parents=True, exist_ok=True)
    local.write_bytes(b"x" * 1024)

    cfg = CloudDriveConfig(
        remote_dir="MyRemote:telegram/downloads",
        rclone_path="/usr/bin/rclone",
    )
    cfg.dir_cache = _AlwaysCached()

    procs = []

    def on_move_done():
        if remove_source_on_move:
            os.remove(local)

    def factory(cmd, **kwargs):
        if len(procs) == 0:
            proc = _UploadProc(
                ["Transferred: 1 KiB / 1 KiB, 100%, 0 B/s, ETA -"], on_wait=on_move_done
            )
        else:
            proc = _UploadProc(hang=True)
        procs.append(proc)
        return proc

    return cfg, save_path, str(local), factory, procs


def test_upload_verify_timeout_with_source_moved_counts_as_success(tmp_path):
    """校验超时 + 源文件已被 move 走 ⇒ 视为上传成功（不得记失败、不得抛异常）。"""
    cfg, save_path, local, factory, procs = _upload_case(tmp_path, True)

    with mock.patch(
        "module.cloud_drive.asyncio.create_subprocess_shell", side_effect=factory
    ), mock.patch("module.cloud_drive.VERIFY_TIMEOUT_SECONDS", 0.05):
        ok = asyncio.run(CloudDrive.rclone_upload_file(cfg, save_path, local, None, ()))

    assert ok is True
    assert not os.path.exists(local)
    verify_proc = procs[1]
    assert verify_proc.communicate_called is True
    assert verify_proc.killed is True  # 超时后必须杀掉卡住的校验进程


def test_upload_verify_timeout_with_source_present_still_fails(tmp_path):
    """校验超时但源文件仍在 ⇒ 仍按失败处理（不能把真失败放行）。"""
    cfg, save_path, local, factory, procs = _upload_case(tmp_path, False)

    with mock.patch(
        "module.cloud_drive.asyncio.create_subprocess_shell", side_effect=factory
    ), mock.patch("module.cloud_drive.VERIFY_TIMEOUT_SECONDS", 0.05):
        ok = asyncio.run(CloudDrive.rclone_upload_file(cfg, save_path, local, None, ()))

    assert ok is False
    assert os.path.exists(local)  # 源文件保留，等重试
