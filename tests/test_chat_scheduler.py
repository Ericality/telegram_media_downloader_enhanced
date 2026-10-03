"""生产者调度测试：会话时间片（长对话让位）+ 循环复查（完成后回到第一个会话）。

判据与反向正控配套：去掉时间片 / 去掉循环复查 ⇒ 对应判据必须变红。
"""
import ast
import asyncio
import inspect
from pathlib import Path
from unittest import mock

import media_downloader as md
from core.models import ChatDownloadConfig, TaskNode
from workers.download import _config_seconds, download_all_chat, download_chat_task

from .test_common import MockMessage

REPO_ROOT = Path(__file__).resolve().parent.parent


def _history_of(ids, calls=None):
    """造一个按 id 顺序产出的假 get_chat_history_v2（可记录调用参数）。"""

    async def fake_history(*args, **kwargs):
        if calls is not None:
            calls.append({"args": args, "kwargs": kwargs})
        for message_id in ids:
            yield MockMessage(
                id=message_id, media=True, chat_id=123, chat_title="chat", caption="cap"
            )

    return fake_history


class _LoopStub:
    """替掉 ``app.loop``：生产代码用 ``app.loop.create_task`` 起后台重试生产者，
    测试里没有跑步的事件循环 ⇒ 不能真的建任务（会留下 pending task 警告）。"""

    def create_task(self, coro):
        coro.close()
        return mock.MagicMock()


def _run_download_all_chat():
    with mock.patch.object(md.app, "loop", new=_LoopStub(), create=True):
        asyncio.run(download_all_chat(mock.MagicMock()))


def _reset_app(chat_ids=(), max_hours=72.0, recheck_minutes=30.0):
    md.app.is_running = True
    md.app.force_exit = False
    md.app.chat_download_config = {
        chat_id: ChatDownloadConfig() for chat_id in chat_ids
    }
    md.app.chat_max_continuous_hours = max_hours
    md.app.chat_recheck_interval_minutes = recheck_minutes


# --------------------------------------------------------------------------
# 单会话：时间片
# --------------------------------------------------------------------------


def test_download_chat_task_returns_false_when_exhausted():
    """不设时间片 / 时间片内走完 ⇒ 返回 False（本轮无剩余）。"""
    _reset_app()
    chat_cfg = ChatDownloadConfig()
    node = TaskNode(chat_id=123)

    with mock.patch(
        "workers.download.get_chat_history_v2", new=_history_of([1, 2, 3])
    ), mock.patch(
        "workers.download.add_download_task", new=mock.AsyncMock(return_value=True)
    ) as mock_add:
        still_pending = asyncio.run(
            download_chat_task(mock.MagicMock(), 123, chat_cfg, node, max_seconds=None)
        )

    assert still_pending is False
    assert mock_add.await_count == 3


def test_download_chat_task_yields_when_time_slice_exceeded():
    """时间片到点 ⇒ 立即让位：返回 True、未把消息喂完。"""
    _reset_app()
    chat_cfg = ChatDownloadConfig()
    node = TaskNode(chat_id=123)

    with mock.patch(
        "workers.download.get_chat_history_v2", new=_history_of([1, 2, 3])
    ), mock.patch(
        "workers.download.add_download_task", new=mock.AsyncMock(return_value=True)
    ) as mock_add:
        still_pending = asyncio.run(
            download_chat_task(mock.MagicMock(), 123, chat_cfg, node, max_seconds=1e-9)
        )

    assert still_pending is True  # 有剩余 ⇒ 下一轮要继续
    assert mock_add.await_count == 0  # 第一条就撞上时间片，未添加任何任务


def test_download_chat_task_resumes_from_persisted_id():
    """让位后下一轮必须从落盘的 last_read_message_id 续传（不漏不重）。"""
    _reset_app()
    chat_cfg = ChatDownloadConfig()
    node = TaskNode(chat_id=123)
    calls = []

    with mock.patch(
        "workers.download.get_chat_history_v2", new=_history_of([10, 11, 12], calls)
    ), mock.patch(
        "workers.download.add_download_task", new=mock.AsyncMock(return_value=True)
    ):
        asyncio.run(
            download_chat_task(mock.MagicMock(), 123, chat_cfg, node, max_seconds=None)
        )
        # 模拟 add_download_task 的副作用：入队即把 id 落盘（真实实现如此）
        chat_cfg.last_read_message_id = 12
        calls.clear()
        asyncio.run(
            download_chat_task(mock.MagicMock(), 123, chat_cfg, node, max_seconds=None)
        )

    assert calls[0]["kwargs"]["offset_id"] == 12  # 第二轮从 12 续传，不是从头


def test_config_seconds_disabled_and_defaults():
    """调度配置的边界语义：0/负数 ⇒ 关闭；非法值 ⇒ 回落默认；否则换算成秒。"""
    _reset_app()
    md.app.chat_max_continuous_hours = 0
    assert _config_seconds("chat_max_continuous_hours", 3600.0) == 0.0
    md.app.chat_max_continuous_hours = -1
    assert _config_seconds("chat_max_continuous_hours", 3600.0) == 0.0
    md.app.chat_max_continuous_hours = 24
    assert _config_seconds("chat_max_continuous_hours", 3600.0) == 24 * 3600
    md.app.chat_max_continuous_hours = "abc"
    assert _config_seconds("chat_max_continuous_hours", 3600.0) == 72 * 3600
    md.app.chat_recheck_interval_minutes = 30
    assert _config_seconds("chat_recheck_interval_minutes", 60.0) == 1800.0


# --------------------------------------------------------------------------
# 多会话：轮转 + 循环复查
# --------------------------------------------------------------------------


def test_download_all_chat_round_robin_yields_and_continues():
    """一个会话让位后，必须轮到下一个会话；下一轮再回到第一个会话。"""
    _reset_app(chat_ids=(-1, -2))
    order = []

    async def fake_task(client, chat_id, chat_cfg, node, max_seconds=None):
        order.append(chat_id)
        if chat_id == -1 and len(order) >= 3:
            md.app.is_running = False  # 第二轮处理完 A 就收工
        return chat_id == -1  # A 永远"还有剩余"，B 一轮走完

    with mock.patch("workers.download.download_chat_task", new=fake_task):
        _run_download_all_chat()

    assert order == [-1, -2, -1]  # 顺序轮转，且让位后 B 确实被处理了


def test_download_all_chat_sleeps_recheck_interval_when_all_idle():
    """一轮全无新内容 ⇒ 等配置的空闲间隔再来一轮（这就是"不会漏更新"）。"""
    _reset_app(chat_ids=(-1,), recheck_minutes=30.0)
    calls = []

    async def fake_task(client, chat_id, chat_cfg, node, max_seconds=None):
        calls.append(chat_id)
        return False  # 无剩余

    async def fake_sleep(seconds):
        slept.append(seconds)
        if len(slept) >= 2:  # 两次复查后收工，避免测试空转
            md.app.is_running = False
        return True

    slept = []
    with mock.patch("workers.download.download_chat_task", new=fake_task), mock.patch(
        "workers.download.sleep_with_exit_check", new=fake_sleep
    ):
        _run_download_all_chat()

    assert slept == [1800.0, 1800.0]  # 每轮之后等 30 分钟
    assert calls == [-1, -1]  # 第二轮确实回到了第一个会话


def test_download_all_chat_no_recheck_when_disabled():
    """空闲复查设为 0 ⇒ 一轮跑完即结束（旧行为，不空转）。"""
    _reset_app(chat_ids=(-1, -2), recheck_minutes=0.0)
    calls = []

    async def fake_task(client, chat_id, chat_cfg, node, max_seconds=None):
        calls.append(chat_id)
        return False

    with mock.patch("workers.download.download_chat_task", new=fake_task), mock.patch(
        "workers.download.sleep_with_exit_check", new=mock.AsyncMock(return_value=True)
    ) as mock_sleep:
        _run_download_all_chat()

    assert calls == [-1, -2]  # 只跑一轮
    mock_sleep.assert_not_awaited()


def test_download_all_chat_passes_configured_time_slice():
    """时间片配置必须以秒为单位传给 download_chat_task。"""
    _reset_app(chat_ids=(-1,), max_hours=24.0)
    seen = {}

    async def fake_task(client, chat_id, chat_cfg, node, max_seconds=None):
        seen["max_seconds"] = max_seconds
        md.app.is_running = False
        return False

    with mock.patch("workers.download.download_chat_task", new=fake_task):
        _run_download_all_chat()

    assert seen["max_seconds"] == 24 * 3600


# --------------------------------------------------------------------------
# bot 调用签名（既有缺陷回归守卫）
# --------------------------------------------------------------------------


def test_download_chat_task_signature_matches_bot_call_sites():
    """bot 侧调用必须与 download_chat_task 的必需参数个数一致。

    历史缺陷：bot.py 的两处调用只传 3 个位置参数（漏了 chat_id）⇒ 调用即 TypeError。
    """
    params = [
        name
        for name, p in inspect.signature(download_chat_task).parameters.items()
        if p.default is inspect.Parameter.empty
    ]
    assert params == ["client", "chat_id", "chat_download_config", "node"]

    tree = ast.parse((REPO_ROOT / "module" / "bot.py").read_text(encoding="utf-8"))
    call_sites = 0
    for node in ast.walk(tree):
        if not isinstance(node, ast.Call):
            continue
        func = node.func
        if isinstance(func, ast.Attribute) and func.attr == "download_chat_task":
            call_sites += 1
            assert len(node.args) == len(params), (
                f"module/bot.py:{node.lineno} 调用 download_chat_task 只传了 "
                f"{len(node.args)} 个位置参数，需要 {len(params)} 个"
            )
    assert call_sites == 2, f"期望 2 处 bot 调用点，实际 {call_sites}"


class _ClockStub:
    """按预设序列返回 time.time()（用完后固定为最后一个值）。"""

    def __init__(self, values):
        self._values = list(values)
        self._i = 0

    def time(self):
        value = self._values[min(self._i, len(self._values) - 1)]
        self._i += 1
        return value


def test_time_slice_log_reports_actual_elapsed_not_the_limit():
    """让位日志必须打"实际耗时"，不能只打时间片上限（背压会把实际耗时拖过上限）。"""
    _reset_app()
    chat_cfg = ChatDownloadConfig()
    node = TaskNode(chat_id=123)
    limit = 3600  # 时间片上限 1 小时
    clock = _ClockStub([1_000_000.0, 1_000_000.0 + 2 * 3600])  # 起始、第一条消息检查时（实际跑了 2 小时）

    with mock.patch(
        "workers.download.get_chat_history_v2", new=_history_of([1, 2, 3])
    ), mock.patch(
        "workers.download.add_download_task", new=mock.AsyncMock(return_value=True)
    ), mock.patch(
        "workers.download.time", new=clock
    ), mock.patch(
        "workers.download.logger"
    ) as fake_logger:
        still_pending = asyncio.run(
            download_chat_task(mock.MagicMock(), 123, chat_cfg, node, max_seconds=limit)
        )

    assert still_pending is True
    messages = [c.args[0] for c in fake_logger.warning.call_args_list]
    msg = next(m for m in messages if "让位" in m)
    assert "2.00 小时" in msg, f"应打实际耗时 2.00 小时，实际: {msg}"
    assert "1.00 小时" in msg, f"应同时打时间片上限 1.00 小时，实际: {msg}"
