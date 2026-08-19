"""Tests for media_downloader.main startup orchestration."""
from contextlib import ExitStack
from unittest import mock

import media_downloader as md
from media_downloader import main


def _build_mocks(stack):
    """Register all main() dependencies as mocks on the given ExitStack."""
    stack.enter_context(mock.patch("media_downloader.setup_exit_signal_handlers"))
    stack.enter_context(mock.patch("media_downloader.init_web"))
    stack.enter_context(mock.patch("media_downloader.print_config_summary"))
    stack.enter_context(
        mock.patch("media_downloader.check_config_consistency", return_value=[])
    )
    stack.enter_context(mock.patch("media_downloader.HookClient"))
    qm = stack.enter_context(mock.patch("media_downloader.queue_manager"))
    nm = stack.enter_context(mock.patch("media_downloader.notification_manager"))
    stack.enter_context(
        mock.patch("media_downloader.start_server", new=mock.AsyncMock())
    )
    stack.enter_context(
        mock.patch(
            "media_downloader.start_notify_workers", new=mock.AsyncMock(return_value=[])
        )
    )
    stack.enter_context(
        mock.patch(
            "media_downloader.start_download_workers",
            new=mock.AsyncMock(return_value=[]),
        )
    )
    stack.enter_context(
        mock.patch("media_downloader.download_all_chat", new=mock.AsyncMock())
    )
    stack.enter_context(
        mock.patch("media_downloader.run_until_all_task_finish", new=mock.AsyncMock())
    )
    stack.enter_context(
        mock.patch("media_downloader.graceful_shutdown", new=mock.AsyncMock())
    )
    stack.enter_context(
        mock.patch("media_downloader.stop_server", new=mock.AsyncMock())
    )
    stack.enter_context(
        mock.patch("media_downloader.stop_download_bot", new=mock.AsyncMock())
    )
    stack.enter_context(mock.patch("media_downloader.set_max_concurrent_transmissions"))
    stack.enter_context(
        mock.patch("media_downloader.disk_space_monitor_task", new=mock.AsyncMock())
    )
    stack.enter_context(
        mock.patch("media_downloader.stats_notification_task", new=mock.AsyncMock())
    )
    stack.enter_context(
        mock.patch("media_downloader.queue_monitor_task", new=mock.AsyncMock())
    )
    stack.enter_context(
        mock.patch("media_downloader.asyncio.sleep", new=mock.AsyncMock())
    )
    stack.enter_context(mock.patch.object(md.app, "update_config", return_value=True))
    return qm, nm


def test_main_runs_successfully():
    md.app.bot_token = ""
    md.app.is_running = True
    md.app.force_exit = False

    with ExitStack() as stack:
        qm, nm = _build_mocks(stack)
        qm.download_queue_size = 5
        qm.max_download_tasks = 3
        qm.update_limits = mock.MagicMock()
        qm.task_added = 0
        qm.task_processed = 0
        nm.bark_enabled = False
        nm.synology_chat_enabled = False
        nm.load_config = mock.MagicMock()
        nm.should_notify = mock.MagicMock(return_value=False)

        main()


def test_main_handles_keyboard_interrupt():
    md.app.bot_token = ""
    with ExitStack() as stack:
        _build_mocks(stack)
        stack.enter_context(
            mock.patch.object(md.app, "pre_run", side_effect=KeyboardInterrupt)
        )
        main()
