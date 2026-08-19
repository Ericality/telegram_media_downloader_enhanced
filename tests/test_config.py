"""Tests for core.config."""
from unittest import mock

import media_downloader as md
from core.config import _check_config, check_config_consistency


def test_check_config_success(tmp_path):
    md.app.log_level = "INFO"
    md.app.log_file_path = str(tmp_path)
    with mock.patch("core.config._load_config"), mock.patch(
        "core.config.print_meta"
    ), mock.patch("core.config.logger.remove"), mock.patch("core.config.logger.add"):
        assert _check_config() is True


def test_check_config_failure():
    md.app.log_level = "INFO"
    md.app.log_file_path = str(tmp_path) if False else "/tmp"
    with mock.patch(
        "core.config._load_config", side_effect=OSError("boom")
    ), mock.patch("core.config.print_meta"), mock.patch(
        "core.config.logger.remove"
    ), mock.patch(
        "core.config.logger.add"
    ):
        assert _check_config() is False


def test_check_config_consistency_reports_issues():
    md.app.api_id = ""
    md.app.api_hash = ""
    md.app.save_path = "/nonexistent/path"
    md.app.media_types = []
    md.app.file_formats = {}
    md.app.chat_download_config = {}
    md.app.notifications = {}
    issues = check_config_consistency(md.app)
    assert len(issues) >= 4


def test_update_config_preserves_comments(tmp_path):
    """Round-trip YAML loading must preserve comments across update_config."""
    from module.app import Application

    cfg = tmp_path / "config.yaml"
    data = tmp_path / "data.yaml"
    sessions = tmp_path / "sessions"
    sessions.mkdir()
    cfg.write_text(
        "api_hash: abc\n"
        "api_id: 123\n"
        "chat:\n"
        "- chat_id: -100123\n"
        "  last_read_message_id: 5  # 我的频道A\n"
        "- chat_id: -100456  # 频道B\n"
        "  last_read_message_id: 0\n"
    )
    app = Application(str(cfg), str(data))
    app.session_file_path = str(sessions)
    assert app.load_config() is True
    app.chat_download_config[-100123].last_read_message_id = 10
    assert app.update_config() is True

    result = cfg.read_text()
    assert "# 我的频道A" in result
    assert "# 频道B" in result
    assert "last_read_message_id: 10" in result
