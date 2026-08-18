"""Tests for core.config."""
from unittest import mock

import media_downloader as md
from core.config import _check_config, check_config_consistency


def test_check_config_success(tmp_path):
    md.app.log_level = "INFO"
    md.app.log_file_path = str(tmp_path)
    with mock.patch("core.config._load_config"), \
            mock.patch("core.config.print_meta"), \
            mock.patch("core.config.logger.remove"), \
            mock.patch("core.config.logger.add"):
        assert _check_config() is True


def test_check_config_failure():
    md.app.log_level = "INFO"
    md.app.log_file_path = str(tmp_path) if False else "/tmp"
    with mock.patch("core.config._load_config", side_effect=OSError("boom")), \
            mock.patch("core.config.print_meta"), \
            mock.patch("core.config.logger.remove"), \
            mock.patch("core.config.logger.add"):
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
