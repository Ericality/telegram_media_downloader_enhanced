"""Tests for persistent storage helpers (seen media / duplicate count / failed tasks)."""
import asyncio
from unittest import mock

import media_downloader as md
from core.storage import (_load_duplicate_count, _load_seen_media, _save_duplicate_count, _save_seen_media, load_failed_tasks, record_failed_task, remove_failed_task)
from core.storage import DEDUP_DB_FILE, DUPLICATE_COUNT_FILE


def test_save_and_load_seen_media(tmp_path):
    md.app.session_file_path = str(tmp_path)
    _save_seen_media({"a", "b"})
    assert _load_seen_media() == {"a", "b"}


def test_load_seen_media_missing_file(tmp_path):
    md.app.session_file_path = str(tmp_path)
    assert _load_seen_media() == set()


def test_load_seen_media_corrupted_file(tmp_path):
    md.app.session_file_path = str(tmp_path)
    (tmp_path / DEDUP_DB_FILE).write_text("{not valid json")
    assert _load_seen_media() == set()


def test_load_seen_media_non_list_content(tmp_path):
    md.app.session_file_path = str(tmp_path)
    (tmp_path / DEDUP_DB_FILE).write_text('{"not": "a list"}')
    assert _load_seen_media() == set()


def test_save_and_load_duplicate_count(tmp_path):
    md.app.session_file_path = str(tmp_path)
    _save_duplicate_count({"media_1": 3})
    assert _load_duplicate_count() == {"media_1": 3}


def test_load_duplicate_count_missing_file(tmp_path):
    md.app.session_file_path = str(tmp_path)
    assert _load_duplicate_count() == {}


def test_load_duplicate_count_corrupted_file(tmp_path):
    md.app.session_file_path = str(tmp_path)
    (tmp_path / DUPLICATE_COUNT_FILE).write_text("{bad json")
    assert _load_duplicate_count() == {}


def test_record_then_load_failed_task(tmp_path):
    md.app.session_file_path = str(tmp_path)
    md.app.chat_download_config = {}
    retry_count = asyncio.run(record_failed_task(123, 456, "network error"))
    assert retry_count == 0
    tasks = asyncio.run(load_failed_tasks(123))
    assert len(tasks) == 1
    assert tasks[0]["message_id"] == 456
    assert tasks[0]["chat_id"] == "123"
    assert tasks[0]["retry_count"] == 0


def test_record_failed_task_increments_retry(tmp_path):
    md.app.session_file_path = str(tmp_path)
    md.app.chat_download_config = {}
    asyncio.run(record_failed_task(123, 456, "boom"))
    retry_count = asyncio.run(record_failed_task(123, 456, "again"))
    assert retry_count == 1
    tasks = asyncio.run(load_failed_tasks(123))
    assert len(tasks) == 1
    assert tasks[0]["retry_count"] == 1


def test_load_failed_tasks_filters_by_chat(tmp_path):
    md.app.session_file_path = str(tmp_path)
    md.app.chat_download_config = {}
    asyncio.run(record_failed_task(111, 1, "e1"))
    asyncio.run(record_failed_task(222, 2, "e2"))
    assert len(asyncio.run(load_failed_tasks(111))) == 1
    assert len(asyncio.run(load_failed_tasks(222))) == 1
    assert asyncio.run(load_failed_tasks(333)) == []


def test_remove_failed_task(tmp_path):
    md.app.session_file_path = str(tmp_path)
    md.app.chat_download_config = {}
    asyncio.run(record_failed_task(123, 456, "boom"))
    removed = asyncio.run(remove_failed_task(123, 456))
    assert removed is True
    assert asyncio.run(load_failed_tasks(123)) == []


def test_remove_failed_task_nonexistent(tmp_path):
    md.app.session_file_path = str(tmp_path)
    md.app.chat_download_config = {}
    asyncio.run(record_failed_task(123, 456, "boom"))
    removed = asyncio.run(remove_failed_task(123, 999))
    assert removed is False
    assert len(asyncio.run(load_failed_tasks(123))) == 1


def test_save_seen_media_write_error(tmp_path):
    md.app.session_file_path = str(tmp_path)
    with mock.patch("core.storage.open", side_effect=OSError("disk full")):
        _save_seen_media({"a"})  # 不抛异常


def test_load_duplicate_count_read_error(tmp_path):
    md.app.session_file_path = str(tmp_path)
    (tmp_path / DUPLICATE_COUNT_FILE).write_text('{"x": 1}')
    with mock.patch("core.storage.open", side_effect=OSError("boom")):
        assert _load_duplicate_count() == {}


def test_save_duplicate_count_write_error(tmp_path):
    md.app.session_file_path = str(tmp_path)
    with mock.patch("core.storage.open", side_effect=OSError("disk full")):
        _save_duplicate_count({"x": 1})  # 不抛异常


def test_record_failed_task_legacy_format_migration(tmp_path):
    md.app.session_file_path = str(tmp_path)
    md.app.chat_download_config = {}
    (tmp_path / "failed_tasks.json").write_text('{"old": "format"}')
    retry_count = asyncio.run(record_failed_task(123, 456, "e"))
    assert retry_count == 0
    tasks = asyncio.run(load_failed_tasks(123))
    assert len(tasks) == 1


def test_record_failed_task_io_error(tmp_path):
    md.app.session_file_path = str(tmp_path)
    md.app.chat_download_config = {}
    with mock.patch("core.storage.open", side_effect=OSError("boom")):
        retry_count = asyncio.run(record_failed_task(123, 456, "e"))
    assert retry_count == 0


def test_load_failed_tasks_non_list(tmp_path):
    md.app.session_file_path = str(tmp_path)
    (tmp_path / "failed_tasks.json").write_text('{"not": "list"}')
    assert asyncio.run(load_failed_tasks(123)) == []


def test_load_failed_tasks_io_error(tmp_path):
    md.app.session_file_path = str(tmp_path)
    (tmp_path / "failed_tasks.json").write_text('[]')
    with mock.patch("core.storage.open", side_effect=OSError("boom")):
        assert asyncio.run(load_failed_tasks(123)) == []


def test_remove_failed_task_io_error(tmp_path):
    md.app.session_file_path = str(tmp_path)
    (tmp_path / "failed_tasks.json").write_text('[]')
    with mock.patch("core.storage.open", side_effect=OSError("boom")):
        assert asyncio.run(remove_failed_task(123, 1)) is False
