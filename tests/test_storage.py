"""Tests for persistent storage helpers (seen media / duplicate count / failed tasks)."""
import asyncio

import media_downloader as md
from core.storage import DEDUP_DB_FILE, DUPLICATE_COUNT_FILE


def test_save_and_load_seen_media(tmp_path):
    md.app.session_file_path = str(tmp_path)
    md._save_seen_media({"a", "b"})
    assert md._load_seen_media() == {"a", "b"}


def test_load_seen_media_missing_file(tmp_path):
    md.app.session_file_path = str(tmp_path)
    assert md._load_seen_media() == set()


def test_load_seen_media_corrupted_file(tmp_path):
    md.app.session_file_path = str(tmp_path)
    (tmp_path / DEDUP_DB_FILE).write_text("{not valid json")
    assert md._load_seen_media() == set()


def test_load_seen_media_non_list_content(tmp_path):
    md.app.session_file_path = str(tmp_path)
    (tmp_path / DEDUP_DB_FILE).write_text('{"not": "a list"}')
    assert md._load_seen_media() == set()


def test_save_and_load_duplicate_count(tmp_path):
    md.app.session_file_path = str(tmp_path)
    md._save_duplicate_count({"media_1": 3})
    assert md._load_duplicate_count() == {"media_1": 3}


def test_load_duplicate_count_missing_file(tmp_path):
    md.app.session_file_path = str(tmp_path)
    assert md._load_duplicate_count() == {}


def test_load_duplicate_count_corrupted_file(tmp_path):
    md.app.session_file_path = str(tmp_path)
    (tmp_path / DUPLICATE_COUNT_FILE).write_text("{bad json")
    assert md._load_duplicate_count() == {}


def test_record_then_load_failed_task(tmp_path):
    md.app.session_file_path = str(tmp_path)
    md.app.chat_download_config = {}
    retry_count = asyncio.run(md.record_failed_task(123, 456, "network error"))
    assert retry_count == 0
    tasks = asyncio.run(md.load_failed_tasks(123))
    assert len(tasks) == 1
    assert tasks[0]["message_id"] == 456
    assert tasks[0]["chat_id"] == "123"
    assert tasks[0]["retry_count"] == 0


def test_record_failed_task_increments_retry(tmp_path):
    md.app.session_file_path = str(tmp_path)
    md.app.chat_download_config = {}
    asyncio.run(md.record_failed_task(123, 456, "boom"))
    retry_count = asyncio.run(md.record_failed_task(123, 456, "again"))
    assert retry_count == 1
    tasks = asyncio.run(md.load_failed_tasks(123))
    assert len(tasks) == 1
    assert tasks[0]["retry_count"] == 1


def test_load_failed_tasks_filters_by_chat(tmp_path):
    md.app.session_file_path = str(tmp_path)
    md.app.chat_download_config = {}
    asyncio.run(md.record_failed_task(111, 1, "e1"))
    asyncio.run(md.record_failed_task(222, 2, "e2"))
    assert len(asyncio.run(md.load_failed_tasks(111))) == 1
    assert len(asyncio.run(md.load_failed_tasks(222))) == 1
    assert asyncio.run(md.load_failed_tasks(333)) == []


def test_remove_failed_task(tmp_path):
    md.app.session_file_path = str(tmp_path)
    md.app.chat_download_config = {}
    asyncio.run(md.record_failed_task(123, 456, "boom"))
    removed = asyncio.run(md.remove_failed_task(123, 456))
    assert removed is True
    assert asyncio.run(md.load_failed_tasks(123)) == []


def test_remove_failed_task_nonexistent(tmp_path):
    md.app.session_file_path = str(tmp_path)
    md.app.chat_download_config = {}
    asyncio.run(md.record_failed_task(123, 456, "boom"))
    removed = asyncio.run(md.remove_failed_task(123, 999))
    assert removed is False
    assert len(asyncio.run(md.load_failed_tasks(123))) == 1
