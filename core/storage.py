"""Persistent storage helpers — seen media, duplicate count and failed tasks."""
import json
import os
from datetime import datetime
from typing import Dict, Union

from loguru import logger

from core.context import app

DEDUP_DB_FILE = "seen_media.json"
DUPLICATE_COUNT_FILE = "duplicate_count.json"
FAILED_TASKS_FILE = "failed_tasks.json"


def _load_seen_media() -> set:
    """Load previously seen media IDs from disk."""
    db_path = os.path.join(app.session_file_path, DEDUP_DB_FILE)
    if os.path.exists(db_path):
        try:
            with open(db_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, list):
                    seen = set(data)
                    logger.info(f"已加载 {len(seen)} 条媒体去重记录")
                    return seen
        except Exception as e:
            logger.warning(f"加载媒体去重记录失败: {e}")
    return set()


def _save_seen_media(seen: set):
    """Persist seen media IDs to disk."""
    db_path = os.path.join(app.session_file_path, DEDUP_DB_FILE)
    try:
        with open(db_path, 'w', encoding='utf-8') as f:
            json.dump(list(seen), f, ensure_ascii=False)
    except Exception as e:
        logger.warning(f"保存媒体去重记录失败: {e}")


def _load_duplicate_count() -> Dict[str, int]:
    """Load duplicate download counts from disk."""
    db_path = os.path.join(app.session_file_path, DUPLICATE_COUNT_FILE)
    if os.path.exists(db_path):
        try:
            with open(db_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                if isinstance(data, dict):
                    logger.info(f"已加载 {len(data)} 条重复下载计数记录")
                    return data
        except Exception as e:
            logger.warning(f"加载重复下载计数记录失败: {e}")
    return {}


def _save_duplicate_count(counts: Dict[str, int]):
    """Persist duplicate download counts to disk."""
    db_path = os.path.join(app.session_file_path, DUPLICATE_COUNT_FILE)
    try:
        with open(db_path, 'w', encoding='utf-8') as f:
            json.dump(counts, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.warning(f"保存重复下载计数记录失败: {e}")


async def record_failed_task(chat_id: Union[int, str], message_id: int, error_msg: str):
    """Record a failed task for retry (no retry limit)."""
    try:
        failed_tasks_file = os.path.join(app.session_file_path, FAILED_TASKS_FILE)
        tasks = []

        if os.path.exists(failed_tasks_file):
            try:
                with open(failed_tasks_file, 'r', encoding='utf-8') as f:
                    loaded = json.load(f)
                    tasks = loaded if isinstance(loaded, list) else []
                    if not isinstance(loaded, list):
                        logger.info("检测到旧版 failed_tasks.json 格式，将自动迁移")
            except:
                tasks = []

        # Look up chat title
        chat_title = str(chat_id)
        try:
            cfg = app.chat_download_config.get(chat_id) or app.chat_download_config.get(str(chat_id))
            if cfg and cfg.node:
                maybe = getattr(cfg.node, 'chat_title', None)
                if maybe:
                    chat_title = str(maybe)
        except:
            pass

        existing_index = -1
        for i, t in enumerate(tasks):
            if str(t.get('chat_id')) == str(chat_id) and t.get('message_id') == message_id:
                existing_index = i
                break

        if existing_index >= 0:
            tasks[existing_index]['retry_count'] += 1
            tasks[existing_index]['timestamp'] = datetime.now().isoformat()
            tasks[existing_index]['error'] = error_msg[:500]
            tasks[existing_index]['chat_title'] = chat_title
            retry_count = tasks[existing_index]['retry_count']
            logger.warning(f"更新失败任务: chat={chat_title}({chat_id}), message_id={message_id}, 重试次数: {retry_count}")
        else:
            tasks.append({
                'chat_id': str(chat_id),
                'chat_title': chat_title,
                'message_id': message_id,
                'error': error_msg[:500],
                'timestamp': datetime.now().isoformat(),
                'retry_count': 0
            })
            retry_count = 0
            logger.warning(f"记录新失败任务: chat={chat_title}({chat_id}), message_id={message_id}")

        with open(failed_tasks_file, 'w', encoding='utf-8') as f:
            json.dump(tasks, f, ensure_ascii=False, indent=2)

        return retry_count
    except Exception as e:
        logger.error(f"记录失败任务时出错: {e}")
        return 0


async def load_failed_tasks(chat_id: Union[int, str]) -> list:
    """Load failed tasks for a given chat (flat-array format)."""
    try:
        failed_tasks_file = os.path.join(app.session_file_path, FAILED_TASKS_FILE)
        if not os.path.exists(failed_tasks_file):
            return []

        with open(failed_tasks_file, 'r', encoding='utf-8') as f:
            all_tasks = json.load(f)

        if not isinstance(all_tasks, list):
            return []

        return [t for t in all_tasks if str(t.get('chat_id')) == str(chat_id)]
    except Exception as e:
        logger.error(f"加载失败任务时出错: {e}")
        return []


async def remove_failed_task(chat_id: Union[int, str], message_id: int):
    """Remove a successfully completed task from the failed list (flat array)."""
    try:
        failed_tasks_file = os.path.join(app.session_file_path, FAILED_TASKS_FILE)
        if not os.path.exists(failed_tasks_file):
            return False

        with open(failed_tasks_file, 'r', encoding='utf-8') as f:
            tasks = json.load(f)

        if not isinstance(tasks, list):
            return False

        original_count = len(tasks)
        tasks = [t for t in tasks if not (
            str(t.get('chat_id')) == str(chat_id) and t.get('message_id') == message_id
        )]
        removed = original_count != len(tasks)

        if removed:
            with open(failed_tasks_file, 'w', encoding='utf-8') as f:
                json.dump(tasks, f, ensure_ascii=False, indent=2)
            logger.info(f"从失败列表移除成功任务: chat_id={chat_id}, message_id={message_id}")

        return removed
    except Exception as e:
        logger.error(f"移除失败任务时出错: {e}")
        return False
