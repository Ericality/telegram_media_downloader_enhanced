"""Statistics services — collect and summarize download / storage stats."""
import asyncio
import os
from datetime import datetime
from pathlib import Path
from typing import Any, Dict

from loguru import logger

import core.context as ctx
from core.context import app, queue_manager
from core.storage import load_failed_tasks


async def get_storage_summary_text() -> str:
    """Build a one-line local + cloud storage summary (module-level, reusable)."""
    from workers.monitor import check_disk_space

    parts = []
    try:
        has_space, avail_gb, total_gb = await check_disk_space()
        pct = round((1 - avail_gb / total_gb) * 100, 1) if total_gb > 0 else 0
        parts.append(f"本地磁盘: {pct}% ({avail_gb:.1f}G/{total_gb:.1f}G)")
    except:
        pass
    try:
        if (
            app.cloud_drive_config.enable_upload_file
            and app.cloud_drive_config.upload_adapter == "rclone"
        ):
            from module.cloud_drive import get_cloud_storage_used

            cloud_info = await get_cloud_storage_used(app.cloud_drive_config)
            if cloud_info:
                parts.append(f"云端: {cloud_info.replace(chr(10), ' ')}")
    except:
        pass
    return " · ".join(parts) if parts else ""


def calculate_directory_size(directory_path: str) -> int:
    """Calculate total directory size in bytes."""
    total_size = 0
    try:
        path = Path(directory_path)

        if not path.exists() or not path.is_dir():
            return 0

        # Recursively traverse all files
        for file_path in path.rglob("*"):
            try:
                if file_path.is_file():
                    total_size += file_path.stat().st_size
            except (OSError, PermissionError):
                # Skip inaccessible files
                continue
    except Exception as e:
        logger.warning(f"计算目录大小出错 {directory_path}: {e}")

    return total_size


async def collect_stats_async() -> Dict[str, Any]:
    """Collect statistics asynchronously."""
    from workers.monitor import check_disk_space, disk_monitor

    try:
        uptime = datetime.now() - disk_monitor.stats_start_time
        uptime_str = str(uptime).split(".")[0]

        # Get disk space info asynchronously
        try:
            _, available_gb, total_gb = await check_disk_space()
        except Exception as e:
            logger.warning(f"获取磁盘空间信息失败: {e}")
            available_gb, total_gb = 0, 0

        tasks_completed = getattr(app, "total_download_task", 0)

        # Get queue size (sync-safe)
        try:
            queued_tasks = (
                ctx.download_queue.qsize()
                if hasattr(ctx.download_queue, "qsize")
                else 0
            )
        except:
            queued_tasks = 0

        # Count failed tasks across all chats
        total_failed_tasks = 0
        for chat_id, _ in app.chat_download_config.items():
            try:
                failed_tasks = await load_failed_tasks(chat_id)
                total_failed_tasks += len(failed_tasks)
            except Exception as e:
                logger.warning(f"加载失败任务统计失败 ({chat_id}): {e}")

        # Get download directory size
        download_dir_size_gb = 0
        try:
            download_dir = app.get_config("save_path")
            if download_dir and os.path.exists(download_dir):
                download_dir_size = await asyncio.to_thread(
                    calculate_directory_size, download_dir
                )
                download_dir_size_gb = download_dir_size / (1024**3)
                logger.debug(f"下载目录 {download_dir} 大小: {download_dir_size_gb:.2f}GB")
            elif download_dir:
                logger.debug(f"下载目录不存在: {download_dir}")
        except Exception as e:
            logger.warning(f"计算下载目录大小失败: {e}")

        # Active workers = total workers - paused workers
        active_workers = queue_manager.max_download_tasks - len(
            disk_monitor.paused_workers
        )
        if active_workers < 0:
            active_workers = 0

        # Active tasks = sum of all entries in download_result
        from module.download_stat import get_download_result

        try:
            # Shallow copy to avoid mutation during iteration
            snapshot = get_download_result().copy()
            active_tasks = sum(len(msgs) for msgs in snapshot.values())
        except Exception:
            active_tasks = 0

        return {
            "uptime": uptime_str,
            "tasks_completed": tasks_completed,
            "tasks_failed": total_failed_tasks,
            "tasks_skipped": 0,
            "download_size_mb": disk_monitor.stats_since_last_notification[
                "download_size"
            ]
            / (1024**2)
            if disk_monitor.stats_since_last_notification.get("download_size")
            else 0,
            "disk_available_gb": available_gb,
            "disk_total_gb": total_gb,
            "download_dir_size_gb": download_dir_size_gb,
            "active_workers": active_workers,
            "active_tasks": active_tasks,
            "queued_tasks": queued_tasks,
            "space_low": disk_monitor.space_low,
            "failed_tasks_pending": total_failed_tasks,
        }
    except Exception as e:
        logger.error(f"异步收集统计信息失败: {e}")
        return {}


def collect_stats() -> Dict[str, Any]:
    """Collect statistics synchronously (legacy compatibility)."""
    try:
        # If already in async context, create a task
        if asyncio.get_event_loop().is_running():
            # Create new task to avoid blocking
            task = asyncio.create_task(collect_stats_async())
            # Cannot await here; return empty dict
            return {}
        else:
            # Run in synchronous context
            return asyncio.run(collect_stats_async())
    except Exception as e:
        logger.error(f"同步收集统计信息失败: {e}")
        return {}
