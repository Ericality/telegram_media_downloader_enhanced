"""Worker monitor — disk space monitoring and periodic notification tasks."""
import asyncio
import os
import time
from datetime import datetime

import psutil
from loguru import logger

import core.context as ctx
from core.context import app, queue_manager
from services.notifier import notification_manager
from services.stats import collect_stats_async


class DiskSpaceMonitor:
    """Disk space monitor.

    Tracks disk usage, controls worker pause/resume, maintains stats start time.
    """

    def __init__(self):
        self.space_low = False
        self.cloud_space_low = False
        # 当前不足周期是否已发过首条响铃通知；恢复时重置。
        # 不足通知规则：首条响铃(默认level)，冷却后重复通知静音(passive)，恢复通知响铃。
        self.space_low_first_notified = False
        self.last_check_time = 0
        self.last_notification_time = 0
        self.paused_workers = set()
        self.stats_start_time = datetime.now()
        self.retry_success_count = 0
        # 云端连接健康重验（cloud_upload_ok 恢复机制）状态
        self.last_cloud_recheck_time = 0.0
        self.cloud_recheck_interval = 300
        self.stats_since_last_notification = {
            "tasks_completed": 0,
            "tasks_failed": 0,
            "tasks_skipped": 0,
            "download_size": 0,
        }


disk_monitor = DiskSpaceMonitor()


async def check_disk_space(threshold_gb: float = 10.0) -> tuple:
    """Check available disk space in GB."""
    try:
        download_path = (
            app.download_path if hasattr(app, "download_path") else "/app/downloads"
        )
        if not os.path.exists(download_path):
            download_path = "/"

        disk_usage = psutil.disk_usage(download_path)
        available_gb = disk_usage.free / (1024**3)
        total_gb = disk_usage.total / (1024**3)
        threshold_gb = float(threshold_gb)
        has_enough_space = available_gb >= threshold_gb

        return has_enough_space, round(available_gb, 2), round(total_gb, 2)
    except Exception as e:
        logger.error(f"检查磁盘空间失败: {e}")
        return False, 0, 0


async def disk_space_monitor_task():
    """Disk space monitor task."""
    # Check if notification system is enabled
    if not (
        notification_manager.bark_enabled or notification_manager.synology_chat_enabled
    ):
        logger.info("通知系统未启用，跳过磁盘空间监控任务")
        return

    # Get disk space thresholds
    bark_threshold = notification_manager.bark_config.get(
        "disk_space_threshold_gb", 10.0
    )
    synology_threshold = notification_manager.synology_chat_config.get(
        "disk_space_threshold_gb", 10.0
    )
    # Use the smaller threshold
    threshold_gb = min(bark_threshold, synology_threshold)

    # Get check intervals
    bark_interval = notification_manager.bark_config.get("space_check_interval", 300)
    synology_interval = notification_manager.synology_chat_config.get(
        "space_check_interval", 300
    )
    # Use the smaller interval
    check_interval = min(bark_interval, synology_interval)

    logger.info(f"磁盘空间监控已启动，阈值: {threshold_gb}GB，检查间隔: {check_interval}秒")

    # 云端空间检查启用总览（帮助确认策略是否生效）
    cloud_cfg = app.cloud_drive_config
    cloud_enabled_launch = bool(
        cloud_cfg.enable_upload_file
        and cloud_cfg.upload_adapter == "rclone"
        and cloud_cfg.cloud_space_threshold_gb > 0
    )
    logger.info(
        f"云端空间检查: 启用={cloud_enabled_launch} "
        f"(enable_upload_file={cloud_cfg.enable_upload_file}, "
        f"adapter={cloud_cfg.upload_adapter!r}, "
        f"cloud_space_threshold_gb={cloud_cfg.cloud_space_threshold_gb}GB; "
        f"需 enable_upload_file=true 且 adapter=rclone 且阈值>0 才启用，未配置该项默认 10GB)"
    )

    # Run one check immediately on startup
    try:
        has_space, available_gb, total_gb = await check_disk_space(threshold_gb)
        await notification_manager.send_disk_space_notification(
            has_space, available_gb, total_gb, threshold_gb
        )
    except Exception as e:
        logger.error(f"启动时磁盘空间检查失败: {e}")

    # Start periodic checks
    while True:
        # Check exit signal
        if getattr(app, "force_exit", False) or not getattr(app, "is_running", True):
            logger.info("磁盘空间监控任务收到退出信号，准备退出")
            break

        try:
            await asyncio.sleep(
                min(check_interval, 5)
            )  # Cap at 5s for fast exit response

            has_space, available_gb, total_gb = await check_disk_space(threshold_gb)

            # Cloud storage space check (rclone only; None = unknown -> fail-open)
            cloud_enabled = (
                app.cloud_drive_config.enable_upload_file
                and app.cloud_drive_config.upload_adapter == "rclone"
            )
            cloud_ok = None
            cloud_free_gb = None
            cloud_total_gb = None
            cloud_threshold = 10.0
            if cloud_enabled:
                cloud_threshold = getattr(
                    app.cloud_drive_config, "cloud_space_threshold_gb", 10.0
                )
                if cloud_threshold > 0:
                    from module.cloud_drive import check_cloud_space

                    cloud_ok, cloud_free_gb, cloud_total_gb = await check_cloud_space(
                        app.cloud_drive_config, cloud_threshold
                    )
                else:
                    cloud_enabled = False  # 阈值 0 = 不启用云端空间检查

            # 本地和云端都正常才算恢复；云端查询失败(None)按正常处理(不暂停)
            both_ok = has_space and (cloud_ok is not False)

            logger.debug(
                f"存储监控判定: 本地={'充足' if has_space else '不足'}"
                f"(free={available_gb}GB/total={total_gb}GB, 阈值={threshold_gb}GB), "
                f"云端检查={'启用' if cloud_enabled else '未启用'}, "
                f"云端={'充足' if cloud_ok is True else ('不足' if cloud_ok is False else '未知/失败')}"
                f"{'' if cloud_free_gb is None else f' (free={cloud_free_gb}GB/total={cloud_total_gb}GB, 阈值={cloud_threshold}GB)'}, "
                f"综合={'正常' if both_ok else '不足'}, "
                f"space_low={disk_monitor.space_low}, cloud_space_low={disk_monitor.cloud_space_low}"
            )

            current_time = time.time()
            notification_cooldown = 3600

            if not both_ok:
                disk_monitor.space_low = True
                if cloud_ok is False:
                    disk_monitor.cloud_space_low = True
                if (
                    current_time - disk_monitor.last_notification_time
                ) > notification_cooldown:
                    cloud_msg = ""
                    if cloud_ok is False:
                        cloud_msg = (
                            f"\n云端空间不足: 剩余 {cloud_free_gb}GB / 共 {cloud_total_gb}GB"
                            f" (阈值 {cloud_threshold}GB)"
                        )
                    elif cloud_ok is None and cloud_enabled:
                        cloud_msg = "\n云端空间: 查询失败（本次不暂停）"
                    # 首条不足通知响铃，问题持续期间的重复通知静音(passive)
                    first_low = not disk_monitor.space_low_first_notified
                    if first_low:
                        disk_monitor.space_low_first_notified = True
                        logger.info("空间不足首条通知已发送(响铃)，后续持续期间通知将静音")
                    bark_level = None if first_low else "passive"
                    await notification_manager.send_disk_space_notification(
                        both_ok,
                        available_gb,
                        total_gb,
                        threshold_gb,
                        cloud_msg,
                        bark_level=bark_level,
                    )
                    disk_monitor.last_notification_time = current_time
            else:
                if disk_monitor.space_low or disk_monitor.cloud_space_low:
                    disk_monitor.space_low = False
                    disk_monitor.cloud_space_low = False
                    # 问题解决：重置首条标记，恢复通知响铃
                    disk_monitor.space_low_first_notified = False
                    cloud_msg = ""
                    if cloud_enabled:
                        if cloud_ok is not None:
                            cloud_msg = (
                                f"\n云端空间: 剩余 {cloud_free_gb}GB / 共 {cloud_total_gb}GB"
                            )
                        else:
                            cloud_msg = "\n云端空间: 查询失败"
                    await notification_manager.send_disk_space_notification(
                        both_ok,
                        available_gb,
                        total_gb,
                        threshold_gb,
                        cloud_msg,
                        bark_level=None,
                    )

                    if disk_monitor.paused_workers:
                        logger.info("存储空间恢复，准备恢复下载任务...")
                        disk_monitor.paused_workers.clear()

        except Exception as e:
            logger.error(f"磁盘空间监控任务出错: {e}")
            await asyncio.sleep(60)

    logger.info("磁盘空间监控任务已停止")


async def cloud_health_monitor_task():
    """Cloud upload health monitor — re-verify rclone when upload check failed.

    修复隐患：`ctx.cloud_upload_ok` 在启动验证失败后从未恢复，worker 会永远暂停。
    本任务周期性重跑 rclone 读写验证，成功后把标志翻回 True，worker 自动恢复。
    独立于通知系统运行（不依赖 bark/synology 是否启用）。
    """
    if not (
        app.cloud_drive_config.enable_upload_file
        and app.cloud_drive_config.upload_adapter == "rclone"
    ):
        return

    logger.info("云端健康监控已启动")

    while getattr(app, "is_running", True) and not getattr(app, "force_exit", False):
        try:
            await asyncio.sleep(5)  # Cap at 5s for fast exit response

            if ctx.cloud_upload_ok:
                continue

            now = time.time()
            interval = getattr(disk_monitor, "cloud_recheck_interval", 300)
            if now - disk_monitor.last_cloud_recheck_time < interval:
                continue

            disk_monitor.last_cloud_recheck_time = now
            from module.cloud_drive import verify_rclone_remote

            cloud_ok, cloud_msg = await verify_rclone_remote(app.cloud_drive_config)
            if cloud_ok:
                ctx.cloud_upload_ok = True
                logger.success(f"☁️  云端连接恢复: {cloud_msg}")
                await notification_manager.send_event_notification(
                    "cloud_recovered", "☁️ 云端连接恢复，下载已继续", cloud_msg, "info"
                )
            else:
                logger.warning(f"☁️  云端仍未恢复: {cloud_msg}")
        except Exception as e:
            logger.error(f"云端健康监控任务出错: {e}")
            await asyncio.sleep(60)

    logger.info("云端健康监控任务已停止")


async def stats_notification_task():
    """Periodic stats notification task."""
    # Check if notification system is enabled
    if not notification_manager.should_notify("stats_summary"):
        logger.info("统计摘要通知未启用，跳过统计通知任务")
        return

    logger.info("统计通知任务已启动")

    # Run one notification immediately on startup
    try:
        stats = await collect_stats_async()
        if stats:
            await notification_manager.send_stats_notification(stats)
            logger.success("启动测试统计通知发送成功")
        else:
            logger.warning("收集统计信息失败，跳过启动测试通知")
    except Exception as e:
        logger.error(f"启动测试统计通知发送失败: {e}")

    # Get notification intervals
    bark_interval = notification_manager.bark_config.get(
        "stats_notification_interval", 3600
    )
    global_interval = notification_manager.global_config.get(
        "stats_notification_interval", 3600
    )
    # Use the shorter interval
    interval = min(bark_interval, global_interval)

    logger.info(f"统计通知任务将每 {interval} 秒执行一次")

    while getattr(app, "is_running", True):
        try:
            await asyncio.sleep(interval)

            stats = await collect_stats_async()
            if not stats:
                logger.warning("收集统计信息失败，跳过本次通知")
                continue

            await notification_manager.send_stats_notification(stats)

            # Reset stats counters
            disk_monitor.stats_since_last_notification = {
                "tasks_completed": 0,
                "tasks_failed": 0,
                "tasks_skipped": 0,
                "download_size": 0,
            }
        except Exception as e:
            logger.error(f"统计通知任务出错: {e}")
            await asyncio.sleep(60)


async def queue_monitor_task():
    """Queue monitor task; detects prolonged queue saturation."""
    # Check if notification system is enabled
    queue_status_enabled = notification_manager.should_notify("queue_status")
    queue_full_enabled = notification_manager.should_notify("queue_full")

    if not (queue_status_enabled or queue_full_enabled):
        logger.info("队列通知未启用，跳过队列监控任务")
        return

    logger.info("队列监控任务已启动")

    # Get monitor interval
    global_interval = notification_manager.global_config.get(
        "queue_monitor_interval", 300
    )

    while getattr(app, "is_running", True):
        try:
            await asyncio.sleep(global_interval)

            current_size = ctx.download_queue.qsize()
            queue_capacity = queue_manager.download_queue_size
            usage_percent = current_size / queue_capacity if queue_capacity > 0 else 0

            # Send status report if queue usage exceeds 80%
            if usage_percent > 0.8 and queue_status_enabled:
                # Get actual active worker count
                active_workers = queue_manager.max_download_tasks - len(
                    disk_monitor.paused_workers
                )

                # Get currently downloading task count from download_result (consistent with Web UI)
                from module.download_stat import get_download_result

                downloading_count = sum(
                    len(msgs) for msgs in get_download_result().values()
                )

                # Queued task count
                queued_count = ctx.download_queue.qsize()

                message = (
                    f"📊 队列状态报告\n"
                    f"队列使用率: {current_size}/{queue_capacity} ({int(usage_percent * 100)}%)\n"
                    f"Active workers: {active_workers}\n"
                    f"Downloading tasks: {downloading_count}\n"
                    f"Queued tasks: {queued_count}\n"
                    f"暂停worker数: {len(disk_monitor.paused_workers)}\n"
                    f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                )

                await notification_manager.send_event_notification(
                    "queue_status", "队列状态", message, "info"
                )

        except Exception as e:
            logger.error(f"队列监控任务出错: {e}")
            await asyncio.sleep(60)
