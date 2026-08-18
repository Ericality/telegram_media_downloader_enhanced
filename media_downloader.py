"""Telegram Media Downloader

Downloads media from Telegram chats with:
- Multi-chat download management & progress tracking
- Bark & Synology Chat dual notification system
- Separate download queue & notification queue
- Disk space monitoring with auto pause/resume
- Infinite failed task retry mechanism
- Rclone / Aligo cloud storage upload
- Web admin panel
"""
import asyncio
import logging
import os
import signal
import sys
import time
from datetime import datetime

import pyrogram
from loguru import logger
from rich.console import Console
from rich.logging import RichHandler
from rich.theme import Theme

import core.context as ctx
from core.config import _check_config, check_config_consistency, print_config_summary
from core.context import CONFIG_NAME, app, queue_manager
from core.storage import _load_duplicate_count, load_failed_tasks, record_failed_task
from module.app import DownloadStatus
from module.bot import start_download_bot, stop_download_bot
from module.language import _t
from module.pyrogram_extension import HookClient, set_max_concurrent_transmissions
from module.web import init_web
from services.notifier import notification_manager
from workers.download import (
    add_download_task,
    download_all_chat,
    download_chat_task,
    download_worker,
)
from workers.monitor import (
    disk_monitor,
    disk_space_monitor_task,
    queue_monitor_task,
    stats_notification_task,
)
from workers.notify import notify_worker


custom_theme = Theme({
    "info": "cyan",
    "warning": "yellow",
    "error": "red",
    "success": "green",
    "debug": "dim blue",
})
console = Console(theme=custom_theme)

# RichHandler initialized at INFO level; reconfigured after config loaded
rich_handler = RichHandler(
    console=console,
    rich_tracebacks=True,
    markup=True,
    show_time=True,
    show_path=False,
    tracebacks_show_locals=False,
    level=logging.INFO
)

logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    datefmt="[%X]",
    handlers=[rich_handler],
)


# Bark notification level mapping
BARK_LEVELS = {
    "active": "active",
    "timeSensitive": "timeSensitive",
    "passive": "passive"
}







def run_async_sync(coroutine, loop=None, timeout=10):
    """Run async coroutine synchronously."""
    if loop is None:
        loop = app.loop

    if loop and loop.is_running():
        # Use run_coroutine_threadsafe if loop is already running
        import asyncio as aio
        future = aio.run_coroutine_threadsafe(coroutine, loop)
        return future.result(timeout=timeout)
    else:
        # Otherwise use run_until_complete
        return loop.run_until_complete(coroutine)


def setup_exit_signal_handlers():
    """Set up graceful exit signal handlers."""

    def signal_handler(signum, frame):
        logger.info(f"接收到信号 {signum}，正在优雅退出...")

        if hasattr(app, 'is_running'):
            app.is_running = False

        if hasattr(app, 'force_exit'):
            app.force_exit = True

        if signum == signal.SIGINT:
            logger.info("正在停止所有任务，请稍候...")
        elif signum == signal.SIGTERM:
            logger.info("收到终止信号，正在停止...")

    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)


async def graceful_shutdown():
    """Gracefully shut down all components."""
    logger.info("开始优雅关闭...")

    # 1. Stop adding new tasks
    app.is_running = False
    app.force_exit = True

    # 2. Send shutdown notification first
    try:
        if notification_manager.should_notify("shutdown"):
            shutdown_title = "程序停止"
            shutdown_message = (
                f"🛑 Telegram媒体下载器已停止\n"
                f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                f"运行时间: {datetime.now() - disk_monitor.stats_start_time}\n"
                f"完成任务: {app.total_download_task}"
            )

            # Give notification time to send
            notification_task = asyncio.create_task(
                notification_manager.send_event_notification("shutdown", shutdown_title, shutdown_message)
            )
            await asyncio.wait_for(notification_task, timeout=10)
            logger.info("关闭通知已发送")
    except Exception as e:
        logger.error(f"发送关闭通知失败: {e}")

    # 3. Brief wait for producers to stop
    await asyncio.sleep(1)

    # 4. Record in-flight and queued tasks to failed list
    pending_messages = []

    # Record all currently downloading tasks
    for chat_id, chat_config in app.chat_download_config.items():
        if chat_config.node and chat_config.node.download_status:
            for message_id, status in chat_config.node.download_status.items():
                if status == DownloadStatus.Downloading:
                    pending_messages.append((message_id, chat_id))
                    logger.debug(f"记录正在下载的任务: chat_id={chat_id}, message_id={message_id}")

    # Record all queued tasks
    try:
        while not ctx.download_queue.empty():
            try:
                message, node = ctx.download_queue.get_nowait()
                pending_messages.append((message.id, node.chat_id))
                ctx.download_queue.task_done()
                logger.debug(f"记录队列中的任务: chat_id={node.chat_id}, message_id={message.id}")
            except (asyncio.QueueEmpty, ValueError):
                break
    except Exception as e:
        logger.error(f"清空下载队列时出错: {e}")

    # Write to failed tasks file
    if pending_messages:
        logger.warning(f"有 {len(pending_messages)} 个未完成任务需要记录到失败列表")
        for message_id, chat_id in pending_messages:
            await record_failed_task(chat_id, message_id, "程序退出，任务未完成")

    # Final shutdown notification already sent above; skip duplicate

    logger.info("优雅关闭完成")


async def run_until_all_task_finish():
    """Main run loop: wait for new tasks to finish, then wait for retry producers or exit signal."""
    logger.info("开始主运行循环...")

    # Wait for new tasks to complete (producers have finished adding)
    while True:
        if getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True):
            logger.info("收到退出信号，准备退出...")
            break

        all_new_tasks_done = True
        for _, value in app.chat_download_config.items():
            if not value.need_check or value.total_task != value.finish_task:
                all_new_tasks_done = False
                break

        if all_new_tasks_done:
            logger.info("所有新任务已完成")
            break

        await asyncio.sleep(1)

    # After new tasks complete, keep running (retry producers still active) until exit signal
    while getattr(app, 'is_running', True) and not getattr(app, 'force_exit', False):
        # Periodic sleep; could add stats logging here
        await asyncio.sleep(10)

    logger.info("主运行循环结束")




async def start_server(client: pyrogram.Client):
    """Start Pyrogram client."""
    await client.start()


async def stop_server(client: pyrogram.Client):
    """Stop Pyrogram client."""
    await client.stop()


async def start_notify_workers():
    """Start notification workers."""
    notify_tasks = []

    for i in range(queue_manager.max_notify_tasks):
        task = app.loop.create_task(notify_worker(i + 1))
        notify_tasks.append(task)
        logger.debug(f"启动通知Worker {i + 1}/{queue_manager.max_notify_tasks}")

    return notify_tasks


async def start_download_workers(client: pyrogram.Client):
    """Start download workers."""
    download_tasks = []

    for i in range(queue_manager.max_download_tasks):
        task = app.loop.create_task(download_worker(client, i + 1))
        download_tasks.append(task)
        logger.debug(f"启动下载Worker {i + 1}/{queue_manager.max_download_tasks}")

    return download_tasks


async def wait_for_queues_to_empty():
    """Wait for queues to empty (with timeout fallback)."""
    logger.info("等待所有队列任务完成...")

    max_wait_time = 30
    start_time = time.time()

    # Try graceful wait first
    while time.time() - start_time < max_wait_time:
        try:
            # Prefer empty() over qsize() for accuracy
            download_queue_size = ctx.download_queue.qsize() if hasattr(ctx.download_queue, 'qsize') else 0
            notify_queue_size = ctx.notify_queue.qsize() if hasattr(ctx.notify_queue, 'qsize') else 0

            logger.debug(f"队列状态: 下载队列={download_queue_size}, 通知队列={notify_queue_size}")

            # More accurate emptiness check
            is_download_queue_empty = ctx.download_queue.empty() if hasattr(ctx.download_queue, 'empty') else (
                        download_queue_size == 0)
            is_notify_queue_empty = ctx.notify_queue.empty() if hasattr(ctx.notify_queue, 'empty') else (notify_queue_size == 0)

            if is_download_queue_empty and is_notify_queue_empty:
                # Check unfinished task counter
                unfinished_download_tasks = ctx.download_queue._unfinished_tasks if hasattr(ctx.download_queue,
                                                                                        '_unfinished_tasks') else 0
                unfinished_notify_tasks = ctx.notify_queue._unfinished_tasks if hasattr(ctx.notify_queue,
                                                                                    '_unfinished_tasks') else 0

                if unfinished_download_tasks == 0 and unfinished_notify_tasks == 0:
                    logger.info("所有队列已清空")
                    return True

                logger.debug(f"未完成任务: 下载={unfinished_download_tasks}, 通知={unfinished_notify_tasks}")

            await asyncio.sleep(1)
        except Exception as e:
            logger.error(f"检查队列状态时出错: {e}")
            break

    # Force clear on timeout
    logger.warning("等待队列清空超时，强制清理队列...")

    # Drain download queue
    try:
        while not ctx.download_queue.empty():
            try:
                ctx.download_queue.get_nowait()
                ctx.download_queue.task_done()
            except (asyncio.QueueEmpty, ValueError):
                break
    except Exception as e:
        logger.error(f"清空下载队列时出错: {e}")

    # Drain notification queue
    try:
        while not ctx.notify_queue.empty():
            try:
                ctx.notify_queue.get_nowait()
                ctx.notify_queue.task_done()
            except (asyncio.QueueEmpty, ValueError):
                break
    except Exception as e:
        logger.error(f"清空通知队列时出错: {e}")

    logger.warning("队列已强制清空")
    return False


def main():
    """Main entry point."""
    setup_exit_signal_handlers()

    # Task lists for cleanup
    notify_tasks = []
    download_tasks = []
    monitor_tasks = []
    chat_tasks = []

    client = None

    try:
        # Initialize application
        app.pre_run()
        init_web(app)

        ctx._media_download_count = _load_duplicate_count()

        # Print config summary
        print_config_summary(app)

        # Check config consistency
        issues = check_config_consistency(app)
        if issues:
            logger.warning("配置检查发现问题:")
            for i, issue in enumerate(issues, 1):
                logger.warning(f"  {i}. {issue}")
        else:
            logger.success("配置检查通过!")

        # Initialize Pyrogram client
        client = HookClient(
            "media_downloader",
            api_id=app.api_id,
            api_hash=app.api_hash,
            proxy=app.proxy,
            workdir=app.session_file_path,
            start_timeout=app.start_timeout,
        )

        # Update queue manager limits
        queue_manager.update_limits()

        # Re-initialize queues and semaphore with configured sizes
        ctx.download_queue = asyncio.Queue(maxsize=queue_manager.download_queue_size)
        ctx.notify_queue = asyncio.Queue(maxsize=100)
        ctx.download_semaphore = asyncio.Semaphore(queue_manager.max_download_tasks)

        logger.info(f"下载队列大小已设置为: {queue_manager.download_queue_size}")

        # Load notification manager config
        notification_manager.load_config()

        # Send startup notification (after notification system is initialized)
        async def send_startup_notification():
            if notification_manager.should_notify("startup"):
                startup_title = "程序启动"
                startup_message = (
                    f"🚀 Telegram媒体下载器已启动\n"
                    f"时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n"
                    f"版本: 1.0.0\n"
                    f"下载任务数: {len(app.chat_download_config)}\n"
                    f"通知系统: {'已启用' if notification_manager.bark_enabled or notification_manager.synology_chat_enabled else '未启用'}"
                )

                # Test notification delivery
                success = await notification_manager.send_event_notification(
                    "startup", startup_title, startup_message
                )
                if success:
                    logger.info("✅ 启动通知发送成功")
                else:
                    logger.warning("启动通知发送失败")

        # Verify cloud storage connectivity FIRST, before startup notification
        async def verify_cloud():
            if app.cloud_drive_config.enable_upload_file and app.cloud_drive_config.upload_adapter == "rclone":
                from module.cloud_drive import verify_rclone_remote
                cloud_ok, cloud_msg = await verify_rclone_remote(app.cloud_drive_config)
                if cloud_ok:
                    logger.success(f"☁️  {cloud_msg}")
                else:
                    ctx.cloud_upload_ok = False
                    logger.error(f"☁️  {cloud_msg}")
                    await notification_manager.send_event_notification("startup", "☁️ 云端写入验证失败，下载暂停", cloud_msg, "error")
                    logger.warning("☁️ 云端写入测试失败：download worker 已暂停，等待云端恢复后自动继续")

        app.loop.run_until_complete(verify_cloud())

        # Run startup notification
        app.loop.run_until_complete(send_startup_notification())

        # Set global exception handler
        def global_exception_handler(loop, context):
            exception = context.get('exception')
            if exception:
                logger.error(f"未处理的异常: {exception}")
            logger.error(f"异常上下文: {context}")

            if hasattr(app, 'force_exit') and app.force_exit:
                logger.info("强制退出程序中...")
                sys.exit(1)

        app.loop.set_exception_handler(global_exception_handler)
        set_max_concurrent_transmissions(client, app.max_concurrent_transmissions)

        # Start Pyrogram client
        app.loop.run_until_complete(start_server(client))
        logger.success(_t("Successfully started (Press Ctrl+C to stop)"))

        # Set running flags
        if not hasattr(app, 'force_exit'):
            app.force_exit = False
        if not hasattr(app, 'is_running'):
            app.is_running = True

        # Step 1: Start all workers
        notify_tasks = app.loop.run_until_complete(start_notify_workers())
        download_tasks = app.loop.run_until_complete(start_download_workers(client))

        # Step 2: Start monitor tasks
        if notification_manager.bark_enabled or notification_manager.synology_chat_enabled:
            # Start disk space monitor
            disk_monitor_task_obj = app.loop.create_task(disk_space_monitor_task())
            monitor_tasks.append(disk_monitor_task_obj)

            # Start stats notification
            stats_task_obj = app.loop.create_task(stats_notification_task())
            monitor_tasks.append(stats_task_obj)

            # Start queue monitor
            queue_monitor_obj = app.loop.create_task(queue_monitor_task())
            monitor_tasks.append(queue_monitor_obj)

            logger.info("通知系统已启用，监控任务已启动")
        else:
            logger.info("所有通知方式均未启用，跳过监控任务")

        # Step 3: Start chat download tasks (async)
        logger.info("启动聊天下载任务...")
        chat_task = app.loop.create_task(download_all_chat(client))
        chat_tasks.append(chat_task)

        # Give producers time to start
        app.loop.run_until_complete(asyncio.sleep(3))

        # Step 4: Start bot if configured
        if app.bot_token:
            logger.info("启动下载机器人...")
            bot_task = app.loop.create_task(
                start_download_bot(app, client, add_download_task, download_chat_task)
            )
            chat_tasks.append(bot_task)

        logger.info("=" * 60)
        logger.info("所有组件已启动，开始处理任务...")
        logger.info("失败任务将无限重试直到成功")
        logger.info("=" * 60)

        # Step 5: Enter main run loop
        app.loop.run_until_complete(run_until_all_task_finish())

    except KeyboardInterrupt:
        logger.info(_t("KeyboardInterrupt"))
        if hasattr(app, 'force_exit'):
            app.force_exit = True
    except Exception as e:
        logger.exception("{}", e)
    finally:
        # Set exit flags so all tasks know to exit
        app.is_running = False
        app.force_exit = True

        logger.info("=" * 60)
        logger.info("程序正在停止...")

        try:
            # Perform graceful shutdown first
            app.loop.run_until_complete(graceful_shutdown())
        except Exception as e:
            logger.error(f"优雅关闭过程中出错: {e}")

        # Cancel all tasks
        logger.info("取消所有任务...")
        all_tasks = []
        if 'chat_tasks' in locals():
            all_tasks.extend(chat_tasks)
        if 'monitor_tasks' in locals():
            all_tasks.extend(monitor_tasks)
        if 'download_tasks' in locals():
            all_tasks.extend(download_tasks)
        if 'notify_tasks' in locals():
            all_tasks.extend(notify_tasks)

        for task in all_tasks:
            if hasattr(task, 'done') and not task.done():
                try:
                    task.cancel()
                except:
                    pass

        # Brief wait for tasks to respond to cancellation
        try:
            app.loop.run_until_complete(asyncio.sleep(2))
        except:
            pass

        # Print final chat config state
        logger.info("当前聊天配置状态:")
        for chat_id, chat_config in app.chat_download_config.items():
            logger.info(
                f"  - 聊天 {chat_id}: last_read_message_id={getattr(chat_config, 'last_read_message_id', '未设置')}")

        logger.info(f"{_t('update config')}......")
        try:
            # Try to update config
            success = app.update_config()
            if success:
                logger.success(f"{_t('Updated last read message_id to config file')}")

                # Show updated config from app.config
                if hasattr(app, 'config') and 'chat' in app.config:
                    logger.info("更新后的聊天配置:")
                    for chat_item in app.config['chat']:
                        chat_id = chat_item.get('chat_id')
                        last_id = chat_item.get('last_read_message_id')
                        logger.info(f"  - chat_id: {chat_id}, last_read_message_id: {last_id}")
                else:
                    logger.warning("无法获取更新后的配置信息")
            else:
                logger.warning(f"配置更新可能失败，请检查日志")
        except Exception as e:
            logger.error(f"保存配置时出错: {e}")
            import traceback
            logger.error(f"堆栈信息: {traceback.format_exc()}")

        # Check config file size
        try:
            if os.path.exists(CONFIG_NAME):
                file_size = os.path.getsize(CONFIG_NAME)
                logger.info(f"配置文件大小: {file_size} 字节")
        except:
            pass

        if app.bot_token:
            try:
                app.loop.run_until_complete(stop_download_bot())
            except:
                pass

        try:
            if client:
                app.loop.run_until_complete(stop_server(client))
        except:
            pass

        logger.info(_t("Stopped!"))

        logger.info("=" * 60)
        logger.info("下载统计:")
        logger.success(
            f"{_t('total download')} {app.total_download_task}, "
            f"{_t('total upload file')} "
            f"{app.cloud_drive_config.total_upload_success_file_count}"
        )

        # Report remaining failed tasks
        try:
            async def get_final_failed_tasks():
                total = 0
                for chat_id, _ in app.chat_download_config.items():
                    failed_tasks = await load_failed_tasks(chat_id)
                    total += len(failed_tasks)
                return total

            total_failed_tasks = run_async_sync(get_final_failed_tasks(), timeout=30)
            if total_failed_tasks > 0:
                logger.warning(f"仍有 {total_failed_tasks} 个任务待重试，将在下次启动时继续重试")
        except Exception as e:
            logger.error(f"统计失败任务时出错: {e}")

        logger.info(f"队列管理器统计: 添加任务={queue_manager.task_added}, 处理任务={queue_manager.task_processed}")
        logger.info("=" * 60)


if __name__ == "__main__":
    if _check_config():
        main()