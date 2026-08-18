"""Notification queue worker."""
import asyncio
import time

from loguru import logger

import core.context as ctx
from core.context import app
from services.notifier import send_bark_notification_sync, send_synology_chat_notification_sync


async def notify_worker(worker_id: int):
    """Notification queue worker with delay monitoring."""
    logger.debug(f"通知Worker {worker_id} 启动")

    while True:
        # Check exit signal; continue processing if queue is not empty
        should_exit = getattr(app, 'force_exit', False) or not getattr(app, 'is_running', True)

        try:
            # Exit only if queue is empty
            if should_exit and ctx.notify_queue.empty():
                logger.debug(f"通知Worker {worker_id} 队列已空，准备退出")
                break

            # Use timed get to avoid indefinite blocking
            try:
                task = await asyncio.wait_for(ctx.notify_queue.get(), timeout=0.5)
            except asyncio.TimeoutError:
                continue

            task_type = task.get('type')
            create_time = task.get('create_time', '未知')
            queue_time = task.get('queue_time', time.time())

            # Calculate delay
            current_time = time.time()
            delay_seconds = current_time - queue_time

            # Log delay if significant
            if delay_seconds > 10:  # Warn if delay exceeds 10s
                logger.warning(f"通知Worker {worker_id}: 任务延迟 {delay_seconds:.1f} 秒, 创建时间={create_time}")
            elif delay_seconds > 60:  # Critical if delay exceeds 60s
                logger.error(f"通知Worker {worker_id}: 任务严重延迟 {delay_seconds:.1f} 秒, 创建时间={create_time}")

            if task_type == 'bark_notification':
                # Process Bark notification
                title = task.get('title')
                body = task.get('body')
                url = task.get('url')
                group = task.get('group')
                level = task.get('level')

                logger.debug(f"通知Worker {worker_id} 处理Bark通知: {title}, 延迟={delay_seconds:.1f}秒")

                try:
                    success = await send_bark_notification_sync(title, body, url, group, level)
                    if success:
                        logger.debug(f"通知Worker {worker_id}: {title} 发送成功, 总延迟={delay_seconds:.1f}秒")
                    else:
                        logger.warning(f"通知Worker {worker_id}: {title} 发送失败, 延迟={delay_seconds:.1f}秒")
                except Exception as e:
                    logger.error(f"通知Worker {worker_id} 发送Bark通知时出错: {e}, 延迟={delay_seconds:.1f}秒")
                finally:
                    ctx.notify_queue.task_done()

            elif task_type == 'synology_chat_notification':
                # Process Synology Chat notification
                title = task.get('title')
                message = task.get('message')
                level = task.get('level', 'info')
                webhook_url = task.get('webhook_url')
                bot_name = task.get('bot_name')
                bot_avatar = task.get('bot_avatar')
                mention_users = task.get('mention_users')
                mention_channels = task.get('mention_channels')

                logger.debug(f"通知Worker {worker_id} 处理群晖Chat通知: {title}, 延迟={delay_seconds:.1f}秒")

                try:
                    success = await send_synology_chat_notification_sync(
                        title, message, level, webhook_url, bot_name, bot_avatar,
                        mention_users, mention_channels
                    )
                    if success:
                        logger.debug(
                            f"通知Worker {worker_id}: 群晖Chat通知 {title} 发送成功, 延迟={delay_seconds:.1f}秒")
                    else:
                        logger.warning(
                            f"通知Worker {worker_id}: 群晖Chat通知 {title} 发送失败, 延迟={delay_seconds:.1f}秒")
                except Exception as e:
                    logger.error(f"通知Worker {worker_id} 发送群晖Chat通知时出错: {e}, 延迟={delay_seconds:.1f}秒")
                finally:
                    ctx.notify_queue.task_done()

            elif task_type == 'stats_notification':
                # Placeholder for future notification types
                pass

        except asyncio.CancelledError:
            logger.debug(f"通知Worker {worker_id} 被取消")
            break
        except Exception as e:
            logger.error(f"通知Worker {worker_id} 异常: {e}")
            try:
                ctx.notify_queue.task_done()
            except:
                pass
            await asyncio.sleep(1)

    logger.debug(f"通知Worker {worker_id} 退出")
