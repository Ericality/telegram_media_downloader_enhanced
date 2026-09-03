"""Notification services — Bark push and Synology Chat Bot."""
import asyncio
import json
import time
import urllib.parse
from datetime import datetime

import aiohttp
from loguru import logger

import core.context as ctx
from core.context import app


class NotificationManager:
    """Notification manager.

    Manages both Bark push and Synology Chat Bot notifications,
    with per-event-type toggle, grouping, and level configuration.
    """

    def __init__(self):
        self.bark_enabled = False
        self.synology_chat_enabled = False

    def _notifications(self) -> dict:
        """Read notifications config live from the unified app config store."""
        return app.get_config("notifications", {})

    @property
    def bark_config(self) -> dict:
        """Bark notification config (live, no cached copy)."""
        return self._notifications().get("bark", {})

    @property
    def synology_chat_config(self) -> dict:
        """Synology Chat config (live, no cached copy)."""
        return self._notifications().get("synology_chat", {})

    @property
    def global_config(self) -> dict:
        """Global notification config (live, no cached copy)."""
        return self._notifications().get("global", {})

    def load_config(self):
        """Load notification config from app settings."""
        self.bark_enabled = self.bark_config.get("enabled", False)
        self.synology_chat_enabled = self.synology_chat_config.get("enabled", False)
        logger.info(
            f"通知管理器加载: Bark={self.bark_enabled}, 群晖Chat={self.synology_chat_enabled}"
        )

    def should_notify(self, event_type: str, notification_type: str = None) -> bool:
        """Check whether a notification type should be sent for a given event."""
        if notification_type == "bark":
            if not self.bark_enabled:
                return False
            events_to_notify = self.bark_config.get("events_to_notify", [])
            return event_type in events_to_notify

        elif notification_type == "synology_chat":
            if not self.synology_chat_enabled:
                return False
            events_to_notify = self.synology_chat_config.get("events_to_notify", [])
            return event_type in events_to_notify

        # If no specific type, check if any notification channel should send
        bark_should = self.should_notify(event_type, "bark")
        synology_should = self.should_notify(event_type, "synology_chat")
        return bark_should or synology_should

    async def send_event_notification(
        self,
        event_type: str,
        title: str,
        body: str,
        level: str = None,
        custom_config: dict = None,
    ):
        """Send event notification via all enabled channels."""
        tasks = []

        # Send Bark notification
        if self.should_notify(event_type, "bark"):
            bark_group = self.bark_config.get("default_group")
            bark_level = level or self.bark_config.get("default_level")

            if custom_config and custom_config.get("bark"):
                bark_group = custom_config["bark"].get("group", bark_group)
                bark_level = custom_config["bark"].get("level", bark_level)

            task = asyncio.create_task(
                send_bark_notification(title, body, group=bark_group, level=bark_level)
            )
            tasks.append(task)

        # Send Synology Chat notification
        if self.should_notify(event_type, "synology_chat"):
            synology_level = level or self.synology_chat_config.get(
                "default_level", "info"
            )

            if custom_config and custom_config.get("synology_chat"):
                synology_level = custom_config["synology_chat"].get(
                    "level", synology_level
                )

            task = asyncio.create_task(
                send_synology_chat_notification(title, body, level=synology_level)
            )
            tasks.append(task)

        # Wait for all notifications to complete
        if tasks:
            results = await asyncio.gather(*tasks, return_exceptions=True)
            success_count = sum(
                1 for r in results if r is True and not isinstance(r, Exception)
            )

            if success_count == 0:
                logger.warning(f"事件 {event_type} 的所有通知发送失败")
            elif success_count < len(tasks):
                logger.warning(f"事件 {event_type} 的部分通知发送失败")

            return success_count > 0

        return False

    async def send_disk_space_notification(
        self,
        has_space: bool,
        available_gb: float,
        total_gb: float,
        threshold_gb: float,
        cloud_extra: str = "",
        bark_level: str = None,
    ):
        """Send storage space notification (local + optional cloud info).

        bark_level controls the Bark sound/level only (valid Bark values:
        ``active``/``timeSensitive``/``passive``; ``None`` = use the Bark
        default_level from config, normally the ringing ``active``). Synology
        Chat keeps the generic info/warning level.
        """
        if has_space:
            title = "存储空间充足"
            message = (
                f"✅ 存储空间充足\n"
                f"本地磁盘可用: {available_gb:.2f}GB / {total_gb:.2f}GB\n"
                f"本地阈值: {threshold_gb}GB"
            )
            level = "info"
        else:
            title = "存储空间不足"
            message = (
                f"⚠️ 存储空间不足\n"
                f"本地磁盘可用: {available_gb:.2f}GB / {total_gb:.2f}GB\n"
                f"本地阈值: {threshold_gb}GB"
            )
            level = "warning"
        if cloud_extra:
            message += cloud_extra

        # Bark-only level override (do not leak info/warning into Bark's level)
        bark_override = bark_level or self.bark_config.get("default_level", "active")
        custom_config = {"bark": {"level": bark_override}}

        return await self.send_event_notification(
            "disk_space", title, message, level, custom_config
        )

    async def send_queue_notification(
        self, current_size: int, capacity: int, wait_time_minutes: int = None
    ):
        """Send queue status notification."""
        usage_percent = int(current_size / capacity * 100) if capacity > 0 else 0

        if wait_time_minutes and wait_time_minutes > 60:
            title = "队列长时间满载"
            message = f"⚠️ 队列长时间满载\n使用率: {current_size}/{capacity} ({usage_percent}%)\n已等待: {wait_time_minutes}分钟"
            event_type = "queue_full"
            level = "warning"
        else:
            title = "队列状态报告"
            message = f"📊 队列状态报告\n使用率: {current_size}/{capacity} ({usage_percent}%)"
            event_type = "queue_status"
            level = "info"

        return await self.send_event_notification(event_type, title, message, level)

    async def send_stats_notification(self, stats: dict):
        """Send statistics notification."""
        from services.stats import get_storage_summary_text
        from workers.monitor import disk_monitor

        title = "下载统计"
        storage_line = (
            stats.get("storage_summary", "") or await get_storage_summary_text()
        )
        retried = disk_monitor.retry_success_count
        retry_info = f"\n重试成功: {retried}" if retried > 0 else ""
        message = (
            f"📊 统计摘要\n"
            f"运行时间: {stats.get('uptime', 'N/A')}\n"
            f"完成任务: {stats.get('tasks_completed', 0)}\n"
            f"失败任务(待重试): {stats.get('failed_tasks_pending', 0)}{retry_info}\n"
            f"下载大小: {stats.get('download_size_mb', 0):.2f}MB\n"
            f"磁盘可用: {stats.get('disk_available_gb', 0):.2f}GB/{stats.get('disk_total_gb', 0):.2f}GB\n"
            f"下载目录大小: {stats.get('download_dir_size_gb', 0):.2f}GB\n"
            f"活动任务: {stats.get('active_tasks', 0)}\n"
            f"队列任务: {stats.get('queued_tasks', 0)}\n"
            f"空间不足: {'是' if stats.get('space_low', False) else '否'}"
        )
        if storage_line:
            message += f"\n💾 {storage_line}"

        return await self.send_event_notification(
            "stats_summary", title, message, "info"
        )

    async def send_test_notification(self):
        """Send test notification."""
        test_title = "测试通知"
        test_message = "Telegram媒体下载器通知系统测试成功！"

        bark_success = False
        if self.bark_enabled:
            bark_success = await send_bark_notification(test_title, test_message)
            logger.info(f"Bark测试通知: {'成功' if bark_success else '失败'}")

        synology_success = False
        if self.synology_chat_enabled:
            synology_success = await send_synology_chat_notification(
                test_title, test_message
            )
            logger.info(f"群晖Chat测试通知: {'成功' if synology_success else '失败'}")

        return {"bark": bark_success, "synology_chat": synology_success}


notification_manager = NotificationManager()


async def send_bark_notification_sync(
    title: str,
    body: str,
    url: str = None,
    group: str = None,
    level: str = None,
    max_retries: int = 2,
):
    """Send Bark notification synchronously with retry and group/level support."""
    if not url:
        bark_config = app.get_config("bark_notification", {})
        if not bark_config.get("enabled", False):
            return False
        url = bark_config.get("url", "")

    if not url:
        logger.warning("Bark通知URL未设置")
        return False

    # Ensure URL has scheme
    if not url.startswith("http"):
        url = f"https://{url}"

    # Get default group, level and sound from config
    bark_config = app.get_config("bark_notification", {})
    default_group = bark_config.get("default_group", "TelegramDownloader")
    default_level = bark_config.get("default_level", "active")
    sound = bark_config.get("sound", "alarm")

    # Build payload
    payload = {
        "title": title[:100],  # Limit title length
        "body": body[:500],  # Limit body length
        "sound": sound,
        "icon": "https://telegram.org/img/t_logo.png",
    }

    # Add group param (use provided or default)
    if group:
        payload["group"] = group
    elif default_group:
        payload["group"] = default_group

    # Add level param (use provided or default)
    if level:
        payload["level"] = level
    elif default_level:
        payload["level"] = default_level

    # Retry logic
    for retry in range(max_retries + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=15)  # 15s timeout
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(url, json=payload, timeout=timeout) as response:
                    if response.status == 200:
                        logger.debug(
                            f"Bark通知发送成功: {title}, group={payload.get('group')}, level={payload.get('level')}"
                        )
                        return True
                    else:
                        response_text = await response.text()
                        logger.warning(
                            f"Bark通知发送失败: HTTP {response.status}, 响应: {response_text[:100]}"
                        )

                        # Client error (4xx): do not retry
                        if 400 <= response.status < 500:
                            return False

                        # Server error (5xx): retry with backoff
                        if retry < max_retries:
                            wait_time = 2**retry  # Exponential backoff
                            logger.info(
                                f"等待 {wait_time} 秒后重试 ({retry + 1}/{max_retries})..."
                            )
                            await asyncio.sleep(wait_time)
        except asyncio.TimeoutError:
            logger.warning(f"Bark通知超时 ({retry + 1}/{max_retries + 1})")
            if retry < max_retries:
                await asyncio.sleep(2**retry)
        except aiohttp.ClientError as e:
            logger.warning(f"Bark通知网络错误: {e} ({retry + 1}/{max_retries + 1})")
            if retry < max_retries:
                await asyncio.sleep(2**retry)
        except Exception as e:
            logger.error(f"发送Bark通知时出错: {e}")
            return False

    return False


async def send_bark_notification(
    title: str, body: str, url: str = None, group: str = None, level: str = None
):
    """Enqueue Bark notification with timestamp."""
    try:
        # Add creation timestamp
        create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Put notification task into queue
        await ctx.notify_queue.put(
            {
                "type": "bark_notification",
                "title": title,
                "body": body,
                "url": url,
                "group": group,
                "level": level,
                "create_time": create_time,  # Creation time
                "queue_time": time.time(),  # Queue entry timestamp (Unix)
            }
        )
        logger.debug(f"已添加通知任务到队列: {title}, 创建时间={create_time}")
        return True
    except asyncio.QueueFull:
        logger.warning("通知队列已满，丢弃通知")
        return False
    except Exception as e:
        logger.error(f"添加通知任务到队列失败: {e}")
        return False


async def send_synology_chat_notification_sync(
    title: str,
    message: str,
    level: str = "info",
    webhook_url: str = None,
    bot_name: str = None,
    bot_avatar: str = None,
    mention_users: list = None,
    mention_channels: list = None,
    max_retries: int = 2,
) -> bool:
    """Send Synology Chat notification synchronously (url-encoded format)."""
    # Get config
    notifications_config = app.get_config("notifications", {})
    synology_config = notifications_config.get("synology_chat", {})

    if not synology_config.get("enabled", False):
        logger.debug("群晖 Chat Bot 未启用")
        return False

    if not webhook_url:
        webhook_url = synology_config.get("webhook_url", "")

    if not webhook_url:
        logger.warning("群晖 Chat Bot Webhook URL 未设置")
        return False

    # Mask token in log output
    safe_url = webhook_url
    if "token=" in safe_url:
        parts = safe_url.split("token=")
        if len(parts) > 1:
            token = parts[1]
            if len(token) > 10:
                masked_token = token[:10] + "..." + token[-5:]
                safe_url = parts[0] + "token=" + masked_token

    logger.debug(f"群晖 Chat Webhook URL: {safe_url}")

    # Select emoji by level
    level_config = {
        "info": {"emoji": "ℹ️"},
        "warning": {"emoji": "⚠️"},
        "error": {"emoji": "❌"},
        "success": {"emoji": "✅"},
    }

    level_info = level_config.get(level.lower(), level_config["info"])

    # Build full message
    full_message = f"{level_info['emoji']} {title}\n\n{message}"

    # Build mention string
    mention_text = ""
    if mention_users:
        for user in mention_users:
            mention_text += f"@{user} "

    if mention_channels:
        for channel in mention_channels:
            mention_text += f"#{channel} "

    if mention_text:
        full_message += f"\n\n{mention_text}"

    logger.debug(f"准备发送群晖 Chat 通知: {title}, 级别: {level}")

    # Build payload (verified format)
    payload_json = {"text": full_message}

    # Convert payload to string and URL-encode
    payload_str = json.dumps(payload_json, ensure_ascii=False)
    encoded_payload = urllib.parse.quote(payload_str)

    data = f"payload={encoded_payload}"

    logger.debug(f"请求数据长度: {len(data)} 字符")

    for retry in range(max_retries + 1):
        try:
            timeout = aiohttp.ClientTimeout(total=15)
            headers = {
                "Content-Type": "application/x-www-form-urlencoded",
                "Accept": "application/json",
            }

            async with aiohttp.ClientSession(
                timeout=timeout, headers=headers
            ) as session:
                async with session.post(
                    webhook_url, data=data, timeout=timeout
                ) as response:
                    response_text = await response.text()

                    if response.status in [200, 201, 204]:
                        try:
                            response_json = json.loads(response_text)
                            if response_json.get("success", False):
                                logger.info(f"群晖 Chat 通知发送成功: {title}")
                                return True
                            else:
                                error_msg = response_json.get("error", {}).get(
                                    "errors", "未知错误"
                                )
                                logger.warning(f"群晖 Chat 通知返回失败: {error_msg}")
                        except json.JSONDecodeError:
                            logger.info(
                                f"群晖 Chat 通知发送成功，但响应不是JSON: {response_text[:100]}"
                            )
                            return True
                        except Exception as e:
                            logger.warning(
                                f"解析群晖 Chat 响应时出错: {e}, 响应: {response_text[:100]}"
                            )
                            return True
                    else:
                        logger.warning(f"群晖 Chat 通知发送失败: HTTP {response.status}")

                        try:
                            error_json = json.loads(response_text)
                            error_msg = error_json.get("error", {}).get(
                                "errors", response_text[:200]
                            )
                            logger.debug(f"错误详情: {error_msg}")
                        except:
                            logger.debug(f"响应内容: {response_text[:200]}")

                        if retry < max_retries:
                            wait_time = 2**retry
                            logger.info(
                                f"等待 {wait_time} 秒后重试 ({retry + 1}/{max_retries})..."
                            )
                            await asyncio.sleep(wait_time)
                        else:
                            return False
        except asyncio.TimeoutError:
            logger.warning(f"群晖 Chat 通知超时 ({retry + 1}/{max_retries + 1})")
            if retry < max_retries:
                await asyncio.sleep(2**retry)
            else:
                break
        except aiohttp.ClientError as e:
            logger.warning(f"群晖 Chat 通知网络错误: {e} ({retry + 1}/{max_retries + 1})")
            if retry < max_retries:
                await asyncio.sleep(2**retry)
            else:
                break
        except Exception as e:
            logger.error(f"发送群晖 Chat 通知时出错: {e}")
            return False

    logger.error(f"群晖 Chat 通知发送失败，已尝试 {max_retries + 1} 次")
    return False


async def send_synology_chat_notification(
    title: str,
    message: str,
    level: str = "info",
    webhook_url: str = None,
    bot_name: str = None,
    bot_avatar: str = None,
    mention_users: list = None,
    mention_channels: list = None,
) -> bool:
    """Enqueue Synology Chat notification with timestamp."""
    try:
        create_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        await ctx.notify_queue.put(
            {
                "type": "synology_chat_notification",
                "title": title,
                "message": message,
                "level": level,
                "webhook_url": webhook_url,
                "bot_name": bot_name,
                "bot_avatar": bot_avatar,
                "mention_users": mention_users,
                "mention_channels": mention_channels,
                "create_time": create_time,  # Creation time
                "queue_time": time.time(),  # Queue entry timestamp
            }
        )
        logger.debug(f"已添加群晖 Chat 通知任务到队列: {title}, 创建时间={create_time}")
        return True
    except asyncio.QueueFull:
        logger.warning("通知队列已满，丢弃群晖 Chat 通知")
        return False
    except Exception as e:
        logger.error(f"添加群晖 Chat 通知任务到队列失败: {e}")
        return False
