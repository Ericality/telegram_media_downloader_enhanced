"""Config loading, checking and summary helpers."""
import logging
import os
import sys
from datetime import datetime

from loguru import logger

from core.context import app
from utils.meta import print_meta


def _load_config():
    """Load application config."""
    app.load_config()


def _check_config() -> bool:
    """Check and apply config."""
    print_meta(logger)
    try:
        _load_config()

        # Remove loguru default handler
        logger.remove()

        # Set log level from config
        log_level = app.log_level.upper() if hasattr(app, "log_level") else "INFO"

        logger.debug(f"设置日志级别为: {log_level}")

        # Add console handler
        logger.add(
            sys.stderr,
            level=log_level,
            format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | <level>{level: <8}</level> | <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - <level>{message}</level>",
            colorize=True,
            backtrace=False,
            diagnose=False,
        )

        # Archive previous log file on startup
        log_path = os.path.join(app.log_file_path, "tdl.log")
        if os.path.exists(log_path) and os.path.getsize(log_path) > 0:
            archived = os.path.join(
                app.log_file_path, f"tdl.{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
            )
            try:
                os.rename(log_path, archived)
            except OSError:
                pass  # if rename fails (e.g. file locked), just append

        # Add file handler
        logger.add(
            log_path,
            rotation="10 MB",
            retention="10 days",
            level=log_level,
            format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | {name}:{function}:{line} - {message}",
            backtrace=False,
            diagnose=False,
        )

        # Set stdlib logging level
        if log_level == "DEBUG":
            os.environ["DEBUG"] = "1"
            logging.getLogger().setLevel(logging.DEBUG)
        else:
            if "DEBUG" in os.environ:
                os.environ.pop("DEBUG")
            logging.getLogger().setLevel(getattr(logging, log_level, logging.INFO))

        # Verify log level immediately
        logger.debug(f"DEBUG日志测试 - 如果看到这一行，说明日志级别是DEBUG")
        logger.info(f"INFO日志测试 - 程序启动，日志级别设置为: {log_level}")

        return True
    except Exception as e:
        logger.exception(f"load config error: {e}")
        return False


def print_config_summary(app):
    """Print config summary for debugging."""
    logger.info("=" * 60)
    logger.info("配置摘要 (用于调试)")
    logger.info("=" * 60)

    # Basic info
    logger.info("基本信息:")
    logger.info(f"  配置文件名: {app.config_file}")
    logger.info(f"  数据文件名: {app.app_data_file}")
    logger.info(f"  应用名称: {app.application_name}")
    logger.info(f"  会话文件路径: {app.session_file_path}")
    logger.info(f"  日志文件路径: {app.log_file_path}")
    logger.info(f"  日志级别: {app.log_level}")
    logger.info(f"  启动超时: {app.start_timeout}秒")

    # API config (credentials masked)
    logger.info("\nAPI配置:")
    logger.info(f"  API ID: {'已设置' if app.api_id else '未设置'}")
    logger.info(f"  API Hash: {'已设置' if app.api_hash else '未设置'}")
    logger.info(f"  Bot Token: {'已设置' if app.bot_token else '未设置'}")
    logger.info(f"  代理: {app.proxy if app.proxy else '未设置'}")

    # Download config
    logger.info("\n下载配置:")
    logger.info(f"  下载路径: {app.save_path}")
    logger.info(f"  临时路径: {app.temp_save_path}")
    logger.info(f"  媒体类型: {app.media_types}")
    logger.info(f"  文件格式: {app.file_formats}")
    logger.info(f"  最大下载任务数: {app.max_download_task}")
    logger.info(f"  最大并发传输数: {app.max_concurrent_transmissions}")
    logger.info(f"  隐藏文件名: {app.hide_file_name}")
    logger.info(f"  日期格式: {app.date_format}")
    logger.info(f"  启用文本下载: {app.enable_download_txt}")
    logger.info(f"  丢弃无音视频: {app.drop_no_audio_video}")

    # Notification config
    logger.info("\n通知配置:")

    # Check for new-format notifications config
    if hasattr(app, "notifications"):
        notifications = app.notifications
        logger.info("  [新版配置]")

        # Bark config
        bark_config = notifications.get("bark", {})
        logger.info(f"  Bark通知:")
        logger.info(f"    启用: {bark_config.get('enabled', False)}")
        if bark_config.get("enabled", False):
            logger.info(f"    URL: {'已设置' if bark_config.get('url') else '未设置'}")
            logger.info(
                f"    默认分组: {bark_config.get('default_group', 'TelegramDownloader')}"
            )
            logger.info(f"    默认级别: {bark_config.get('default_level', 'active')}")
            logger.info(
                f"    磁盘空间阈值: {bark_config.get('disk_space_threshold_gb', 10.0)}GB"
            )
            logger.info(f"    空间检查间隔: {bark_config.get('space_check_interval', 300)}秒")
            logger.info(
                f"    统计通知间隔: {bark_config.get('stats_notification_interval', 3600)}秒"
            )
            logger.info(f"    通知worker数量: {bark_config.get('notify_worker_count', 1)}")
            logger.info(f"    通知事件列表: {bark_config.get('events_to_notify', [])}")

        # Synology Chat config
        synology_config = notifications.get("synology_chat", {})
        logger.info(f"  群晖Chat通知:")
        logger.info(f"    启用: {synology_config.get('enabled', False)}")
        if synology_config.get("enabled", False):
            logger.info(
                f"    Webhook URL: {'已设置' if synology_config.get('webhook_url') else '未设置'}"
            )
            logger.info(f"    机器人名称: {synology_config.get('bot_name', 'Telegram下载器')}")
            logger.info(f"    默认级别: {synology_config.get('default_level', 'info')}")
            logger.info(f"    通知事件列表: {synology_config.get('events_to_notify', [])}")

        # Global config
        global_config = notifications.get("global", {})
        logger.info(f"  全局配置:")
        logger.info(
            f"    统计通知间隔: {global_config.get('stats_notification_interval', 3600)}秒"
        )
        logger.info(f"    队列监控间隔: {global_config.get('queue_monitor_interval', 300)}秒")
        logger.info(f"    最大重试次数: {global_config.get('max_notification_retries', 3)}")

    # Also check legacy config (backward compat)
    elif hasattr(app, "bark_notification"):
        bark_config = app.bark_notification
        logger.info("  [旧版配置]")
        logger.info(f"  Bark通知:")
        logger.info(f"    启用: {bark_config.get('enabled', False)}")
        if bark_config.get("enabled", False):
            logger.info(f"    URL: {'已设置' if bark_config.get('url') else '未设置'}")
            logger.info(
                f"    磁盘空间阈值: {bark_config.get('disk_space_threshold_gb', 10.0)}GB"
            )
            logger.info(f"    空间检查间隔: {bark_config.get('space_check_interval', 300)}秒")
            logger.info(
                f"    统计通知间隔: {bark_config.get('stats_notification_interval', 3600)}秒"
            )
            logger.info(f"    通知worker数量: {bark_config.get('notify_worker_count', 1)}")
            logger.info(f"    通知事件列表: {bark_config.get('events_to_notify', [])}")
    else:
        logger.info("  通知配置: 未找到")

    # File naming config
    logger.info("\n文件命名配置:")
    logger.info(f"  文件路径前缀: {app.file_path_prefix}")
    logger.info(f"  文件名前缀: {app.file_name_prefix}")
    logger.info(f"  文件名前缀分隔符: {app.file_name_prefix_split}")

    # Web config
    logger.info("\nWeb配置:")
    logger.info(f"  Web主机: {app.web_host}")
    logger.info(f"  Web端口: {app.web_port}")
    logger.info(f"  Web调试模式: {app.debug_web}")
    logger.info(f"  Web登录密钥: {'已设置' if app.web_login_secret else '未设置'}")

    # Language and permissions
    logger.info("\n语言和权限:")
    logger.info(f"  语言: {app.language}")
    logger.info(
        f"  允许的用户ID: {len(app.allowed_user_ids) if app.allowed_user_ids else 0}个"
    )
    if app.allowed_user_ids and len(app.allowed_user_ids) <= 10:
        logger.info(f"    具体ID: {list(app.allowed_user_ids)}")

    # Chat config
    logger.info("\n聊天配置:")
    logger.info(f"  聊天数量: {len(app.chat_download_config)}")
    for i, (chat_id, config) in enumerate(app.chat_download_config.items(), 1):
        logger.info(f"  聊天 #{i}:")
        logger.info(f"    ID: {chat_id}")
        logger.info(f"    最后读取消息ID: {config.last_read_message_id}")
        logger.info(f"    待重试消息数: {len(config.ids_to_retry)}")
        logger.info(
            f"    过滤器: {config.download_filter[:50] + '...' if config.download_filter and len(config.download_filter) > 50 else config.download_filter}"
        )
        logger.info(f"    上传Telegram聊天ID: {config.upload_telegram_chat_id}")

    # Cloud drive config
    logger.info("\n云存储配置:")
    logger.info(f"  启用文件上传: {app.cloud_drive_config.enable_upload_file}")
    if app.cloud_drive_config.enable_upload_file:
        logger.info(f"  上传适配器: {app.cloud_drive_config.upload_adapter}")
        logger.info(f"  Rclone路径: {app.cloud_drive_config.rclone_path}")
        logger.info(f"  远程目录: {app.cloud_drive_config.remote_dir}")
        logger.info(f"  上传前压缩: {app.cloud_drive_config.before_upload_file_zip}")
        logger.info(f"  上传后删除: {app.cloud_drive_config.after_upload_file_delete}")
        logger.info(
            f"  云端空间阈值: {app.cloud_drive_config.cloud_space_threshold_gb}GB"
            f"{' (0=不启用)' if app.cloud_drive_config.cloud_space_threshold_gb == 0 else ''}"
        )

    # Other config
    logger.info("\n其他配置:")
    logger.info(f"  程序重启标志: {app.restart_program}")
    logger.info(f"  上传Telegram后删除: {app.after_upload_telegram_delete}")
    logger.info(
        f"  转发限制: {app.forward_limit_call.max_limit_call_times if hasattr(app, 'forward_limit_call') else '未设置'}"
    )

    logger.info("=" * 60)


def check_config_consistency(app):
    """Check config consistency and report issues."""
    issues = []

    # Check API config
    if not app.api_id or not app.api_hash:
        issues.append("API ID或API Hash未设置")

    # Check download path
    if not os.path.exists(app.save_path):
        logger.warning(f"下载路径不存在: {app.save_path}")
        issues.append(f"下载路径不存在: {app.save_path}")

    # Check media types
    if not app.media_types:
        issues.append("媒体类型未设置")

    # Check file formats
    if not app.file_formats:
        issues.append("文件格式未设置")

    # Check chat config
    if not app.chat_download_config:
        issues.append("聊天配置为空")

    # Check notification config
    notifications_config = getattr(app, "notifications", {})

    # Check Bark config
    bark_config = notifications_config.get("bark", {})
    if bark_config.get("enabled", False):
        if not bark_config.get("url"):
            issues.append("Bark通知已启用但URL未设置")

    # Check Synology Chat config
    synology_config = notifications_config.get("synology_chat", {})
    if synology_config.get("enabled", False):
        if not synology_config.get("webhook_url"):
            issues.append("群晖Chat通知已启用但Webhook URL未设置")

    return issues
