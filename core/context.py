"""Application context — shared application instance and state.

Centralizes module-level state that is accessed across multiple modules
(media_downloader, module.web, module.bot, core.storage, core.config,
workers.*, services.*).
"""
from core.queues import QueueManager
from module.app import Application

CONFIG_NAME = "config.yaml"
DATA_FILE_NAME = "data.yaml"
APPLICATION_NAME = "media_downloader"

app = Application(CONFIG_NAME, DATA_FILE_NAME, APPLICATION_NAME)

# 去重与下载计数状态
_media_seen: set = set()
_media_download_count: dict = {}

# 云端上传健康标志：False 表示上传验证失败，下载 worker 暂停
cloud_upload_ok: bool = True

# 队列管理器
queue_manager = QueueManager()

# asyncio 队列 / 信号量（main() 中按配置重新初始化）
download_semaphore = None
download_queue = None
notify_queue = None

# 下载重试超时（秒）
RETRY_TIME_OUT = 3
