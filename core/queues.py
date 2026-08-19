"""Download queue manager."""
import asyncio

from loguru import logger


class QueueManager:
    """Download queue manager.

    Manages download/notification worker limits, queue capacity, and task counters.
    """

    def __init__(self):
        self.max_download_tasks = 0
        self.max_notify_tasks = 1
        self.download_queue_size = 0
        self.task_added = 0
        self.task_processed = 0
        self.lock = asyncio.Lock()

    def update_limits(self):
        """Update queue limits from config."""
        from core.context import app
        self.max_download_tasks = app.get_config('max_download_task', 5)
        # Read notify worker count from config
        bark_config = app.get_config('bark_notification', {})
        self.max_notify_tasks = bark_config.get('notify_worker_count', 1)
        # Queue size set to worker count
        self.download_queue_size = self.max_download_tasks
        logger.info(f"队列管理器初始化: 下载worker={self.max_download_tasks}, "
                    f"通知worker={self.max_notify_tasks}, 下载队列大小={self.download_queue_size}")
