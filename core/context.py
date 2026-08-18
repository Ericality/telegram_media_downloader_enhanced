"""Application context — shared application instance and constants.

Centralizes module-level state that is accessed across multiple modules
(media_downloader, module.web, module.bot, core.storage, core.config, ...).
"""
from module.app import Application

CONFIG_NAME = "config.yaml"
DATA_FILE_NAME = "data.yaml"
APPLICATION_NAME = "media_downloader"

app = Application(CONFIG_NAME, DATA_FILE_NAME, APPLICATION_NAME)
