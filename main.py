"""Telegram Media Downloader — main entry point.

Wraps ``media_downloader.main`` for backward compatibility while providing a
canonical entry module for the refactored codebase.
"""
from media_downloader import _check_config, main

if __name__ == "__main__":
    if _check_config():
        main()
