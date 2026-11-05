from __future__ import annotations

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
import os


def configure_logging(project_root: Path, log_dir: str) -> logging.Logger:
    level = getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO)
    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()

    console = logging.StreamHandler()
    console.setLevel(level)
    console.setFormatter(logging.Formatter("%(asctime)s | %(levelname)-7s | %(message)s", "%Y-%m-%d %H:%M:%S"))
    root.addHandler(console)

    out_dir = project_root / log_dir
    out_dir.mkdir(parents=True, exist_ok=True)
    file_handler = RotatingFileHandler(
        out_dir / "bot.log", maxBytes=5_000_000, backupCount=5, encoding="utf-8"
    )
    file_handler.setLevel(level)
    file_handler.setFormatter(logging.Formatter("%(asctime)s | %(levelname)s | %(name)s | %(message)s"))
    root.addHandler(file_handler)

    return logging.getLogger("crypto_momentum")
