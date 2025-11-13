# common/logging_conf.py
import logging
import os
from typing import Optional

_DEFAULT_FMT = "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"

def setup_logging(level: Optional[str] = None) -> None:
    """
    Initialize root logging once. Level can be 'DEBUG', 'INFO', etc.
    Falls back to LOG_LEVEL env or INFO.
    """
    if getattr(setup_logging, "_inited", False):
        return
    lvl_name = (level or os.getenv("LOG_LEVEL") or "INFO").upper()
    lvl = getattr(logging, lvl_name, logging.INFO)
    logging.basicConfig(level=lvl, format=_DEFAULT_FMT)
    setup_logging._inited = True  # type: ignore[attr-defined]

def get_logger(name: str) -> logging.Logger:
    """
    Convenience getter; ensures logging configured.
    """
    setup_logging()
    return logging.getLogger(name)
