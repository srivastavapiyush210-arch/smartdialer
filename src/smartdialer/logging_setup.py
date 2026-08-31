"""Structured logging.

Every important decision is logged as ``[COMPONENT] key=value key=value`` so a
grep answers questions like "why did we request 17 calls?" without a debugger.
"""

from __future__ import annotations

import logging
import sys
from typing import Any

_CONFIGURED = False


def configure(level: int = logging.INFO) -> None:
    global _CONFIGURED
    if _CONFIGURED:
        logging.getLogger("smartdialer").setLevel(level)
        return
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)-5s %(message)s",
                                           datefmt="%H:%M:%S"))
    root = logging.getLogger("smartdialer")
    root.handlers.clear()
    root.addHandler(handler)
    root.setLevel(level)
    root.propagate = False
    _CONFIGURED = True


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(f"smartdialer.{name}")


def kv(component: str, **fields: Any) -> str:
    """Render a structured log line."""
    parts = []
    for key, value in fields.items():
        if isinstance(value, float):
            value = f"{value:.3f}".rstrip("0").rstrip(".")
        parts.append(f"{key}={value}")
    return f"[{component}] " + " ".join(parts)
