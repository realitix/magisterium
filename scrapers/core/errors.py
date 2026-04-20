"""Structured JSONL error logger."""
from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[2]
ERRORS_PATH = REPO_ROOT / "magisterium" / "_metadata" / "errors.log"


def log_error(
    source: str,
    url: str,
    phase: str,
    message: str,
    **extra: Any,
) -> None:
    ERRORS_PATH.parent.mkdir(parents=True, exist_ok=True)
    entry = {
        "ts": datetime.now(UTC).isoformat(),
        "source": source,
        "url": url,
        "phase": phase,
        "message": message,
        **extra,
    }
    with ERRORS_PATH.open("a", encoding="utf-8") as f:
        f.write(json.dumps(entry, ensure_ascii=False) + "\n")
