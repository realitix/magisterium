"""Per-domain fetch strategy persistence (_metadata/fetch-strategy.json)."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Literal

Method = Literal["httpx", "curl", "chrome-mcp"]

REPO_ROOT = Path(__file__).resolve().parents[2]
STRATEGY_PATH = REPO_ROOT / "magisterium" / "_metadata" / "fetch-strategy.json"


def load() -> dict[str, Method]:
    if not STRATEGY_PATH.exists():
        return {}
    return json.loads(STRATEGY_PATH.read_text())


def save(strategy: dict[str, Method]) -> None:
    STRATEGY_PATH.parent.mkdir(parents=True, exist_ok=True)
    STRATEGY_PATH.write_text(json.dumps(strategy, indent=2, sort_keys=True) + "\n")


def get(domain: str) -> Method | None:
    return load().get(domain)


def set_domain(domain: str, method: Method) -> None:
    strategy = load()
    strategy[domain] = method
    save(strategy)
