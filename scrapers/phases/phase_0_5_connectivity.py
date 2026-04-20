"""Phase 0.5 — connectivity test across all sources.

For each source, tries fetch chain (httpx → curl → chrome-mcp) on one witness URL,
persists the winning method to _metadata/fetch-strategy.json, and prints an ASCII
report. Sources run in parallel; rate limit is per-domain (1 req / 2s) so running
parallel across distinct domains is safe.
"""
from __future__ import annotations

import asyncio
import sys
from urllib.parse import urlparse

from scrapers.core import fetcher
from scrapers.sources import WITNESSES


async def probe(label: str, url: str) -> dict:
    try:
        result = await fetcher.fetch(url)
        return {
            "label": label,
            "url": url,
            "method": result.method,
            "status": result.status,
            "latency_ms": result.latency_ms,
            "size_bytes": result.size_bytes,
            "content_type": result.content_type,
            "ok": True,
            "attempts": result.attempts,
        }
    except fetcher.FetchError as e:
        return {
            "label": label,
            "url": url,
            "method": None,
            "status": None,
            "latency_ms": None,
            "size_bytes": 0,
            "content_type": None,
            "ok": False,
            "attempts": e.attempts,
        }
    except Exception as e:  # noqa: BLE001
        return {
            "label": label,
            "url": url,
            "method": None,
            "status": None,
            "latency_ms": None,
            "size_bytes": 0,
            "content_type": None,
            "ok": False,
            "attempts": [{"error": f"{type(e).__name__}: {e}"}],
        }


def _fmt_size(n: int) -> str:
    if n >= 1024 * 1024:
        return f"{n / 1024 / 1024:.1f} MB"
    if n >= 1024:
        return f"{n / 1024:.1f} KB"
    return f"{n} B"


def _render_report(rows: list[dict]) -> str:
    headers = ("source", "witness", "method", "latency", "size", "status")
    source_w = max(len(r["label"]) for r in rows)
    source_w = max(source_w, len(headers[0]))
    witness_w = 40
    rendered_rows = []
    for r in rows:
        witness = urlparse(r["url"]).path[-witness_w:]
        method = r["method"] or "-"
        latency = f"{r['latency_ms']} ms" if r["latency_ms"] is not None else "-"
        size = _fmt_size(r["size_bytes"]) if r["size_bytes"] else "-"
        status = "OK" if r["ok"] else "FAIL"
        rendered_rows.append(
            f"{r['label']:<{source_w}} | {witness:<{witness_w}} | "
            f"{method:<10} | {latency:>8} | {size:>9} | {status}"
        )
    header_line = (
        f"{headers[0]:<{source_w}} | {headers[1]:<{witness_w}} | "
        f"{headers[2]:<10} | {headers[3]:>8} | {headers[4]:>9} | {headers[5]}"
    )
    sep = "-" * len(header_line)
    return "\n".join([header_line, sep, *rendered_rows])


def _render_failures(rows: list[dict]) -> str:
    out: list[str] = []
    for r in rows:
        if r["ok"]:
            continue
        out.append(f"\n>>> {r['label']} — {r['url']}")
        for a in r["attempts"]:
            out.append(f"    - {a}")
    return "\n".join(out)


async def main() -> int:
    tasks = [probe(label, url) for label, (url, _desc) in WITNESSES.items()]
    rows = await asyncio.gather(*tasks)
    report = _render_report(rows)
    print("\nPhase 0.5 — connectivity report\n")
    print(report)
    failures = _render_failures(rows)
    if failures:
        print("\nFailure details:")
        print(failures)
    n_ok = sum(1 for r in rows if r["ok"])
    print(f"\n{n_ok}/{len(rows)} sources reachable")
    return 0 if n_ok == len(rows) else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
