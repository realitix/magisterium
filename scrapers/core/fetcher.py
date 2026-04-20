"""Fetch chain: httpx → curl → chrome-mcp, with per-domain strategy persistence."""
from __future__ import annotations

import asyncio
import subprocess
import tempfile
import time
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import urlparse

import httpx

from . import strategy
from .rate_limit import GLOBAL_LIMITER

USER_AGENT = (
    "Mozilla/5.0 (compatible; MagisteriumArchiver/1.0; "
    "+https://github.com/realitix/catholique)"
)
HTTP_TIMEOUT = 60.0
CURL_TIMEOUT = 75
RETRIES_PER_METHOD = 3

# Domains with legit-but-broken TLS (expired/mismatched certs) that we trust
# because they serve public-domain Catholic texts. Verified manually.
INSECURE_DOMAINS: set[str] = {
    "www.salve-regina.com",
    "salve-regina.com",
}


@dataclass
class FetchResult:
    url: str
    method: strategy.Method
    status: int
    latency_ms: int
    size_bytes: int
    content: bytes
    content_type: str | None
    attempts: list[dict]  # diagnostic trace


@dataclass
class FetchError(Exception):
    url: str
    attempts: list[dict]

    def __str__(self) -> str:  # pragma: no cover
        return f"all methods failed for {self.url}: {self.attempts}"


def _domain(url: str) -> str:
    return urlparse(url).netloc.lower()


async def _try_httpx(url: str) -> FetchResult:
    t0 = time.monotonic()
    verify = _domain(url) not in INSECURE_DOMAINS
    async with httpx.AsyncClient(
        http2=True,
        follow_redirects=True,
        timeout=HTTP_TIMEOUT,
        verify=verify,
        headers={"User-Agent": USER_AGENT, "Accept-Language": "la,fr,en,it;q=0.8"},
    ) as client:
        r = await client.get(url)
    if r.status_code >= 400:
        raise RuntimeError(f"httpx status {r.status_code}")
    latency_ms = int((time.monotonic() - t0) * 1000)
    return FetchResult(
        url=url,
        method="httpx",
        status=r.status_code,
        latency_ms=latency_ms,
        size_bytes=len(r.content),
        content=r.content,
        content_type=r.headers.get("content-type"),
        attempts=[],
    )


async def _try_curl(url: str) -> FetchResult:
    t0 = time.monotonic()
    with tempfile.NamedTemporaryFile(delete=False, suffix=".bin") as tmp:
        tmp_path = Path(tmp.name)
    try:
        curl_args = [
            "curl", "-L", "--compressed",
            "-A", USER_AGENT,
            "--retry", "3", "--retry-delay", "2",
            "--max-time", str(CURL_TIMEOUT),
            "-sS", "-w", "%{http_code} %{content_type}",
            "-o", str(tmp_path),
        ]
        if _domain(url) in INSECURE_DOMAINS:
            curl_args.append("-k")
        curl_args.append(url)
        proc = await asyncio.to_thread(
            subprocess.run,
            curl_args,
            capture_output=True,
            text=True,
            timeout=CURL_TIMEOUT + 30,
        )
        if proc.returncode != 0:
            raise RuntimeError(f"curl rc={proc.returncode}: {proc.stderr.strip()[:200]}")
        stdout_parts = proc.stdout.strip().split(" ", 1)
        http_code = int(stdout_parts[0])
        ctype = stdout_parts[1] if len(stdout_parts) > 1 else None
        if http_code >= 400:
            raise RuntimeError(f"curl http {http_code}")
        content = tmp_path.read_bytes()
    finally:
        tmp_path.unlink(missing_ok=True)
    latency_ms = int((time.monotonic() - t0) * 1000)
    return FetchResult(
        url=url,
        method="curl",
        status=http_code,
        latency_ms=latency_ms,
        size_bytes=len(content),
        content=content,
        content_type=ctype,
        attempts=[],
    )


async def _try_chrome_mcp(url: str) -> FetchResult:
    # Chrome MCP integration is deferred: a human-assisted fallback.
    # When a site blocks both httpx and curl, the scraper logs it and
    # produces a to-do list for chrome-mcp capture in a second pass.
    raise RuntimeError("chrome-mcp fallback not yet wired (deferred to pass 2)")


METHOD_FUNCS = {
    "httpx": _try_httpx,
    "curl": _try_curl,
    "chrome-mcp": _try_chrome_mcp,
}
DEFAULT_CHAIN: list[strategy.Method] = ["httpx", "curl", "chrome-mcp"]


async def fetch(url: str, *, prefer: strategy.Method | None = None) -> FetchResult:
    domain = _domain(url)
    await GLOBAL_LIMITER.acquire(domain)

    # Choose method order: persisted-strategy first, else DEFAULT_CHAIN.
    persisted = strategy.get(domain)
    chain: list[strategy.Method]
    if prefer:
        chain = [prefer] + [m for m in DEFAULT_CHAIN if m != prefer]
    elif persisted:
        chain = [persisted] + [m for m in DEFAULT_CHAIN if m != persisted]
    else:
        chain = list(DEFAULT_CHAIN)

    attempts: list[dict] = []
    for method in chain:
        func = METHOD_FUNCS[method]
        last_err: str | None = None
        for attempt_idx in range(RETRIES_PER_METHOD):
            try:
                result = await func(url)
                result.attempts = attempts
                if strategy.get(domain) != method:
                    strategy.set_domain(domain, method)
                return result
            except Exception as e:  # noqa: BLE001
                last_err = f"{type(e).__name__}: {e}"
                attempts.append({
                    "method": method,
                    "attempt": attempt_idx + 1,
                    "error": last_err,
                })
                # small backoff between retries of same method
                await asyncio.sleep(1.5 * (attempt_idx + 1))
        # escalate to next method

    raise FetchError(url=url, attempts=attempts)
