"""HTML → Markdown conversion via pandoc, preserving paragraph numbering."""
from __future__ import annotations

import subprocess


def html_to_markdown(html: str | bytes) -> str:
    if isinstance(html, bytes):
        html_bytes = html
    else:
        html_bytes = html.encode("utf-8")
    proc = subprocess.run(
        [
            "pandoc",
            "--from=html",
            "--to=gfm-raw_html",
            "--wrap=none",
            "--strip-comments",
        ],
        input=html_bytes,
        capture_output=True,
        check=True,
        timeout=60,
    )
    return proc.stdout.decode("utf-8")
