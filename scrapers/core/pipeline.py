"""Generic document pipeline: DocRef → fetch → markdown → meta.yaml sidecar.

Idempotent: skips documents whose .meta.yaml already exists unless refresh=True.
Runs domains in parallel (rate limit is per-domain).
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from selectolax.parser import HTMLParser

from . import dedup, fetcher, markdown, strategy
from .errors import log_error
from .meta import DocMeta, Source

REPO_ROOT = Path(__file__).resolve().parents[2]
MAGISTERIUM_ROOT = REPO_ROOT / "magisterium"

# Domain-level body selectors — applied when DocRef has no explicit selector.
# Verified by manual inspection of each site's layout.
SITE_SELECTORS: dict[str, str] = {
    "www.papalencyclicals.net": "div.entry-content",
    "www.vatican.va": "div.documento, div.testo, #testoen, #testo",
    "www.ewtn.com": "main, article, div.body",
    "laportelatine.org": "article, main, div.entry-content",
    "www.salve-regina.com": "#mw-content-text",
    "vatican2voice.org": "#content, body",
    "www.clerus.va": "main, article, body",
    "www.documentacatholicaomnia.eu": "body",
}


@dataclass
class DocRef:
    """One document to fetch and archive."""
    url: str
    target_dir: Path              # absolute path under magisterium/
    slug: str                     # YYYY-MM-DD_incipit_type (no lang suffix)
    lang: str                     # e.g. "la", "fr", "en", "it"
    meta_hints: dict[str, Any] = field(default_factory=dict)
    body_selector: str | None = None  # CSS selector for main content, if needed
    # Tags to "unwrap" (drop tag, keep children) after body extraction, before
    # pandoc. Useful for layout tables (e.g. vatican.va hist_councils pages,
    # where the real body is wrapped in <table>/<td> that pandoc collapses to
    # "[TABLE]").
    unwrap_tags: list[str] = field(default_factory=list)


@dataclass
class PipelineResult:
    n_ok: int = 0
    n_skipped: int = 0
    n_errors: int = 0


def _target_md(ref: DocRef) -> Path:
    return ref.target_dir / f"{ref.slug}.{ref.lang}.md"


def _target_meta(ref: DocRef) -> Path:
    return ref.target_dir / f"{ref.slug}.meta.yaml"


def _extract_body(
    html: bytes,
    selector: str | None,
    unwrap_tags: list[str] | None = None,
) -> str:
    text = html.decode("utf-8", errors="replace")
    tree = HTMLParser(text)
    # Strip script/style/nav chrome in all cases
    for bad in tree.css("script, style, noscript, nav, header, footer, aside"):
        bad.decompose()

    # Pick root: matched selector > <body> > whole doc
    root = None
    if selector:
        for sel in (s.strip() for s in selector.split(",") if s.strip()):
            node = tree.css_first(sel)
            if node is not None:
                root = node
                break
    if root is None:
        root = tree.css_first("body") or tree.root

    # Unwrap requested tags (drop the tag itself, keep its children inline).
    # Useful for layout tables and <font>/<span> cruft on legacy vatican.va
    # pages. Selectolax has no native unwrap; we do it via string replacement
    # on the serialized HTML, which is safe because we only touch tags by name.
    html_out = root.html or ""
    if unwrap_tags:
        import re
        for tag in unwrap_tags:
            # Remove opening <tag ...> and closing </tag>
            html_out = re.sub(
                rf"<{tag}(\s[^>]*)?>",
                "",
                html_out,
                flags=re.IGNORECASE,
            )
            html_out = re.sub(
                rf"</{tag}\s*>",
                "",
                html_out,
                flags=re.IGNORECASE,
            )
    return html_out


async def process_one(
    ref: DocRef,
    phase: str,
    refresh: bool = False,
) -> tuple[str, str | None]:
    """Return (status, error). status ∈ {ok, skipped, error}."""
    meta_path = _target_meta(ref)
    md_path = _target_md(ref)
    if not refresh and meta_path.exists() and md_path.exists():
        return ("skipped", None)

    try:
        result = await fetcher.fetch(ref.url)
    except fetcher.FetchError as e:
        log_error(
            source=fetcher._domain(ref.url),
            url=ref.url,
            phase=phase,
            message="fetch failed (all methods)",
            attempts=e.attempts,
            slug=ref.slug,
        )
        return ("error", "fetch failed")
    except Exception as e:  # noqa: BLE001
        log_error(
            source=fetcher._domain(ref.url),
            url=ref.url,
            phase=phase,
            message=f"fetch unexpected: {type(e).__name__}: {e}",
            slug=ref.slug,
        )
        return ("error", f"fetch unexpected: {e}")

    try:
        ctype = (result.content_type or "").lower()
        if "pdf" in ctype or ref.url.lower().endswith(".pdf"):
            # Store PDF as-is + minimal meta; pandoc can't do PDF→MD well.
            ref.target_dir.mkdir(parents=True, exist_ok=True)
            pdf_path = ref.target_dir / f"{ref.slug}.{ref.lang}.pdf"
            pdf_path.write_bytes(result.content)
            body_md = f"[PDF file: {pdf_path.name}]\n"
            md_path.write_text(body_md, encoding="utf-8")
            content_hash = dedup.sha256_text(result.content.decode("latin-1", errors="replace"))
        else:
            selector = ref.body_selector or SITE_SELECTORS.get(fetcher._domain(ref.url))
            body_html = _extract_body(result.content, selector, ref.unwrap_tags)
            body_md = markdown.html_to_markdown(body_html)
            ref.target_dir.mkdir(parents=True, exist_ok=True)
            md_path.write_text(body_md, encoding="utf-8")
            content_hash = dedup.sha256_text(body_md)

        hints = dict(ref.meta_hints)
        sources_list = hints.pop("sources", None)
        source_entry = Source(
            url=ref.url,
            site=fetcher._domain(ref.url),
            langue=ref.lang,
            fetch_method=result.method,
        )
        if sources_list:
            sources_list.append(source_entry.model_dump())
        else:
            sources_list = [source_entry.model_dump()]

        meta = DocMeta(
            incipit=hints.pop("incipit", ref.slug),
            titre_fr=hints.pop("titre_fr", None),
            auteur=hints.pop("auteur", "inconnu"),
            periode=hints.pop("periode", "pre-vatican-ii"),
            type=hints.pop("type", "document"),
            date=hints.pop("date", None),
            autorite_magisterielle=hints.pop("autorite_magisterielle", None),
            langues_disponibles=hints.pop("langues_disponibles", [ref.lang]),
            langue_originale=hints.pop("langue_originale", ref.lang),
            denzinger=hints.pop("denzinger", []),
            sujets=hints.pop("sujets", []),
            themes_doctrinaux=hints.pop("themes_doctrinaux", []),
            references_anterieures=hints.pop("references_anterieures", []),
            references_posterieures=hints.pop("references_posterieures", []),
            sources=sources_list,
            sha256={ref.lang: content_hash},
        )
        meta.write(meta_path)
        return ("ok", None)
    except Exception as e:  # noqa: BLE001
        log_error(
            source=fetcher._domain(ref.url),
            url=ref.url,
            phase=phase,
            message=f"post-fetch: {type(e).__name__}: {e}",
            slug=ref.slug,
        )
        return ("error", f"post-fetch: {e}")


async def run_pipeline(
    refs: list[DocRef],
    phase: str,
    refresh: bool = False,
    concurrency: int = 16,
) -> PipelineResult:
    """Run pipeline over DocRefs, with global concurrency cap.

    Rate limiting is per-domain (GLOBAL_LIMITER); this semaphore bounds total
    in-flight jobs so we don't blow memory on huge phases.
    """
    sem = asyncio.Semaphore(concurrency)
    result = PipelineResult()
    result_lock = asyncio.Lock()

    async def worker(ref: DocRef) -> None:
        async with sem:
            status, _ = await process_one(ref, phase=phase, refresh=refresh)
            async with result_lock:
                if status == "ok":
                    result.n_ok += 1
                elif status == "skipped":
                    result.n_skipped += 1
                else:
                    result.n_errors += 1

    await asyncio.gather(*(worker(r) for r in refs))
    return result


__all__ = ["DocRef", "PipelineResult", "run_pipeline", "process_one", "MAGISTERIUM_ROOT"]
