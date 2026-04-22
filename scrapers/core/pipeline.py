"""Generic document pipeline: DocRef → fetch → markdown → meta.yaml sidecar.

Idempotent: skips documents whose .meta.yaml already exists unless refresh=True.
Runs domains in parallel (rate limit is per-domain).
"""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal

import yaml
from selectolax.parser import HTMLParser

from . import dedup, fetcher, markdown, strategy, translations
from .errors import log_error
from .meta import DocMeta, Source, Traduction

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
    # Provenance de la traduction à écrire dans `traductions[lang]`.
    # "originale" par défaut ; "officielle" pour les langues découvertes
    # automatiquement sur le site source ; "ia" réservé aux traductions
    # produites hors pipeline par le skill translate-corpus.
    kind: Literal["originale", "officielle", "ia"] = "originale"


@dataclass
class PipelineResult:
    n_ok: int = 0
    n_skipped: int = 0
    n_errors: int = 0


def _target_md(ref: DocRef) -> Path:
    return ref.target_dir / f"{ref.slug}.{ref.lang}.md"


def _target_meta(ref: DocRef) -> Path:
    return ref.target_dir / f"{ref.slug}.meta.yaml"


def _detect_charset(html: bytes) -> str:
    """Detect charset from <meta> declaration; fall back to utf-8.

    Legacy Catholic sites (maranatha.it, many 1990s vatican.va pages) are
    served as windows-1252 / iso-8859-1 without a response Content-Type
    header; the only hint is the <meta http-equiv="Content-Type" ...>
    inside the document. Decoding as utf-8 would mangle every accented
    character to U+FFFD.
    """
    import re as _re
    head = html[:4096]
    m = _re.search(
        rb'(?i)charset\s*=\s*["\']?([a-zA-Z0-9_\-]+)',
        head,
    )
    if not m:
        return "utf-8"
    enc = m.group(1).decode("ascii", errors="replace").lower()
    # Normalise a few common aliases.
    if enc in {"iso-8859-1", "latin-1", "latin1"}:
        return "iso-8859-1"
    if enc in {"windows-1252", "cp1252"}:
        return "windows-1252"
    return enc


def _extract_body(
    html: bytes,
    selector: str | None,
    unwrap_tags: list[str] | None = None,
) -> str:
    enc = _detect_charset(html)
    try:
        text = html.decode(enc, errors="replace")
    except LookupError:
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

    # Auto-detect layout tables: on vatican.va (and other legacy sites) the
    # body text is often wrapped in a <table><tr><td>...</td></tr></table>
    # purely for visual centering. Pandoc collapses these to "[TABLE]",
    # losing the entire document. Heuristic: any <table> that contains
    # paragraph tags (<p>) is a layout table, not a data table — real data
    # tables are built from <td>/<th> cells with inline text, not <p>.
    # Add the structural tags to unwrap_tags so they're stripped below,
    # keeping the inner <p> content.
    effective_unwrap = list(unwrap_tags) if unwrap_tags else []
    if "table" not in (t.lower() for t in effective_unwrap):
        layout_table_found = False
        for tbl in root.css("table"):
            if tbl.css_first("p") is not None:
                layout_table_found = True
                break
        if layout_table_found:
            for tag in ("table", "tbody", "thead", "tfoot", "tr", "td", "th"):
                if tag not in (t.lower() for t in effective_unwrap):
                    effective_unwrap.append(tag)

    # Strip site chrome (vatican.va logos, share icons, spacer gifs, dead
    # javascript/mailto/social anchors, empty wrappers left behind). Runs
    # before the tag unwrap pass so the unwrap doesn't leave orphan empty
    # containers that used to wrap chrome elements.
    from . import clean_html as _clean_html
    _clean_html.clean_scraped_html(root)

    # Unwrap requested tags (drop the tag itself, keep its children inline).
    # Useful for layout tables and <font>/<span> cruft on legacy vatican.va
    # pages. Selectolax has no native unwrap; we do it via string replacement
    # on the serialized HTML, which is safe because we only touch tags by name.
    html_out = root.html or ""
    if effective_unwrap:
        import re
        for tag in effective_unwrap:
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
    """Return (status, error). status ∈ {ok, skipped, error}.

    Idempotence : on saute uniquement si le .md de cette langue précise
    existe ET qu'une entrée `traductions[lang]` est déjà présente dans le
    meta.yaml. Un meta.yaml existant sans la langue courante est donc un
    cas valide (ajout d'une traduction officielle sur un doc déjà scrapé).
    """
    meta_path = _target_meta(ref)
    md_path = _target_md(ref)
    if not refresh and meta_path.exists() and md_path.exists():
        try:
            existing_data = yaml.safe_load(meta_path.read_text(encoding="utf-8")) or {}
        except Exception:  # noqa: BLE001
            existing_data = {}
        existing_trads = (existing_data.get("traductions") or {}) if isinstance(existing_data, dict) else {}
        if ref.lang in existing_trads:
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
        domain = fetcher._domain(ref.url)
        now = datetime.now(timezone.utc)
        new_trad = Traduction(
            kind=ref.kind,
            sha256=content_hash,
            source_url=ref.url,
            fetched_at=now,
            fetch_method=result.method,
        )

        if meta_path.exists():
            # Fusion avec l'existant : on n'écrase que les champs qui concernent
            # la nouvelle langue. Le reste (incipit, auteur, sujets…) reste tel quel.
            existing = yaml.safe_load(meta_path.read_text(encoding="utf-8")) or {}
            if not isinstance(existing, dict):
                existing = {}
            sources_list = list(existing.get("sources") or [])
            # Ajouter la source si pas déjà présente pour cette langue
            if not any(
                (isinstance(s, dict) and s.get("langue") == ref.lang and s.get("url") == ref.url)
                for s in sources_list
            ):
                sources_list.append({
                    "url": ref.url, "site": domain,
                    "langue": ref.lang, "fetch_method": result.method,
                })
            existing["sources"] = sources_list
            existing.setdefault("traductions", {})
            existing["traductions"][ref.lang] = new_trad.model_dump(
                mode="json", exclude_none=True,
            )
            meta = DocMeta.model_validate(existing)
        else:
            sources_list = hints.pop("sources", None) or []
            sources_list.append({
                "url": ref.url, "site": domain,
                "langue": ref.lang, "fetch_method": result.method,
            })
            meta = DocMeta(
                incipit=hints.pop("incipit", ref.slug),
                titre_fr=hints.pop("titre_fr", None),
                titre_original=hints.pop("titre_original", None),
                auteur=hints.pop("auteur", "inconnu"),
                periode=hints.pop("periode", "pre-vatican-ii"),
                type=hints.pop("type", "document"),
                date=hints.pop("date", None),
                autorite_magisterielle=hints.pop("autorite_magisterielle", None),
                langue_originale=hints.pop("langue_originale", ref.lang),
                denzinger=hints.pop("denzinger", []),
                sujets=hints.pop("sujets", []),
                themes_doctrinaux=hints.pop("themes_doctrinaux", []),
                references_anterieures=hints.pop("references_anterieures", []),
                references_posterieures=hints.pop("references_posterieures", []),
                sources=sources_list,
                traductions={ref.lang: new_trad},
            )

        meta.sync_legacy_fields()
        meta.write(meta_path)

        # Découverte des traductions officielles : uniquement sur les scrapes
        # d'une originale, et uniquement si on a une page HTML en main.
        # Les DocRefs découverts héritent de tout sauf de l'URL/lang/kind.
        if ref.kind == "originale" and "pdf" not in ctype and not ref.url.lower().endswith(".pdf"):
            try:
                siblings = translations.discover(result.content, ref.url)
            except Exception as exc:  # noqa: BLE001
                siblings = []
                log_error(
                    source=domain, url=ref.url, phase=phase,
                    message=f"discover_translations: {type(exc).__name__}: {exc}",
                    slug=ref.slug,
                )
            for sib_lang, sib_url in siblings:
                sib_ref = DocRef(
                    url=sib_url,
                    target_dir=ref.target_dir,
                    slug=ref.slug,
                    lang=sib_lang,
                    meta_hints={},  # meta.yaml existe déjà, hints non utilisés
                    body_selector=ref.body_selector,
                    unwrap_tags=list(ref.unwrap_tags),
                    kind="officielle",
                )
                # Récursion bornée : kind=officielle → pas de redécouverte.
                await process_one(sib_ref, phase=phase, refresh=refresh)
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
