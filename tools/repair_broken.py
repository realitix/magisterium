"""Re-scrape documents whose .md sidecar is empty or contains only "[TABLE]".

Such files slipped through the first scraping pass because the vatican.va
layout wraps the body in a <table> that pandoc collapses to "[TABLE]".
The pipeline now auto-detects layout tables, so re-running the fetch fixes
these documents in place.

This tool:
  1. Scans magisterium/ for every .meta.yaml whose companion .md looks
     broken (see `_is_broken`).
  2. Rebuilds a DocRef from the meta (URL + slug + language + folder).
  3. Runs it through `process_one(..., refresh=True)`.
  4. Re-checks and reports which files are still broken.

Usage:
    uv run python -m tools.repair_broken [--dry-run] [--limit N] [--glob PATTERN]
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from dataclasses import dataclass
from pathlib import Path

import yaml

from scrapers.core.pipeline import DocRef, process_one

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "magisterium"

# Any file whose content is just "[TABLE]" lines (plus whitespace and maybe a
# BOM and a title) is considered broken. We also flag tiny files (<800 B) that
# pandoc may have emitted as navigation-only output.
BROKEN_MARKERS = {"[TABLE]"}


@dataclass
class Candidate:
    meta_path: Path
    md_path: Path
    url: str
    lang: str
    size: int
    reason: str


def _is_broken(md_path: Path) -> tuple[bool, str]:
    if not md_path.exists():
        return False, ""
    try:
        text = md_path.read_text(encoding="utf-8")
    except OSError as e:
        return False, f"read error: {e}"
    size = len(text.encode("utf-8"))

    # PDF sidecars are legit: "[PDF file: ...]"
    if text.lstrip().startswith("[PDF file:"):
        return False, ""

    # Strip BOM and whitespace for the content check
    stripped = text.replace("﻿", "").strip()
    # Drop all lines that are just "[TABLE]" or navigation links; if nothing
    # substantial remains, the file is broken.
    if not stripped:
        return True, "empty"
    non_table_lines = [
        ln for ln in stripped.splitlines()
        if ln.strip() and ln.strip() not in BROKEN_MARKERS
    ]
    joined = "\n".join(non_table_lines).strip()
    if not joined:
        return True, "only [TABLE]"
    # If the file contains "[TABLE]" AND most of the remaining content is
    # navigation (markdown links) or boilerplate, treat as broken. This
    # catches newer vatican.va content-template pages where the body was
    # replaced by "[TABLE]" but the language-switcher and footer survived.
    has_table_marker = "[TABLE]" in text
    if has_table_marker:
        # "substantial" lines = not a link, not a heading, not image, not copyright
        def _is_nav(ln: str) -> bool:
            s = ln.strip()
            if not s:
                return True
            if s.startswith("#"):  # headings like "## Sancta Sedes"
                return True
            if s.startswith("!["):  # image
                return True
            if s.lower().startswith("copyright"):
                return True
            # mostly-link lines
            link_chars = s.count("](")
            if link_chars >= 1:
                # Strip markdown links and images; if what remains is
                # negligible, the line is pure navigation/chrome.
                import re as _re
                stripped_s = _re.sub(r"!?\[[^\]]*\]\([^)]*\)", "", s).strip(" -|\t ")
                if len(stripped_s) < 20:
                    return True
            return False

        substantial = [ln for ln in non_table_lines if not _is_nav(ln)]
        substantial_text = " ".join(substantial).strip()
        # A real body will have at least one paragraph of several hundred chars.
        if len(substantial_text) < 400:
            return True, f"[TABLE]+nav ({size}B, substantial={len(substantial_text)})"
    return False, ""


def _find_md_for_meta(meta_path: Path, langues: list[str], lang_orig: str | None) -> Path | None:
    stem = meta_path.name.removesuffix(".meta.yaml")
    parent = meta_path.parent
    # Prefer langue_originale, then langues_disponibles, then plain stem.md
    preferred: list[str] = []
    if lang_orig:
        preferred.append(lang_orig)
    for l in langues or []:
        if l not in preferred:
            preferred.append(l)
    for l in preferred:
        cand = parent / f"{stem}.{l}.md"
        if cand.exists():
            return cand
    legacy = parent / f"{stem}.md"
    if legacy.exists():
        return legacy
    return None


def _scan(glob_pat: str | None = None) -> list[Candidate]:
    candidates: list[Candidate] = []
    for meta_path in sorted(CORPUS.rglob("*.meta.yaml")):
        if glob_pat and glob_pat not in str(meta_path):
            continue
        try:
            data = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict):
            continue
        langues = data.get("langues_disponibles") or []
        lang_orig = data.get("langue_originale")
        md_path = _find_md_for_meta(meta_path, langues, lang_orig)
        if md_path is None:
            continue
        broken, reason = _is_broken(md_path)
        if not broken:
            continue
        # Pick the source that matches the md's lang suffix
        md_lang = md_path.stem.split(".")[-1] if "." in md_path.stem else lang_orig
        sources = data.get("sources") or []
        url = None
        for s in sources:
            if s.get("langue") == md_lang:
                url = s.get("url")
                break
        if url is None and sources:
            url = sources[0].get("url")
        if url is None:
            continue
        candidates.append(
            Candidate(
                meta_path=meta_path,
                md_path=md_path,
                url=url,
                lang=md_lang or lang_orig or "unknown",
                size=md_path.stat().st_size,
                reason=reason,
            )
        )
    return candidates


def _build_ref_from_meta(c: Candidate) -> DocRef:
    data = yaml.safe_load(c.meta_path.read_text(encoding="utf-8"))
    stem = c.meta_path.name.removesuffix(".meta.yaml")
    hints: dict = {
        "incipit": data.get("incipit") or stem,
        "titre_fr": data.get("titre_fr"),
        "auteur": data.get("auteur", "inconnu"),
        "periode": data.get("periode", "post-vatican-ii"),
        "type": data.get("type", "document"),
        "date": data.get("date"),
        "autorite_magisterielle": data.get("autorite_magisterielle"),
        "langues_disponibles": data.get("langues_disponibles") or [c.lang],
        "langue_originale": data.get("langue_originale") or c.lang,
        "denzinger": data.get("denzinger") or [],
        "sujets": data.get("sujets") or [],
        "themes_doctrinaux": data.get("themes_doctrinaux") or [],
        "references_anterieures": data.get("references_anterieures") or [],
        "references_posterieures": data.get("references_posterieures") or [],
    }
    return DocRef(
        url=c.url,
        target_dir=c.meta_path.parent,
        slug=stem,
        lang=c.lang,
        meta_hints=hints,
    )


async def _repair_all(candidates: list[Candidate], concurrency: int = 4) -> list[tuple[Candidate, str, str | None]]:
    sem = asyncio.Semaphore(concurrency)
    results: list[tuple[Candidate, str, str | None]] = []

    async def worker(c: Candidate) -> None:
        ref = _build_ref_from_meta(c)
        async with sem:
            status, err = await process_one(ref, phase="repair-broken", refresh=True)
        results.append((c, status, err))

    await asyncio.gather(*(worker(c) for c in candidates))
    return results


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true", help="list candidates without re-fetching")
    p.add_argument("--limit", type=int, default=None, help="process at most N files")
    p.add_argument("--glob", type=str, default=None, help="only consider meta paths containing this substring")
    p.add_argument("--concurrency", type=int, default=4)
    args = p.parse_args()

    candidates = _scan(glob_pat=args.glob)
    print(f"Found {len(candidates)} broken .md sidecar(s).")
    if args.limit is not None:
        candidates = candidates[: args.limit]
        print(f"Limiting to first {len(candidates)}.")

    if args.dry_run:
        for c in candidates:
            rel = c.md_path.relative_to(CORPUS)
            print(f"  {c.size:6d}B  {c.reason:20s}  {rel}  <- {c.url}")
        return 0

    results = asyncio.run(_repair_all(candidates, concurrency=args.concurrency))
    ok = sum(1 for _, s, _ in results if s == "ok")
    err = sum(1 for _, s, _ in results if s == "error")
    skipped = sum(1 for _, s, _ in results if s == "skipped")
    print(f"\nRepair complete: ok={ok} error={err} skipped={skipped}")

    # Post-check: still broken?
    still_broken: list[Candidate] = []
    for c, status, error in results:
        if status == "ok":
            broken_after, reason = _is_broken(c.md_path)
            if broken_after:
                still_broken.append(c)
                print(f"  STILL BROKEN ({reason}): {c.md_path.relative_to(CORPUS)}")
        elif status == "error":
            print(f"  FETCH ERROR ({error}): {c.md_path.relative_to(CORPUS)}")

    print(f"\nPost-check: {len(still_broken)} still broken.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
