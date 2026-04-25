"""Validate every .meta.yaml in the corpus.

Checks:
  - YAML parses
  - Passes DocMeta (pydantic)
  - Companion .md exists and is non-empty, OR a .MISSING.md sibling exists
  - sha256 dict is present and non-empty

Run: ``uv run python -m tools.validate``.
"""
from __future__ import annotations

import sys
from pathlib import Path

import yaml
from pydantic import ValidationError

from scrapers.core.meta import DocMeta

ROOT = Path(__file__).resolve().parents[1]
# Deux racines à valider : le corpus magistériel et la collection de livres
# non-magistériels. Voir CLAUDE.md.
CORPUS_ROOTS: tuple[Path, ...] = (
    ROOT / "magisterium",
    ROOT / "livres",
)


def _companion_ok(meta_path: Path, langs: list[str]) -> tuple[bool, str]:
    """Return (ok, reason) for the companion markdown file(s).

    Markdown sidecars use the ``<stem>.<lang>.md`` naming convention (one per
    available language), with an optional ``.MISSING.md`` sentinel.
    """
    stem = meta_path.name.removesuffix(".meta.yaml")
    parent = meta_path.parent
    missing = parent / f"{stem}.MISSING.md"
    if missing.exists():
        return True, "missing-md sentinel"

    # Prefer per-language files; fall back to plain <stem>.md for
    # legacy/simple documents.
    candidates: list[Path] = []
    for lang in langs or []:
        candidates.append(parent / f"{stem}.{lang}.md")
    if not candidates:
        candidates.append(parent / f"{stem}.md")
    else:
        candidates.append(parent / f"{stem}.md")

    existing = [c for c in candidates if c.exists()]
    if not existing:
        return False, f"no .md found (tried {', '.join(c.name for c in candidates)})"
    for md in existing:
        try:
            if md.stat().st_size == 0:
                return False, f"{md.name} empty"
        except OSError as exc:
            return False, f"stat error on {md.name}: {exc}"
    return True, "ok"


def main() -> int:
    meta_files: list[Path] = []
    for corpus in CORPUS_ROOTS:
        if corpus.exists():
            meta_files.extend(sorted(corpus.rglob("*.meta.yaml")))
    yaml_errors: list[tuple[Path, str]] = []
    pydantic_errors: list[tuple[Path, str]] = []
    md_errors: list[tuple[Path, str]] = []
    sha_errors: list[tuple[Path, str]] = []
    ok = 0

    for p in meta_files:
        try:
            data = yaml.safe_load(p.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            yaml_errors.append((p, str(exc)))
            continue
        if not isinstance(data, dict):
            yaml_errors.append((p, "top-level is not a mapping"))
            continue

        try:
            DocMeta.model_validate(data)
        except ValidationError as exc:
            pydantic_errors.append((p, exc.errors(include_url=False)[0]["msg"] if exc.errors() else str(exc)))
            continue

        sha = data.get("sha256") or {}
        if not sha:
            sha_errors.append((p, "empty sha256"))

        md_ok, reason = _companion_ok(p, data.get("langues_disponibles") or [])
        if not md_ok:
            md_errors.append((p, reason))

        if md_ok and sha:
            ok += 1

    print(f"Validation report — {len(meta_files)} .meta.yaml files")
    print(f"  OK              : {ok}")
    print(f"  YAML errors     : {len(yaml_errors)}")
    print(f"  Pydantic errors : {len(pydantic_errors)}")
    print(f"  sha256 missing  : {len(sha_errors)}")
    print(f"  .md anomalies   : {len(md_errors)}")

    def _dump(label: str, rows: list[tuple[Path, str]], limit: int = 25) -> None:
        if not rows:
            return
        print(f"\n-- {label} (showing {min(limit, len(rows))}/{len(rows)}) --")
        for p, reason in rows[:limit]:
            print(f"  {p.relative_to(ROOT)}: {reason}")
        if len(rows) > limit:
            print(f"  ... ({len(rows) - limit} more)")

    _dump("YAML errors", yaml_errors)
    _dump("Pydantic errors", pydantic_errors)
    _dump("sha256 missing", sha_errors)
    _dump(".md anomalies", md_errors)

    return 0 if not (yaml_errors or pydantic_errors) else 1


if __name__ == "__main__":
    sys.exit(main())
