"""Scanner du corpus pour le skill `translate-corpus`.

Produit sur stdout un JSONL de jobs de traduction à réaliser. Chaque ligne :

    {
      "slug": "...",
      "lang_target": "fr",
      "source_lang": "la",
      "source_path": "/absolute/path/to/<slug>.la.md",
      "target_path": "/absolute/path/to/<slug>.fr.md",
      "meta_path": "/absolute/path/to/<slug>.meta.yaml",
      "source_sha256": "...",
      "source_tokens_approx": 1234,
      "needs_chunking": false,
      "reason": "missing" | "source_changed"
    }

Sauté :
  * (slug, lang) où `traductions[lang]` existe avec `kind in {originale, officielle}`.
  * (slug, lang) où `traductions[lang].kind == ia` et `source_sha256` inchangé.

Run : ``uv run python -m tools.translate_scan --langs fr,en [--slug ...] [--source ...] [--limit N] [--dry-run]``.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "magisterium"

# Seuil de découpe en nombre de tokens approximés (~3.5 chars/token en moyenne
# pour latin/fr/en).
CHUNK_THRESHOLD_TOKENS = 25_000

# Priorité de fallback pour choisir une source de traduction quand
# `langue_originale` n'a pas de fichier disponible avec kind originale/officielle.
SOURCE_PRIORITY: list[str] = ["la", "it", "fr", "en", "de", "es"]


def _sha256_text(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _approx_tokens(path: Path) -> int:
    try:
        size = path.stat().st_size
    except OSError:
        return 0
    return max(1, size // 4)  # ~4 bytes/token (approx pour UTF-8 latin alphabet)


def _pick_source_lang(
    meta: dict,
    trads: dict,
    target_lang: str,
) -> str | None:
    """Choisit la langue source optimale pour traduire vers `target_lang`.

    Priorités :
      1. `langue_originale` si son entrée traductions[la] a kind ∈ {originale, officielle}.
      2. Première langue de `SOURCE_PRIORITY` présente avec kind ∈ {originale, officielle}
         et différente de `target_lang`.
    """
    lo = meta.get("langue_originale")
    if lo and lo != target_lang:
        t = trads.get(lo)
        if isinstance(t, dict) and t.get("kind") in {"originale", "officielle"}:
            return lo
    for lang in SOURCE_PRIORITY:
        if lang == target_lang:
            continue
        t = trads.get(lang)
        if isinstance(t, dict) and t.get("kind") in {"originale", "officielle"}:
            return lang
    return None


def scan(
    langs_target: list[str],
    slug_filter: str | None = None,
    source_filter: str | None = None,
    limit: int | None = None,
) -> list[dict]:
    jobs: list[dict] = []
    meta_files = sorted(CORPUS.rglob("*.meta.yaml"))
    for meta_path in meta_files:
        if slug_filter:
            stem = meta_path.name.removesuffix(".meta.yaml")
            if stem != slug_filter:
                continue
        if source_filter:
            rel = meta_path.relative_to(CORPUS).as_posix()
            if not rel.startswith(source_filter.rstrip("/") + "/"):
                continue
        try:
            meta = yaml.safe_load(meta_path.read_text(encoding="utf-8")) or {}
        except Exception:  # noqa: BLE001
            continue
        if not isinstance(meta, dict):
            continue
        trads = meta.get("traductions") or {}
        slug = meta_path.name.removesuffix(".meta.yaml")

        for target in langs_target:
            existing = trads.get(target)
            # Skip si déjà traduit par source faisant autorité
            if isinstance(existing, dict) and existing.get("kind") in {"originale", "officielle"}:
                continue

            source_lang = _pick_source_lang(meta, trads, target)
            if source_lang is None:
                continue
            source_md = meta_path.with_name(f"{slug}.{source_lang}.md")
            if not source_md.exists():
                continue

            source_sha = _sha256_text(source_md)

            # Skip IA déjà à jour
            if (
                isinstance(existing, dict)
                and existing.get("kind") == "ia"
                and existing.get("source_sha256") == source_sha
            ):
                continue

            target_md = meta_path.with_name(f"{slug}.{target}.md")
            tokens = _approx_tokens(source_md)
            reason = "source_changed" if isinstance(existing, dict) and existing.get("kind") == "ia" else "missing"

            jobs.append({
                "slug": slug,
                "lang_target": target,
                "source_lang": source_lang,
                "source_path": str(source_md),
                "target_path": str(target_md),
                "meta_path": str(meta_path),
                "source_sha256": source_sha,
                "source_tokens_approx": tokens,
                "needs_chunking": tokens > CHUNK_THRESHOLD_TOKENS,
                "reason": reason,
            })

        if limit is not None and len(jobs) >= limit:
            break
    return jobs


def main() -> int:
    p = argparse.ArgumentParser(description="Scanner : liste les jobs de traduction IA à réaliser.")
    p.add_argument("--langs", default="fr,en", help="Langues cibles, séparées par virgule (défaut : fr,en).")
    p.add_argument("--slug", default=None, help="Ne traiter qu'un seul slug.")
    p.add_argument("--source", default=None, help="Restreindre à un sous-chemin sous magisterium/.")
    p.add_argument("--limit", type=int, default=None, help="Cap sur le nombre de jobs.")
    p.add_argument("--dry-run", action="store_true", help="Mode humain : stats sommaires au lieu du JSONL.")
    args = p.parse_args()

    langs = [x.strip() for x in args.langs.split(",") if x.strip()]
    jobs = scan(langs, slug_filter=args.slug, source_filter=args.source, limit=args.limit)

    if args.dry_run:
        by_lang: dict[str, int] = {}
        by_lang_chunk: dict[str, int] = {}
        for j in jobs:
            by_lang[j["lang_target"]] = by_lang.get(j["lang_target"], 0) + 1
            if j["needs_chunking"]:
                by_lang_chunk[j["lang_target"]] = by_lang_chunk.get(j["lang_target"], 0) + 1
        print(f"[translate_scan] {len(jobs)} jobs à faire", file=sys.stderr)
        for lang in sorted(by_lang):
            print(f"  {lang}: {by_lang[lang]} docs ({by_lang_chunk.get(lang, 0)} à découper)", file=sys.stderr)
    else:
        for j in jobs:
            sys.stdout.write(json.dumps(j, ensure_ascii=False) + "\n")

    return 0


if __name__ == "__main__":
    sys.exit(main())
