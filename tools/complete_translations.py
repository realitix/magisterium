"""Parcours du corpus existant : pour chaque document, refetch l'URL de
l'originale (si le site la supporte), détecte les traductions officielles
siblings, et scrape celles qui manquent en tant que kind='officielle'.

Ne touche jamais aux fichiers existants : seul `traductions[<nouvelle_lang>]`
et les nouveaux `.lang.md` sont créés. Les champs historiques
`langues_disponibles` / `sha256` sont resynchronisés.

Idempotent, reprenable, rate-limité par domaine via le fetcher standard.

Run : ``uv run python -m tools.complete_translations [--limit N] [--domain www.vatican.va] [--slug <slug>]``.
"""
from __future__ import annotations

import argparse
import asyncio
import sys
from pathlib import Path

import yaml

from scrapers.core import fetcher, translations
from scrapers.core.pipeline import DocRef, process_one, SITE_SELECTORS

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "magisterium"


async def complete_one(meta_path: Path, phase: str) -> tuple[str, int, str | None]:
    """Return (status, n_added, note).

    status ∈ {ok, skip, error}. n_added = nombre de traductions officielles
    ajoutées à ce doc.
    """
    try:
        data = yaml.safe_load(meta_path.read_text(encoding="utf-8")) or {}
    except Exception as exc:  # noqa: BLE001
        return ("error", 0, f"yaml: {exc}")
    if not isinstance(data, dict):
        return ("error", 0, "not a dict")

    existing_trads: dict = data.get("traductions") or {}
    langue_originale = data.get("langue_originale")
    # Trouver la source de l'originale dans `sources[]` (priorité) ou
    # accepter n'importe quelle source dont le site est supporté par la
    # découverte (vatican.va).
    sources = data.get("sources") or []
    origin_source = None
    for s in sources:
        if isinstance(s, dict) and s.get("langue") == langue_originale:
            if s.get("site") == "www.vatican.va":
                origin_source = s
                break
    if origin_source is None:
        # Fallback : première source vatican.va trouvée (cas où langue_originale
        # n'est pas exactement celle de la source scrapée).
        for s in sources:
            if isinstance(s, dict) and s.get("site") == "www.vatican.va":
                origin_source = s
                break
    if origin_source is None:
        return ("skip", 0, "no vatican.va source")

    origin_url = origin_source.get("url")
    if not origin_url:
        return ("skip", 0, "source has no url")

    # Refetch de l'originale pour extraire les liens de traduction
    try:
        result = await fetcher.fetch(origin_url)
    except Exception as exc:  # noqa: BLE001
        return ("error", 0, f"refetch: {exc}")

    siblings = translations.discover(result.content, origin_url)
    # Ne scraper que les langues qu'on n'a pas déjà
    missing = [(lang, url) for lang, url in siblings if lang not in existing_trads]
    if not missing:
        return ("skip", 0, "already complete")

    slug = meta_path.name.removesuffix(".meta.yaml")
    target_dir = meta_path.parent

    added = 0
    for lang, url in missing:
        sib_ref = DocRef(
            url=url,
            target_dir=target_dir,
            slug=slug,
            lang=lang,
            meta_hints={},
            body_selector=None,  # SITE_SELECTORS via le domain lookup
            unwrap_tags=[],
            kind="officielle",
        )
        status, err = await process_one(sib_ref, phase=phase, refresh=False)
        if status == "ok":
            added += 1
        elif status == "error":
            # On ne s'arrête pas, une langue qui échoue ne bloque pas les autres
            pass

    return ("ok", added, f"{added}/{len(missing)} added")


async def run(meta_files: list[Path], concurrency: int) -> dict[str, int]:
    sem = asyncio.Semaphore(concurrency)
    counters = {"ok": 0, "skip": 0, "error": 0, "added": 0}
    lock = asyncio.Lock()

    async def worker(meta_path: Path) -> None:
        async with sem:
            status, n, note = await complete_one(meta_path, phase="complete-translations")
            async with lock:
                counters[status] += 1
                counters["added"] += n
                # Log compact
                rel = meta_path.relative_to(CORPUS)
                if status == "ok" and n > 0:
                    print(f"  [+{n:2d}] {rel}")
                elif status == "error":
                    print(f"  [ERR] {rel}: {note}")

    await asyncio.gather(*(worker(p) for p in meta_files))
    return counters


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=None, help="Ne traiter que les N premiers docs.")
    p.add_argument("--slug", default=None, help="Ne traiter que ce slug précis (debug).")
    p.add_argument("--concurrency", type=int, default=8)
    args = p.parse_args()

    meta_files = sorted(CORPUS.rglob("*.meta.yaml"))
    if args.slug:
        meta_files = [m for m in meta_files if m.stem.removesuffix(".meta") == args.slug]
    else:
        # Pre-filter : on ne garde que les docs qui ont au moins une source
        # vatican.va — seul domaine avec découverte supportée pour l'instant.
        filtered: list[Path] = []
        for m in meta_files:
            try:
                data = yaml.safe_load(m.read_text(encoding="utf-8")) or {}
            except Exception:  # noqa: BLE001
                continue
            if not isinstance(data, dict):
                continue
            if any(
                isinstance(s, dict) and s.get("site") == "www.vatican.va"
                for s in (data.get("sources") or [])
            ):
                filtered.append(m)
        meta_files = filtered
    if args.limit:
        meta_files = meta_files[: args.limit]

    print(f"[complete_translations] scanning {len(meta_files)} docs…")
    counters = asyncio.run(run(meta_files, concurrency=args.concurrency))
    print()
    print(f"  ok     : {counters['ok']}")
    print(f"  skip   : {counters['skip']}")
    print(f"  errors : {counters['error']}")
    print(f"  translations added : {counters['added']}")
    return 0 if counters["error"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
