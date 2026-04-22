"""Migration one-shot : reconstruit le bloc `traductions` dans chaque
.meta.yaml à partir des fichiers .lang.md présents sur le disque.

Règles :
  * Pour chaque .meta.yaml, on scanne les fichiers frères `<stem>.<lang>.md`.
  * Pour chaque langue trouvée, on remplit `traductions[lang]` avec :
      - kind = "originale" si lang == langue_originale
      - kind = "originale" aussi dans les autres cas (tous les docs actuels
        ont été scrapés comme "langue source uniquement") — cette migration
        n'essaie pas de deviner des "officielles" parmi les fichiers déjà
        présents. Si un jour on constate des fichiers multi-langues préexistants
        qui devraient être des "officielles", on ré-exécutera la phase 2 avec
        la découverte de traductions activée.
      - sha256 = recalculé depuis le contenu du .md (source de vérité).
      - source_url / fetch_method = repris de `sources[]` si disponible.
  * Les champs historiques `langues_disponibles` et `sha256` sont reconstruits
    depuis `traductions` par `sync_legacy_fields()`.

Idempotent : si `traductions` existe déjà et couvre toutes les langues
trouvées sur disque, on ne réécrit pas le fichier (sauf --force).

Run : ``uv run python -m tools.migrate_to_traductions [--dry-run] [--force]``.
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from pathlib import Path

import yaml

from scrapers.core.meta import DocMeta, Traduction

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "magisterium"


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def _find_lang_files(meta_path: Path) -> dict[str, Path]:
    """Retourne {lang: chemin} pour tous les `.lang.md` frères du meta."""
    stem = meta_path.name.removesuffix(".meta.yaml")
    parent = meta_path.parent
    out: dict[str, Path] = {}
    for md in parent.glob(f"{stem}.*.md"):
        # Extract "lang" from "<stem>.<lang>.md"
        name = md.name[len(stem) + 1 :]  # strip "<stem>."
        if name.endswith(".md"):
            lang = name[:-3]
            # Skip sentinel files like "MISSING"
            if lang != "MISSING" and len(lang) <= 5:
                out[lang] = md
    return out


def migrate_one(meta_path: Path, *, force: bool, dry_run: bool) -> tuple[str, str | None]:
    """Return (status, note). status ∈ {migrated, skipped, error}."""
    try:
        data = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
    except Exception as exc:  # noqa: BLE001
        return ("error", f"yaml: {exc}")
    if not isinstance(data, dict):
        return ("error", "not a mapping")

    langue_originale = data.get("langue_originale")
    existing_traductions = data.get("traductions") or {}
    lang_files = _find_lang_files(meta_path)

    if not lang_files:
        return ("error", "no .lang.md sibling found")

    # Skip si `traductions` couvre déjà toutes les langues présentes sur disque.
    if not force and existing_traductions:
        have = set(existing_traductions.keys())
        need = set(lang_files.keys())
        if need.issubset(have):
            return ("skipped", f"traductions already covers {sorted(have)}")

    # Map langue → URL d'origine (depuis `sources[]`) pour repopulation.
    sources = data.get("sources") or []
    url_by_lang: dict[str, dict] = {}
    for s in sources:
        if isinstance(s, dict) and s.get("langue"):
            url_by_lang[s["langue"]] = s

    traductions: dict[str, Traduction] = {}
    for lang, md_path in sorted(lang_files.items()):
        src = url_by_lang.get(lang) or {}
        # Préserve le `kind` et la provenance si une entrée existe déjà.
        # Utile en --force après strip_lang_nav : on veut juste recalculer
        # les sha256 sans remettre tout le monde en `originale`.
        prev = existing_traductions.get(lang) or {}
        traductions[lang] = Traduction(
            kind=prev.get("kind", "originale"),
            sha256=_sha256_file(md_path),
            source_url=prev.get("source_url") or src.get("url"),
            fetched_at=prev.get("fetched_at"),
            fetch_method=prev.get("fetch_method") or src.get("fetch_method"),
            model=prev.get("model"),
            translated_from=prev.get("translated_from"),
            source_sha256=prev.get("source_sha256"),
            translated_at=prev.get("translated_at"),
        )

    # Reconstruire le DocMeta complet pour garantir que le fichier reste valide
    # contre le nouveau schema.
    try:
        meta = DocMeta.model_validate({
            **data,
            "traductions": {k: v.model_dump(exclude_none=True) for k, v in traductions.items()},
        })
    except Exception as exc:  # noqa: BLE001
        return ("error", f"pydantic: {exc}")

    meta.sync_legacy_fields()

    if dry_run:
        return ("migrated", f"would write {len(traductions)} trads: {list(traductions.keys())}")

    meta.write(meta_path)
    return ("migrated", f"{len(traductions)} trads: {list(traductions.keys())}")


def main() -> int:
    p = argparse.ArgumentParser(description="Migre les .meta.yaml vers le bloc `traductions`.")
    p.add_argument("--dry-run", action="store_true", help="Ne pas écrire les fichiers.")
    p.add_argument("--force", action="store_true", help="Réécrire même si traductions existe déjà.")
    p.add_argument("--limit", type=int, default=None, help="Ne traiter que les N premiers fichiers (debug).")
    args = p.parse_args()

    meta_files = sorted(CORPUS.rglob("*.meta.yaml"))
    if args.limit is not None:
        meta_files = meta_files[: args.limit]

    counters = {"migrated": 0, "skipped": 0, "error": 0}
    errors: list[tuple[Path, str]] = []

    for meta_path in meta_files:
        status, note = migrate_one(meta_path, force=args.force, dry_run=args.dry_run)
        counters[status] += 1
        if status == "error":
            errors.append((meta_path, note or ""))

    prefix = "[DRY-RUN] " if args.dry_run else ""
    print(f"{prefix}Migration report — {len(meta_files)} .meta.yaml files")
    print(f"  migrated : {counters['migrated']}")
    print(f"  skipped  : {counters['skipped']}")
    print(f"  errors   : {counters['error']}")
    if errors:
        print(f"\n-- errors (showing {min(20, len(errors))}/{len(errors)}) --")
        for path, note in errors[:20]:
            print(f"  {path.relative_to(CORPUS)}: {note}")

    return 0 if counters["error"] == 0 else 1


if __name__ == "__main__":
    sys.exit(main())
