"""Application d'une traduction IA au corpus.

Appelé par le skill `translate-corpus` après qu'un agent a produit un
markdown traduit. Calcule le sha256 du fichier final, met à jour l'entrée
`traductions[lang]` dans le `.meta.yaml` du document avec `kind: ia` et
toutes ses métadonnées de provenance, puis resynchronise les champs
historiques (`langues_disponibles`, `sha256`).

Le markdown traduit doit déjà être en place à `<target_path>` (le script
lit le fichier, calcule son hash, mais ne le modifie pas).

Run :

    uv run python -m tools.translate_apply \
        --meta-path /.../<slug>.meta.yaml \
        --lang fr \
        --source-lang la \
        --source-sha256 abc123 \
        --model claude-opus-4-7

Le markdown cible est déduit : `<meta_path stripped>.{lang}.md`.
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from datetime import datetime, timezone
from pathlib import Path

from scrapers.core.meta import DocMeta, Traduction


def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    h.update(path.read_bytes())
    return h.hexdigest()


def apply(
    meta_path: Path,
    lang: str,
    source_lang: str,
    source_sha256: str,
    model: str,
) -> None:
    stem = meta_path.name.removesuffix(".meta.yaml")
    target_md = meta_path.with_name(f"{stem}.{lang}.md")
    if not target_md.exists():
        raise SystemExit(f"target .md introuvable : {target_md}")

    sha = _sha256_file(target_md)
    # Recharge le meta via Pydantic (validation + conversion yaml→objet)
    meta = DocMeta.read(meta_path)
    meta.traductions[lang] = Traduction(
        kind="ia",
        sha256=sha,
        model=model,
        translated_from=source_lang,
        source_sha256=source_sha256,
        translated_at=datetime.now(timezone.utc),
    )
    meta.sync_legacy_fields()
    meta.write(meta_path)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--meta-path", required=True)
    p.add_argument("--lang", required=True)
    p.add_argument("--source-lang", required=True)
    p.add_argument("--source-sha256", required=True)
    p.add_argument("--model", required=True)
    args = p.parse_args()

    apply(
        meta_path=Path(args.meta_path),
        lang=args.lang,
        source_lang=args.source_lang,
        source_sha256=args.source_sha256,
        model=args.model,
    )
    print(f"[translate_apply] ok: {Path(args.meta_path).name} + traductions[{args.lang}]=ia")
    return 0


if __name__ == "__main__":
    sys.exit(main())
