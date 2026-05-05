"""Build index.jsonl files from all .meta.yaml sidecars.

Génère deux index séparés :
- ``magisterium/_metadata/index.jsonl`` — corpus magistériel (autorité)
- ``livres/_metadata/index.jsonl`` — références non-magistérielles (livres)

Le champ `categorie` du `.meta.yaml` (défaut: ``magistere``) propagé dans chaque
entrée d'index permet aux consommateurs (site Astro) de fusionner les deux
sources tout en distinguant l'autorité.

Run: ``uv run python -m tools.build_index``.
"""
from __future__ import annotations

import json
import sys
from datetime import date as _date
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
ROOTS: tuple[tuple[str, Path], ...] = (
    ("magistere", ROOT / "magisterium"),
    ("livre", ROOT / "livres"),
)


def _pick_sha(sha256: dict, langue_originale: str | None) -> str | None:
    if not sha256:
        return None
    if langue_originale and langue_originale in sha256:
        return sha256[langue_originale]
    # fallback: first available
    for k in sha256:
        return sha256[k]
    return None


def _iso(d) -> str | None:
    if d is None:
        return None
    if isinstance(d, _date):
        return d.isoformat()
    return str(d)


def _build_one(corpus_root: Path, default_categorie: str) -> tuple[int, list[str]]:
    """Build one index for the given corpus root. Returns (count, errors)."""
    if not corpus_root.exists():
        return 0, []
    entries: list[dict] = []
    errors: list[str] = []
    meta_files = sorted(corpus_root.rglob("*.meta.yaml"))
    for meta_path in meta_files:
        try:
            data = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{meta_path}: YAML error {exc}")
            continue
        if not isinstance(data, dict):
            errors.append(f"{meta_path}: not a dict")
            continue

        # Documents marqués `private: true` (rares — ex. textes hors domaine
        # public conservés au dépôt pour analyse mais non publiés) sont exclus
        # de l'index et donc invisibles au site et à Pagefind.
        if data.get("private") is True:
            continue

        rel = meta_path.relative_to(corpus_root).as_posix()
        slug = meta_path.name.removesuffix(".meta.yaml")

        traductions = data.get("traductions") or {}
        traductions_summary = sorted(
            (
                {"lang": lang, "kind": (t or {}).get("kind", "originale")}
                for lang, t in traductions.items()
            ),
            key=lambda e: e["lang"],
        )

        entry = {
            "path": rel,
            "slug": slug,
            "categorie": data.get("categorie", default_categorie),
            "incipit": data.get("incipit"),
            "titre_fr": data.get("titre_fr"),
            "auteur": data.get("auteur"),
            "periode": data.get("periode"),
            "type": data.get("type"),
            "date": _iso(data.get("date")),
            "langue_originale": data.get("langue_originale"),
            "sha256": _pick_sha(data.get("sha256") or {}, data.get("langue_originale")),
            "sujets": data.get("sujets") or [],
            "themes_doctrinaux": data.get("themes_doctrinaux") or [],
            "traductions": traductions_summary,
        }
        ouvrage = data.get("ouvrage")
        if ouvrage is not None:
            entry["ouvrage"] = ouvrage
        entries.append(entry)

    def _key(e: dict):
        d = e.get("date")
        return (0, d) if d else (1, e.get("slug") or "")

    entries.sort(key=_key)

    out_path = corpus_root / "_metadata" / "index.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as fh:
        for e in entries:
            fh.write(json.dumps(e, ensure_ascii=False) + "\n")

    print(f"[build_index] wrote {len(entries)} entries to {out_path.relative_to(ROOT)}")
    return len(entries), errors


def build_index() -> int:
    total = 0
    all_errors: list[str] = []
    for default_categorie, corpus_root in ROOTS:
        count, errors = _build_one(corpus_root, default_categorie)
        total += count
        all_errors.extend(errors)

    if all_errors:
        print(f"[build_index] {len(all_errors)} parse errors:", file=sys.stderr)
        for e in all_errors[:20]:
            print(f"  - {e}", file=sys.stderr)
        if len(all_errors) > 20:
            print(f"  ... ({len(all_errors) - 20} more)", file=sys.stderr)
    print(f"[build_index] total: {total} entries across {len(ROOTS)} corpus roots")
    return 0


def main() -> int:
    return build_index()


if __name__ == "__main__":
    sys.exit(main())
