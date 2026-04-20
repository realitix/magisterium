"""Build magisterium/_metadata/index.jsonl from all .meta.yaml sidecars.

Run: ``uv run python -m tools.build_index``.
"""
from __future__ import annotations

import json
import sys
from datetime import date as _date
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "magisterium"
OUT = CORPUS / "_metadata" / "index.jsonl"


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


def build_index() -> int:
    entries: list[dict] = []
    errors: list[str] = []
    meta_files = sorted(CORPUS.rglob("*.meta.yaml"))
    for meta_path in meta_files:
        try:
            data = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
        except Exception as exc:  # noqa: BLE001
            errors.append(f"{meta_path}: YAML error {exc}")
            continue
        if not isinstance(data, dict):
            errors.append(f"{meta_path}: not a dict")
            continue

        rel = meta_path.relative_to(CORPUS).as_posix()
        # slug: filename stem before ".meta"
        slug = meta_path.name.removesuffix(".meta.yaml")

        entry = {
            "path": rel,
            "slug": slug,
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
        }
        entries.append(entry)

    # Sort by date ascending (None last, to keep stable order)
    def _key(e: dict):
        d = e.get("date")
        return (0, d) if d else (1, e.get("slug") or "")

    entries.sort(key=_key)

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8") as fh:
        for e in entries:
            fh.write(json.dumps(e, ensure_ascii=False) + "\n")

    print(f"[build_index] wrote {len(entries)} entries to {OUT.relative_to(ROOT)}")
    if errors:
        print(f"[build_index] {len(errors)} parse errors:", file=sys.stderr)
        for e in errors[:20]:
            print(f"  - {e}", file=sys.stderr)
        if len(errors) > 20:
            print(f"  ... ({len(errors) - 20} more)", file=sys.stderr)
    return 0


def main() -> int:
    return build_index()


if __name__ == "__main__":
    sys.exit(main())
