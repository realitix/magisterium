"""Corpus statistics: totals, periods, authors, types, languages, themes.

Run: ``uv run python -m tools.stats``.
"""
from __future__ import annotations

import json
import sys
from collections import Counter
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "magisterium"
INDEX = CORPUS / "_metadata" / "index.jsonl"
CONCORDANCE = CORPUS / "_metadata" / "concordance.jsonl"


PERIODS = [
    ("pre-vatican-ii", "pre-V2"),
    ("vatican-ii", "V2"),
    ("post-vatican-ii", "post-V2"),
    ("fsspx", "FSSPX"),
]


def _load_index() -> list[dict]:
    if not INDEX.exists():
        print(f"[stats] index missing: {INDEX}", file=sys.stderr)
        print("  run `just build-index` first", file=sys.stderr)
        sys.exit(1)
    return [json.loads(line) for line in INDEX.read_text(encoding="utf-8").splitlines() if line.strip()]


def _load_concordance() -> list[dict]:
    if not CONCORDANCE.exists():
        return []
    return [json.loads(line) for line in CONCORDANCE.read_text(encoding="utf-8").splitlines() if line.strip()]


def _table(title: str, rows: list[tuple[str, int]], total: int | None = None) -> str:
    lines = [f"\n== {title} =="]
    if not rows:
        lines.append("  (none)")
        return "\n".join(lines)
    width = max(len(str(k)) for k, _ in rows)
    for k, v in rows:
        pct = f"  ({100 * v / total:5.1f}%)" if total else ""
        lines.append(f"  {str(k).ljust(width)}  {str(v).rjust(5)}{pct}")
    return "\n".join(lines)


def main() -> int:
    entries = _load_index()
    total = len(entries)

    print(f"Corpus magistériel — {total} documents indexés")

    # Periods
    by_period = Counter(e.get("periode") or "(unknown)" for e in entries)
    rows = []
    for key, label in PERIODS:
        rows.append((label, by_period.get(key, 0)))
    for k in sorted(by_period):
        if k not in [p[0] for p in PERIODS]:
            rows.append((k, by_period[k]))
    print(_table("Répartition par période", rows, total=total))

    # Top 15 authors
    by_author = Counter(e.get("auteur") or "(inconnu)" for e in entries)
    print(_table("Top 15 auteurs / papes", by_author.most_common(15), total=total))

    # Types
    by_type = Counter(e.get("type") or "(unknown)" for e in entries)
    print(_table("Répartition par type", by_type.most_common(), total=total))

    # Langues originales
    by_lang = Counter(e.get("langue_originale") or "(unknown)" for e in entries)
    print(_table("Langue originale", by_lang.most_common(), total=total))

    # Themes (from concordance)
    conc = _load_concordance()
    if conc:
        rows = []
        all_classified: set[str] = set()
        for rec in conc:
            count = len(rec["pre_v2"]) + len(rec["v2"]) + len(rec["post_v2"])
            rows.append((rec["theme"], count))
            all_classified.update(rec["pre_v2"])
            all_classified.update(rec["v2"])
            all_classified.update(rec["post_v2"])
        rows.sort(key=lambda r: (-r[1], r[0]))
        print(_table("Documents par thème (concordance)", rows))

        unclassified = [e for e in entries if e.get("slug") not in all_classified]
        print(f"\n== Documents sans aucun thème : {len(unclassified)} / {total} ==")
        # Show a small sample to guide manual curation.
        sample = unclassified[:25]
        for e in sample:
            date = e.get("date") or "????-??-??"
            incipit = e.get("incipit") or e.get("slug") or "?"
            auteur = e.get("auteur") or "?"
            print(f"  - [{date}] {incipit} — {auteur}")
        if len(unclassified) > len(sample):
            print(f"  ... ({len(unclassified) - len(sample)} autres)")
    else:
        print("\n(concordance.jsonl absent — lancer `just build-concordance`)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
