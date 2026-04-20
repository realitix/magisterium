"""Phase 1 — 20 ecumenical councils (Nicaea I → Vatican I).

Vatican II is handled separately in Phase 2.

Source: papalencyclicals.net (chronological council index, mostly English
translations, some Latin embedded). User's rule: take original language when
possible, translation when not. Council texts here are mostly English
translations of the Latin acta; for a full-Latin version we could later add
DCO, but papalencyclicals is the most coherent single index.
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date

from scrapers.core.pipeline import DocRef, MAGISTERIUM_ROOT, run_pipeline

BASE = "https://www.papalencyclicals.net/councils"

# (slug-suffix, incipit/name, date_iso, pope, url_fragment)
COUNCILS: list[tuple[str, str, str, str, str]] = [
    ("0325_nicaea-i",             "Nicée I",             "0325-06-19", "Sylvestre Ier",  "ecum01.htm"),
    ("0381_constantinople-i",     "Constantinople I",    "0381-05-01", "Damase Ier",     "ecum02.htm"),
    ("0431_ephese",               "Éphèse",              "0431-06-22", "Célestin Ier",   "ecum03.htm"),
    ("0451_chalcedoine",          "Chalcédoine",         "0451-10-08", "Léon Ier",       "ecum04.htm"),
    ("0553_constantinople-ii",    "Constantinople II",   "0553-05-05", "Vigile",         "ecum05.htm"),
    ("0680_constantinople-iii",   "Constantinople III",  "0680-11-07", "Agathon",        "ecum06.htm"),
    ("0787_nicee-ii",             "Nicée II",            "0787-09-24", "Hadrien Ier",    "ecum07.htm"),
    ("0869_constantinople-iv",    "Constantinople IV",   "0869-10-05", "Hadrien II",     "ecum08.htm"),
    ("1123_latran-i",             "Latran I",            "1123-03-18", "Calixte II",     "ecum09.htm"),
    ("1139_latran-ii",            "Latran II",           "1139-04-04", "Innocent II",    "ecum10.htm"),
    ("1179_latran-iii",           "Latran III",          "1179-03-05", "Alexandre III",  "ecum11.htm"),
    ("1215_latran-iv",            "Latran IV",           "1215-11-11", "Innocent III",   "ecum12-2.htm"),
    ("1245_lyon-i",               "Lyon I",              "1245-06-28", "Innocent IV",    "ecum13.htm"),
    ("1274_lyon-ii",              "Lyon II",             "1274-05-07", "Grégoire X",     "ecum14.htm"),
    ("1311_vienne",               "Vienne",              "1311-10-16", "Clément V",      "ecum15.htm"),
    ("1414_constance",            "Constance",           "1414-11-16", "Jean XXIII (antipape) / Martin V", "ecum16.htm"),
    ("1431_florence",             "Florence",            "1431-07-23", "Eugène IV",      "ecum17.htm"),
    ("1512_latran-v",             "Latran V",            "1512-05-03", "Jules II / Léon X", "ecum18.htm"),
    ("1545_trente",               "Trente",              "1545-12-13", "Paul III / Jules III / Pie IV", "trent.htm"),
    ("1869_vatican-i",            "Vatican I",           "1869-12-08", "Pie IX",         "ecum20.htm"),
]


def build_refs() -> list[DocRef]:
    target_dir = MAGISTERIUM_ROOT / "A-pre-vatican-ii" / "conciles-oecumeniques"
    refs: list[DocRef] = []
    for slug_suffix, incipit, date_iso, pope, frag in COUNCILS:
        refs.append(
            DocRef(
                url=f"{BASE}/{frag}",
                target_dir=target_dir,
                slug=slug_suffix,
                lang="en",
                meta_hints={
                    "incipit": incipit,
                    "titre_fr": f"Concile de {incipit}",
                    "auteur": pope,
                    "periode": "pre-vatican-ii",
                    "type": "concile-oecumenique",
                    "date": date.fromisoformat(date_iso),
                    "autorite_magisterielle": "magistere-extraordinaire-concile-oecumenique",
                    "langue_originale": "la",
                    "langues_disponibles": ["la", "en"],
                    "sujets": ["concile-oecumenique"],
                },
            )
        )
    return refs


async def main() -> int:
    import os
    refresh = os.environ.get("REFRESH") == "1"
    refs = build_refs()
    print(f"Phase 1 — {len(refs)} conciles œcuméniques")
    result = await run_pipeline(refs, phase="phase-1-conciles", refresh=refresh)
    print(
        f"ok={result.n_ok} skipped={result.n_skipped} "
        f"errors={result.n_errors}"
    )
    return 0 if result.n_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
