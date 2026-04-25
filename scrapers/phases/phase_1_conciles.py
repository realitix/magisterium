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

# Concile de Trente : les 25 sessions + la bulle d'indiction. Chaque session
# est un document distinct (canons et décrets propres), regroupés sous le
# pattern « ouvrage » avec parent slug `1545_trente`. Le `1545_trente` legacy
# (fichier TOC) est conservé pour rétrocompatibilité et sert de page d'index.
TRENTE_BASE = "https://www.papalencyclicals.net/councils/trent"

# (partie_index, slug-suffix, url-fragment, partie-titre)
TRENTE_PARTS: list[tuple[int, str, str, str]] = [
    (1,  "00-bulla-indictionis",                  "ctbull.htm",                      "Bulle d'indiction Laetare Ierusalem (1544)"),
    (2,  "sess-01",                               "firstsession.htm",                "Première session — ouverture (13 décembre 1545)"),
    (3,  "sess-02",                               "second-session.htm",              "Deuxième session — règles de vie pendant le concile (7 janvier 1546)"),
    (4,  "sess-03",                               "third-session.htm",               "Troisième session — symbole de foi (4 février 1546)"),
    (5,  "sess-04",                               "fourth-session.htm",              "Quatrième session — Écritures canoniques et tradition (8 avril 1546)"),
    (6,  "sess-05",                               "fifth-session.htm",               "Cinquième session — péché originel (17 juin 1546)"),
    (7,  "sess-06-de-iustificatione",             "sixth-session.htm",               "Sixième session — De iustificatione (13 janvier 1547)"),
    (8,  "sess-07-de-sacramentis",                "seventh-session.htm",             "Septième session — De sacramentis in genere et de baptismo (3 mars 1547)"),
    (9,  "sess-08",                               "eighth-session.htm",              "Huitième session — translation à Bologne (11 mars 1547)"),
    (10, "sess-09",                               "ninth-session.htm",               "Neuvième session — prorogation à Bologne (21 avril 1547)"),
    (11, "sess-10",                               "tenth-session.htm",               "Dixième session — prorogation à Bologne (2 juin 1547)"),
    (12, "sess-11",                               "eleventh-session.htm",            "Onzième session — reprise du concile à Trente (1ᵉʳ mai 1551)"),
    (13, "sess-12",                               "twelfth-session.htm",             "Douzième session — prorogation (1ᵉʳ septembre 1551)"),
    (14, "sess-13-de-eucharistia",                "thirteenth-session.htm",          "Treizième session — De ss. Eucharistiae sacramento (11 octobre 1551)"),
    (15, "sess-14-de-poenitentia",                "fourteenth-session.htm",          "Quatorzième session — De sacramento poenitentiae et extremae unctionis (25 novembre 1551)"),
    (16, "sess-15",                               "fifteenth-session.htm",           "Quinzième session — prorogation (25 janvier 1552)"),
    (17, "sess-16",                               "sixteenth-session.htm",           "Seizième session — suspension (28 avril 1552)"),
    (18, "sess-17",                               "seventeenth-session.htm",         "Dix-septième session — reprise sous Pie IV (18 janvier 1562)"),
    (19, "sess-18",                               "eighteenth-session.htm",          "Dix-huitième session — index des livres prohibés (26 février 1562)"),
    (20, "sess-19",                               "nineteenth-session.htm",          "Dix-neuvième session — prorogation (14 mai 1562)"),
    (21, "sess-20",                               "twentieth-session.htm",           "Vingtième session — prorogation (4 juin 1562)"),
    (22, "sess-21-de-communione",                 "twenty-first-session.htm",        "Vingt-et-unième session — De communione sub utraque specie (16 juillet 1562)"),
    (23, "sess-22-de-sacrificio-missae",          "twenty-second-session.htm",       "Vingt-deuxième session — De sacrificio missae (17 septembre 1562)"),
    (24, "sess-23-de-sacramento-ordinis",         "twenty-third-session.htm",        "Vingt-troisième session — De sacramento ordinis (15 juillet 1563)"),
    (25, "sess-24-de-sacramento-matrimonii",      "twenty-fourth-session.htm",       "Vingt-quatrième session — De sacramento matrimonii (11 novembre 1563)"),
    (26, "sess-25",                               "twenty-fifth-session.htm",        "Vingt-cinquième session — purgatoire, saints, indulgences, clôture (3-4 décembre 1563)"),
]
TRENTE_TOTAL_PARTIES = len(TRENTE_PARTS)

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


def build_trente_parts_refs() -> list[DocRef]:
    """26 documents : la bulle d'indiction + les 25 sessions du concile de Trente.

    Chaque partie est rangée sous `magisterium/A-pre-vatican-ii/conciles-oecumeniques/1545-trente/`
    avec un bloc `ouvrage` pointant sur le slug parent `1545_trente` (TOC legacy).
    Cela permet aux fiches Q/R d'attaquer les sessions doctrinales individuellement
    (ex. session XXII pour le sacrifice de la messe, session XIII pour
    l'eucharistie) plutôt que le slug parent qui n'est qu'un index.
    """
    target_dir = MAGISTERIUM_ROOT / "A-pre-vatican-ii" / "conciles-oecumeniques" / "1545-trente"
    refs: list[DocRef] = []
    for partie_index, slug_suffix, frag, partie_titre in TRENTE_PARTS:
        full_slug = f"1545_trente_{slug_suffix}"
        is_bulle = slug_suffix.endswith("bulla-indictionis")
        refs.append(
            DocRef(
                url=f"{TRENTE_BASE}/{frag}",
                target_dir=target_dir,
                slug=full_slug,
                lang="en",
                meta_hints={
                    "incipit": partie_titre.split("—")[0].strip() or "Trente",
                    "titre_fr": partie_titre,
                    "auteur": "Paul III / Jules III / Pie IV",
                    "periode": "pre-vatican-ii",
                    "type": "constitution-apostolique" if is_bulle else "concile-session",
                    "date": date(1545, 12, 13),
                    "autorite_magisterielle": "magistere-extraordinaire-concile-oecumenique",
                    "langue_originale": "la",
                    "langues_disponibles": ["la", "en"],
                    "sujets": ["concile-oecumenique", "trente"],
                    "ouvrage": {
                        "slug": "1545_trente",
                        "titre": "Concile de Trente (1545-1563)",
                        "partie_index": partie_index,
                        "partie_titre": partie_titre,
                        "total_parties": TRENTE_TOTAL_PARTIES,
                    },
                },
            )
        )
    return refs


async def main() -> int:
    import os
    refresh = os.environ.get("REFRESH") == "1"
    refs = build_refs() + build_trente_parts_refs()
    print(f"Phase 1 — {len(refs)} conciles œcuméniques (incl. {TRENTE_TOTAL_PARTIES} parties de Trente)")
    result = await run_pipeline(refs, phase="phase-1-conciles", refresh=refresh)
    print(
        f"ok={result.n_ok} skipped={result.n_skipped} "
        f"errors={result.n_errors}"
    )
    return 0 if result.n_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
