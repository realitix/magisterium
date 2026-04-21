"""Phase 7 — Official catechisms, pre- and post-Vatican II.

Inventory
---------

Pre-V2  (magisterium/A-pre-vatican-ii/catechismes/)
  1. 1566 Catéchisme du concile de Trente (Catechismus Romanus) .. scraped (OCR, 6 Partes)
  2. 1555 Catéchisme de Canisius                                 .. MISSING (OCR inadéquat)
  3. 1598 Catéchisme de Bellarmin (Dottrina cristiana breve)     .. scraped (archive.org OCR 1842)
  4. 1885 Catéchisme de Baltimore (4 vols, Project Gutenberg)    .. scraped
  5. 1908 Catéchisme de Pie X                                    .. scraped (maranatha.it)

Post-V2 (magisterium/C-post-vatican-ii/catechismes/)
  6. 1992 Catéchisme de l'Église catholique, version FR initiale .. scraped (~430 pp.)
  7. 1997 CCC editio typica Latina (LT)                          .. scraped (~105 pp.)
  8. 2005 Compendium du CCC (IT: confirmé, LA: introuvable)      .. partial
  9. 2011 YOUCAT                                                 .. MISSING (copyright)

Strategy
--------
For the two CCC editions we discover the full page list from the master
index (_INDEX.HTM for FR, index_lt.htm for LT) and emit one DocRef per
sub-page, all sharing the same directory. Each sub-page lands as
  {date}_{id}_ccc-{part}.{lang}.md
with its own .meta.yaml (slug uniqueness is needed because the pipeline
writes one meta.yaml per slug). This is consistent with other phases that
treat every HTML page as one archival unit.

MISSING catechisms — those without a stable, machine-reachable source in
the original language — get a .MISSING.md stub per incipit that explains
why, what was searched, and suggests manual paths.
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Optional

import httpx

from scrapers.core.errors import log_error
from scrapers.core.fetcher import USER_AGENT
from scrapers.core.pipeline import DocRef, MAGISTERIUM_ROOT, run_pipeline
from scrapers.core.rate_limit import GLOBAL_LIMITER

PHASE = "phase-7-catechismes"

PRE_V2_DIR = MAGISTERIUM_ROOT / "A-pre-vatican-ii" / "catechismes"
POST_V2_DIR = MAGISTERIUM_ROOT / "C-post-vatican-ii" / "catechismes"


# -- Small HTTP helper (rate-limited GET) ----------------------------------

async def _http_get_text(url: str, *, verify: bool = True) -> Optional[str]:
    domain = httpx.URL(url).host
    await GLOBAL_LIMITER.acquire(domain)
    try:
        async with httpx.AsyncClient(
            http2=True, follow_redirects=True, timeout=60.0,
            verify=verify,
            headers={
                "User-Agent": USER_AGENT,
                "Accept-Language": "la,fr,it,en;q=0.8",
            },
        ) as client:
            r = await client.get(url)
        if r.status_code != 200:
            return None
        return r.text
    except Exception as e:  # noqa: BLE001
        log_error(
            source=domain, url=url, phase=PHASE,
            message=f"discover-fetch: {type(e).__name__}: {e}",
        )
        return None


# -- MISSING stubs ---------------------------------------------------------

MISSING_NOTES: list[tuple[Path, str, str]] = [
    # 1566 Catechismus Romanus — ingéré en Phase 10 V2 à partir de l'OCR
    # DjVu du scan archive.org `gri_33125007759364` (Ratisbonae 1905,
    # editio stereotypa sexta). Le texte latin est post-traité (suppression
    # des en-têtes/pieds de page courants, dédoublage des espaces,
    # déhyphénation, correctifs OCR ciblés) puis segmenté en six fichiers
    # `.la.md` dans `1566-romain-trente/` : praefatio (lettre Clemens XIII),
    # Pars Prima → Pars Quarta, et Praxis Catechismi. Qualité OCR mesurée :
    # ~0,07 % de mots suspects sur ~148 000 mots — bien en-deçà du seuil
    # projet (20 %). Aucun stub MISSING.md n'est plus émis.
    (
        PRE_V2_DIR / "1555-canisius",
        "1555_summa-doctrinae.MISSING.md",
        """# Catéchisme de Canisius (1555) — MANQUANT (OCR inadéquat)

*Summa doctrinae christianae*, saint Pierre Canisius SJ (1521-1597),
édition princeps Vienne 1555.

Réexamen Phase 10 (2026-04-21) : aucune source HTML propre ; toutes les
numérisations archive.org (1587 Plantin, 1764 Viennae, 1823 Landshut,
1834 4 vols) ont un OCR dégradé (majuscules ornées lues en cyrillique,
coupures mid-mot, pieds de page Digitized-by-Google). Le PDF DCO
(19,5 Mo, édition critique Latina) n'a pas d'équivalent HTML. Voir
stub `1555_summa-doctrinae.MISSING.md` pour tableau comparatif et
pistes V2.

Conformément à « Ne pas forcer une ingestion bancale », conservé en
MISSING — candidat V2 prioritaire.
""",
    ),
    # 1598 Bellarmin — ingéré en Phase 10 depuis archive.org/details/
    # bub_gb_KHZRwxilf7MC (édition 1842 de la Dottrina cristiana breve
    # de 1598, + Istruzioni per i Sacramenti). OCR nettoyé + segmenté
    # en 6 parties (~7 800 mots). La Dichiarazione più copiosa reste à
    # faire (archive.org bub_gb_Xlc-tGjQ7YAC, ~300 Ko OCR, non ingérée).
    # 1908 Pie X — ingéré depuis maranatha.it/catpiox/NNtext.htm (IT).
    (
        POST_V2_DIR / "2011-youcat",
        "2011_youcat.MISSING.md",
        """# YOUCAT (2011) — MANQUANT (droits d'auteur actifs)

**Titre complet :** *YOUCAT Deutsch — Jugendkatechismus der Katholischen Kirche*
**Langue originale :** allemand (Deutsch).
**Préface :** Benoît XVI.
**Éditeur original (DE) :** Pattloch Verlag GmbH & Co. KG, München, 2010/2011
(ISBN 978-3-629-02194-8). Éditeur anglophone : Ignatius Press.

## Décision : non ingérable dans la V1 du corpus

YOUCAT est un livre commercial sous copyright actif (Pattloch Verlag /
Ignatius Press, «All rights reserved»). Aucune édition libre officielle
(Creative Commons ou domaine public anticipé) n'a été publiée. Vatican.va
n'héberge pas YOUCAT ; `youcat.org` ne distribue que des aperçus
commerciaux. Les copies PDF qui circulent sur archive.org / scribd /
agrégateurs sont des violations de copyright, non des diffusions
autorisées — donc exclues par la règle projet d'absence de violation
des droits.

## Alternative disponible dans le corpus

Le **Compendium du Catéchisme de l'Église catholique (2005)** est le
catéchisme court officiel du Saint-Siège. Il est présent dans le corpus :
- `2005-06-28_compendium-ccc.it.md` (IT, 598 questions, complet)
- `2005-06-28_compendium-ccc-editio-typica-latina.la.pdf` (LA, editio
  typica, PDF officiel).
""",
    ),
]


# -- 1. CCC 1992 FR (vatican.va/archive/FRA0013/) --------------------------

CCC_FR_INDEX = "https://www.vatican.va/archive/FRA0013/_INDEX.HTM"
CCC_FR_BASE = "https://www.vatican.va/archive/FRA0013/"
# href patterns on the index: __P*.HTM (alphanum, up to 3 chars). The
# source HTML uses unquoted attributes (`href=__P1.HTM`), so the regex
# accepts either quoted or bare values.
CCC_FR_HREF_RE = re.compile(
    r'(?i)href=(?:"|\')?(__P[0-9A-Z]{1,3}\.HTM)(?:"|\')?[\s>]',
)


async def discover_ccc_fr() -> list[DocRef]:
    """All __P*.HTM subpages of the 1992 French CCC."""
    text = await _http_get_text(CCC_FR_INDEX)
    if not text:
        log_error(source="www.vatican.va", url=CCC_FR_INDEX, phase=PHASE,
                  message="CCC FR index unreachable")
        return []
    target_dir = POST_V2_DIR / "1992-ccc"
    refs: list[DocRef] = []
    # Include the index itself as page 00
    seen: set[str] = set()
    leaves = ["_INDEX.HTM"] + [
        m.group(1).upper()
        for m in CCC_FR_HREF_RE.finditer(text)
    ]
    hints = _ccc_1992_hints()
    for leaf in leaves:
        if leaf in seen:
            continue
        seen.add(leaf)
        page_id = _ccc_fr_page_id(leaf)
        slug = f"1992-10-11_ccc_{page_id}"
        refs.append(DocRef(
            url=CCC_FR_BASE + leaf,
            target_dir=target_dir,
            slug=slug,
            lang="fr",
            # Legacy vatican.va IntraText pages use layout tables + <font>
            # wrappers that pandoc collapses to "[TABLE]" tokens. Unwrap
            # them so the prose flows.
            unwrap_tags=["table", "tbody", "tr", "td", "th", "font", "center"],
            meta_hints=dict(hints),
        ))
    return refs


def _ccc_fr_page_id(leaf: str) -> str:
    # "_INDEX.HTM" → "index"; "__P1.HTM" → "p1"; "__PA.HTM" → "pa"; "__P1A.HTM" → "p1a"
    base = leaf.upper().removesuffix(".HTM")
    base = base.lstrip("_")
    return base.lower()


def _ccc_1992_hints() -> dict:
    return {
        "incipit": "Catéchisme de l'Église catholique",
        "titre_fr": "Catéchisme de l'Église catholique (1992)",
        "auteur": "Jean-Paul II",
        "periode": "post-vatican-ii",
        "type": "catechisme",
        "date": date(1992, 10, 11),
        "autorite_magisterielle": "magistere-ordinaire-universel",
        "langue_originale": "fr",
        "langues_disponibles": ["fr"],
        "sujets": ["catechisme"],
    }


# -- 2. CCC 1997 editio typica Latina (vatican.va/archive/catechism_lt/) --

CCC_LT_INDEX = "https://www.vatican.va/archive/catechism_lt/index_lt.htm"
CCC_LT_BASE = "https://www.vatican.va/archive/catechism_lt/"
# hrefs are bare *_lt.htm filenames (p1s1c1_lt.htm, p122a3p1_lt.htm, etc.),
# possibly followed by a fragment (#ARTICULUS...). Accept quoted or bare values
# and ignore the fragment portion. Reject absolute URLs (we only want local
# leaves under catechism_lt/).
CCC_LT_HREF_RE = re.compile(
    r'(?i)href=(?:"|\')?([a-z0-9_\-]+_lt\.htm)(?:#[^"\'\s>]*)?(?:"|\')?[\s>]',
)


async def discover_ccc_lt() -> list[DocRef]:
    text = await _http_get_text(CCC_LT_INDEX)
    if not text:
        log_error(source="www.vatican.va", url=CCC_LT_INDEX, phase=PHASE,
                  message="CCC LT index unreachable")
        return []
    target_dir = POST_V2_DIR / "1997-ccc-editio-typica-latina"
    refs: list[DocRef] = []
    seen: set[str] = set()
    leaves = ["index_lt.htm"] + [
        m.group(1).lower()
        for m in CCC_LT_HREF_RE.finditer(text)
    ]
    hints = _ccc_1997_hints()
    for leaf in leaves:
        if leaf in seen:
            continue
        seen.add(leaf)
        page_id = leaf.removesuffix(".htm")
        slug = f"1997-08-15_ccc-lt_{page_id}"
        refs.append(DocRef(
            url=CCC_LT_BASE + leaf,
            target_dir=target_dir,
            slug=slug,
            lang="la",
            unwrap_tags=["table", "tbody", "tr", "td", "th", "font", "center"],
            meta_hints=dict(hints),
        ))
    return refs


def _ccc_1997_hints() -> dict:
    return {
        "incipit": "Catechismus Catholicae Ecclesiae",
        "titre_fr": "Catéchisme de l'Église catholique — editio typica latina (1997)",
        "auteur": "Jean-Paul II",
        "periode": "post-vatican-ii",
        "type": "catechisme",
        "date": date(1997, 8, 15),
        "autorite_magisterielle": "magistere-ordinaire-universel",
        "langue_originale": "la",
        "langues_disponibles": ["la"],
        "sujets": ["catechisme"],
    }


# -- 3. Compendium CCC 2005 (single-page HTML) -----------------------------

# Verified in Phase 7 probing:
#   FR: archive_2005_compendium-ccc_fr.html  → 200 OK (single page)
#   IT: archive_2005_compendium-ccc_it.html  → 200 OK (complete, 598 Q)
#   LA: archive_2005_compendium-ccc_la.html  → 404 (no Latin HTML)
#   LA: compendium_catech_lit.pdf            → 200 OK (editio typica PDF)
#   EN: archive_2005_compendium-ccc_en.html  → 200 OK
# The Compendium's editio typica is Latin and was only published as PDF
# (never HTML). We archive both:
#   - the Italian HTML (fine-grained, 598 question-level anchors),
#   - the Latin PDF (editio typica, stored as-is via the pipeline's PDF path).
COMPENDIUM_BASE = (
    "https://www.vatican.va/archive/compendium_ccc/documents/"
)


def build_compendium_refs() -> list[DocRef]:
    target_dir = POST_V2_DIR / "2005-compendium"
    hints_it = {
        "incipit": "Compendium Catechismi Catholicae Ecclesiae",
        "titre_fr": "Compendium du Catéchisme de l'Église catholique (2005)",
        "auteur": "Benoît XVI",
        "periode": "post-vatican-ii",
        "type": "catechisme",
        "date": date(2005, 6, 28),
        "autorite_magisterielle": "magistere-ordinaire-universel",
        "langue_originale": "la",
        "langues_disponibles": ["it", "la"],
        "sujets": ["catechisme", "compendium"],
    }
    hints_la = dict(hints_it)
    return [
        DocRef(
            url=COMPENDIUM_BASE + "archive_2005_compendium-ccc_it.html",
            target_dir=target_dir,
            slug="2005-06-28_compendium-ccc",
            lang="it",
            unwrap_tags=["table", "tbody", "tr", "td", "th", "font", "center"],
            meta_hints=dict(hints_it),
        ),
        # Editio typica latina — published only as PDF. Pipeline stores
        # PDF bytes + stub .md marker. Enables future text extraction
        # without re-fetching.
        DocRef(
            url=COMPENDIUM_BASE + "compendium_catech_lit.pdf",
            target_dir=target_dir,
            slug="2005-06-28_compendium-ccc-editio-typica-latina",
            lang="la",
            meta_hints=dict(hints_la),
        ),
    ]


# -- 4. Catéchisme de Baltimore (1885) — Project Gutenberg ----------------

# 4 volumes. Gutenberg HTML URLs (verified redirect 20-04-2026):
#   https://www.gutenberg.org/cache/epub/14551/pg14551-images.html
#   14552, 14553, 14554 same pattern.
BALTIMORE_VOLS: list[tuple[int, int, str]] = [
    (1, 14551, "Baltimore Catechism No. 1"),
    (2, 14552, "Baltimore Catechism No. 2"),
    (3, 14553, "Baltimore Catechism No. 3"),
    (4, 14554, "Baltimore Catechism No. 4"),
]


def build_baltimore_refs() -> list[DocRef]:
    target_dir = PRE_V2_DIR / "1885-baltimore"
    refs: list[DocRef] = []
    for vol_num, gutenberg_id, title in BALTIMORE_VOLS:
        refs.append(DocRef(
            url=f"https://www.gutenberg.org/cache/epub/{gutenberg_id}/pg{gutenberg_id}-images.html",
            target_dir=target_dir,
            slug=f"1885-04-06_baltimore-catechism-no-{vol_num}",
            lang="en",
            meta_hints={
                "incipit": f"Baltimore Catechism No. {vol_num}",
                "titre_fr": f"Catéchisme de Baltimore n° {vol_num}",
                "auteur": "Conférence plénière des évêques des États-Unis",
                "periode": "pre-vatican-ii",
                "type": "catechisme",
                "date": date(1885, 4, 6),
                "autorite_magisterielle": "magistere-ordinaire-universel",
                "langue_originale": "en",
                "langues_disponibles": ["en"],
                "sujets": ["catechisme"],
            },
        ))
    return refs


# -- 5. Catéchisme de Pie X (1908) — maranatha.it/catpiox/ -----------------
#
# Mirror italien stable (vérifié 21-04-2026) hébergeant le *Catechismo
# Maggiore di San Pio X*, édition « Compendio della dottrina cristiana
# prescritto da Sua Santità Papa Pio X alle diocesi della provincia di
# Roma, Roma, Tipogr. Vaticana, 1905 ». Le site utilise un frameset :
#   {NN}page.htm  — wrapper frameset (non exploitable par pandoc)
#   {NN}left.htm  — sidebar décoratif
#   {NN}text.htm  — contenu réel, seul à ingérer
# Encodage déclaré : windows-1252 (pipeline _extract_body détecte charset).
# 16 pages de contenu : 02text.htm … 17text.htm (18text.htm → 404).

PIO_X_BASE = "https://www.maranatha.it/catpiox/"

# (page_num, slug_suffix, section_label). Ordre canonique du catéchisme.
PIO_X_PAGES: list[tuple[int, str, str]] = [
    (2,  "00-introduzione",         "Introduzione al Compendio"),
    (3,  "01-lettera-respighi",     "Lettera di San Pio X al Cardinale Respighi"),
    (4,  "02-lezione-preliminare",  "Lezione preliminare — Della Dottrina Cristiana"),
    (5,  "03-parte-1-credo",        "Parte prima — Del Simbolo degli Apostoli (Credo)"),
    (6,  "04-parte-2-orazione",     "Parte seconda — Dell'Orazione"),
    (7,  "05-parte-3-comandamenti", "Parte terza — Dei Comandamenti di Dio e della Chiesa"),
    (8,  "06-parte-4-sacramenti",   "Parte quarta — Dei Sacramenti"),
    (9,  "07-parte-5-virtu",        "Parte quinta — Delle virtù principali"),
    (10, "08-feste-1-signore",      "Istruzione sulle feste — parte I (del Signore)"),
    (11, "09-feste-2-vergine-santi","Istruzione sulle feste — parte II (Vergine e santi)"),
    (12, "10-storia-principi",      "Breve storia della religione — Principi e nozioni fondamentali"),
    (13, "11-storia-1-antico-testamento", "Breve storia — parte I (Antico Testamento)"),
    (14, "12-storia-2-nuovo-testamento",  "Breve storia — parte II (Nuovo Testamento)"),
    (15, "13-storia-3-ecclesiastica",     "Breve storia — parte III (Storia ecclesiastica)"),
    (16, "14-preghiere-1",          "Appendice — Preghiere e formule (1)"),
    (17, "15-preghiere-2",          "Appendice — Preghiere e formule (2)"),
]


def _pio_x_hints(section_label: str) -> dict:
    return {
        "incipit": f"Catechismo Maggiore di San Pio X — {section_label}",
        "titre_fr": f"Catéchisme de saint Pie X — {section_label}",
        "auteur": "Pie X",
        "periode": "pre-vatican-ii",
        "type": "catechisme",
        "date": date(1908, 6, 15),  # approbation diocésaine de l'édition 1908
        "autorite_magisterielle": "magistere-ordinaire-universel",
        "langue_originale": "it",
        "langues_disponibles": ["it"],
        "sujets": ["catechisme"],
        "themes_doctrinaux": [
            "credo", "decalogue", "sacrements", "oraison-dominicale",
            "vertus",
        ],
    }


def build_pio_x_refs() -> list[DocRef]:
    target_dir = PRE_V2_DIR / "1908-pie-x"
    refs: list[DocRef] = []
    for page_num, slug_suffix, label in PIO_X_PAGES:
        refs.append(DocRef(
            url=f"{PIO_X_BASE}{page_num:02d}text.htm",
            target_dir=target_dir,
            slug=f"1908_catechismo-pio-x_{slug_suffix}",
            lang="it",
            # Legacy Microsoft FrontPage pages use layout tables + <font>
            # wrappers that pandoc otherwise collapses to "[TABLE]" tokens.
            unwrap_tags=[
                "table", "tbody", "tr", "td", "th",
                "font", "center", "map", "area",
            ],
            meta_hints=_pio_x_hints(label),
        ))
    return refs


# -- MISSING stubs: writer -------------------------------------------------

def write_missing_stubs() -> int:
    n = 0
    for dir_path, filename, body in MISSING_NOTES:
        dir_path.mkdir(parents=True, exist_ok=True)
        target = dir_path / filename
        if target.exists():
            continue
        target.write_text(body, encoding="utf-8")
        n += 1
    return n


# -- main ------------------------------------------------------------------

async def main() -> int:
    refresh = os.environ.get("REFRESH") == "1"

    # 1. MISSING stubs (idempotent)
    n_missing = write_missing_stubs()
    print(f"Phase 7 — stubs MISSING écrits : {n_missing}")

    # 2. Discover dynamic refs (CCC FR, CCC LT)
    ccc_fr_refs = await discover_ccc_fr()
    print(f"  CCC 1992 FR : {len(ccc_fr_refs)} pages")
    ccc_lt_refs = await discover_ccc_lt()
    print(f"  CCC 1997 LT : {len(ccc_lt_refs)} pages")

    # 3. Static refs
    compendium_refs = build_compendium_refs()
    baltimore_refs = build_baltimore_refs()
    pio_x_refs = build_pio_x_refs()
    print(f"  Compendium 2005 : {len(compendium_refs)} doc")
    print(f"  Baltimore 1885  : {len(baltimore_refs)} docs")
    print(f"  Pie X 1908      : {len(pio_x_refs)} docs")

    all_refs = (
        ccc_fr_refs + ccc_lt_refs + compendium_refs + baltimore_refs
        + pio_x_refs
    )
    print(f"Phase 7 — total {len(all_refs)} pages à récupérer")
    if not all_refs:
        return 0

    result = await run_pipeline(
        all_refs, phase=PHASE, refresh=refresh, concurrency=8,
    )
    print(
        f"Phase 7 done: ok={result.n_ok} skipped={result.n_skipped} "
        f"errors={result.n_errors}"
    )
    return 0 if result.n_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
