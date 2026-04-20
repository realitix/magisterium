"""Phase 7 — Official catechisms, pre- and post-Vatican II.

Inventory
---------

Pre-V2  (magisterium/A-pre-vatican-ii/catechismes/)
  1. 1566 Catéchisme du concile de Trente (Catechismus Romanus) .. MISSING
  2. 1555 Catéchisme de Canisius                                 .. MISSING
  3. 1598 Catéchisme de Bellarmin                                .. MISSING
  4. 1885 Catéchisme de Baltimore (4 vols, Project Gutenberg)    .. scraped
  5. 1908 Catéchisme de Pie X                                    .. MISSING

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
    (
        PRE_V2_DIR / "1566-romain-trente",
        "1566_catechismus-romanus.MISSING.md",
        """# Catéchisme du concile de Trente (1566) — MANQUANT

**Autres noms :** Catechismus Romanus, Catechismus ad Parochos,
Catéchisme de Pie V.

## Pourquoi absent de la V1 du corpus

Aucune source en ligne stable ne sert le texte latin intégral avec une
arborescence d'URLs prévisible. Tentatives effectuées :

- `documentacatholicaomnia.eu` : pas d'entrée `Catechismus_Romanus` sous
  les patterns 30_10_, 04z_, 20vs, ni sous l'index alphabétique (tous
  404 lors de la Phase 7).
- `la.wikisource.org/wiki/Catechismus_Romanus` : page inexistante au
  20-04-2026.
- `archive.org` : plusieurs éditions historiques (1796, 1804, 1830, 1866)
  mais en tant que PDFs océrisés ou DJVU non segmentés — pas adapté au
  pipeline HTML→markdown.
- `intratext.com` : la recherche ne retourne pas de texte segmenté.

## Pistes pour une V2

1. Scraper une édition `archive.org` en PDF (p. ex.
   <https://archive.org/details/bub_gb_Oow_AAAAcAAJ>) puis OCR + split en
   partie I/II/III/IV.
2. Récupérer la version FR de `salve-regina.com` (SSL foiré, déjà dans
   `INSECURE_DOMAINS`) — mais c'est une traduction française, donc
   sort de la règle « langue source ».
3. Prendre la version anglophone de `catholicprimer.org` ou l'édition
   McHugh/Callan 1923 sur `archive.org` (traduction EN).
""",
    ),
    (
        PRE_V2_DIR / "1555-canisius",
        "1555_summa-doctrinae.MISSING.md",
        """# Catéchisme de Canisius (1555) — MANQUANT

**Œuvre de référence :** *Summa doctrinae christianae*, saint Pierre
Canisius SJ (1521-1597), édition princeps Vienne 1555. Deux versions
plus courtes suivront : *Catechismus Minor* (1556) et *Catechismus
Parvus Catholicorum* (1558).

## Absent de la V1 car

- Pas d'édition HTML en ligne en latin repérée sur DCO, Wikisource latin
  ou intratext.com (recherches effectuées en Phase 7).
- Les éditions historiques existent sur `archive.org` (PDFs facsimilés)
  mais ne sont pas structurées pour un pipeline HTML.

## V2

- Chercher sur `books.google.com` une édition numérisée avec OCR lisible.
- Alternative : récupérer la traduction anglaise moderne publiée par TAN
  Books (sous droits) — donc à écarter.
""",
    ),
    (
        PRE_V2_DIR / "1598-bellarmin",
        "1598_dottrina-cristiana.MISSING.md",
        """# Catéchisme de Bellarmin (1598) — MANQUANT

**Œuvres :**
- *Dottrina cristiana breve* (1597, pour les enfants)
- *Dichiarazione più copiosa della dottrina cristiana* (1598, pour les
  catéchistes)

## Absent de la V1 car

Aucun site public ne sert le texte italien intégral dans une arborescence
stable adaptée au scraping HTML. Plusieurs numérisations `archive.org`
existent (PDF) mais ne sont pas segmentées.

## V2

- `google books` : éditions italiennes XVIIe siècle.
- `archive.org/details/…` PDFs à OCR + segmenter.
""",
    ),
    (
        PRE_V2_DIR / "1908-pie-x",
        "1908_catechismo-pio-x.MISSING.md",
        """# Catéchisme de Pie X (1908) — MANQUANT

**Nom officiel :** *Catechismo della Dottrina Cristiana*, Pie X, 1908
(remanié 1912, "Catechismo Maggiore di San Pio X"). Langue originale :
italien.

## Absent de la V1 car

- `vatican.va` ne sert PAS le Catechismo di Pio X en HTML (la Santa Sede
  publie le CCC de 1992 et son Compendium de 2005, pas le texte de 1908).
- `maranatha.it/Catechismo/PioX/*` : 404 sur tous les patterns testés.
- `cristianicattolici.net`, `credereoggi.it`, `catechesistradizionale.com` :
  respectivement 404, cert SSL invalide, ECONNREFUSED.
- `salve-regina.com` contient une version FR mais (a) traduction, (b) SSL
  obsolète, (c) sans arborescence MediaWiki fiable côté `title=…`.

## V2

- Trouver un mirror italien stable (forums tradi, Una Voce Italia).
- Scraper la traduction FR sur `salve-regina.com` si on accepte une
  exception à la règle « langue source ».
- Ingérer un PDF `archive.org` + OCR.
""",
    ),
    (
        POST_V2_DIR / "2011-youcat",
        "2011_youcat.MISSING.md",
        """# YOUCAT (2011) — MANQUANT (DROITS)

Le *Jugendkatechismus der Katholischen Kirche* (YOUCAT, 2011) n'est pas
publié librement en ligne. Éditeur : Pattloch Verlag / Ignatius Press.
Sous copyright actif ; aucune édition texte intégral libre.

La V1 du corpus ne l'inclut donc pas. Toute V2 nécessiterait :
- une licence des ayants droit, ou
- un extrait limité (fair use), ce qui est hors scope du projet.
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
# hrefs are bare *_lt.htm filenames (p1s1c1_lt.htm, p122a3p1_lt.htm, etc.).
# The source uses unquoted attributes; accept quoted or bare, reject absolute
# URLs (we only want local files).
CCC_LT_HREF_RE = re.compile(
    r'(?i)href=(?:"|\')?([a-z0-9_\-]+_lt\.htm)(?:"|\')?[\s>]',
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
#   IT: archive_2005_compendium-ccc_it.html  → 200 OK
#   LA: archive_2005_compendium-ccc_la.html  → 404 (not published as HTML)
#   EN: archive_2005_compendium-ccc_en.html  → 200 OK
# The Compendium's editio typica is Latin, but the Holy See only published
# it as a PDF. We therefore archive the Italian page (first diffused
# electronically) as langue_originale="it" and note in meta that la is the
# true editio typica. (If the la HTML surfaces later, add it here.)
COMPENDIUM_BASE = (
    "https://www.vatican.va/archive/compendium_ccc/documents/"
)


def build_compendium_refs() -> list[DocRef]:
    target_dir = POST_V2_DIR / "2005-compendium"
    hints = {
        "incipit": "Compendium Catechismi Catholicae Ecclesiae",
        "titre_fr": "Compendium du Catéchisme de l'Église catholique (2005)",
        "auteur": "Benoît XVI",
        "periode": "post-vatican-ii",
        "type": "catechisme",
        "date": date(2005, 6, 28),
        "autorite_magisterielle": "magistere-ordinaire-universel",
        "langue_originale": "la",
        "langues_disponibles": ["it"],  # HTML disponible uniquement en IT/FR/EN
        "sujets": ["catechisme", "compendium"],
    }
    # Italian is closest to the editio typica latina among the available
    # HTML variants; langue_originale stays "la" per the printed edition.
    return [
        DocRef(
            url=COMPENDIUM_BASE + "archive_2005_compendium-ccc_it.html",
            target_dir=target_dir,
            slug="2005-06-28_compendium-ccc",
            lang="it",
            unwrap_tags=["table", "tbody", "tr", "td", "th", "font", "center"],
            meta_hints=dict(hints),
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
    print(f"  Compendium 2005 : {len(compendium_refs)} doc")
    print(f"  Baltimore 1885  : {len(baltimore_refs)} docs")

    all_refs = (
        ccc_fr_refs + ccc_lt_refs + compendium_refs + baltimore_refs
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
