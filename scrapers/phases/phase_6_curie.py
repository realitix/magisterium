"""Phase 6 — Roman Curia documents, pre- and post-Vatican II.

Scope (priority 1) : DDF / ex-CDF / ex-Saint-Office (cfaith). The comprehensive
list is published at :
    https://www.vatican.va/roman_curia/congregations/cfaith/doc_doc_index.htm
This is the single source of truth for ~237 numbered doctrinal documents
issued between 1962 (*Instructio de modo procedendi*, the oldest item in the
online index) and today. Documents dated on or before 1958-10-09 (death of
Pius XII, conventional end of the pre-Vatican-II era adopted elsewhere in
this repo) are filed under the pre-V2 saint-office folder; later documents
go under the post-V2 DDF folder.

Priority 2 : Congregation for Divine Worship (ccdds), Congregation for the
Clergy (cclergy), Congregation for the Evangelization of Peoples / ex-
Propaganda Fide (cevang). These don't have a unified doc-index page; we
scrape their landing `index.htm` (and, for cclergy, the thematic Italian
sub-indexes) and pick the language by preference.

Language preference : Latin first (codes `la` or the legacy `lt` used for
pre-1980 vatican.va documents), then Italian, French, English. Since the
task specifies "source language only", we prefer the editio typica in latin
when it exists (typical for solemn declarations); otherwise Italian
(operational language of most dicastery notes).

Rate-limit : 1 req / 2s on vatican.va, handled by GLOBAL_LIMITER. Idempotent
via the pipeline's meta-exists check.

Control :
    PHASE6_DICASTERES=ddf,culte-divin,clerge,propaganda
Values map to {ddf, culte-divin, clerge, propaganda}. Default : all four.
"""
from __future__ import annotations

import asyncio
import os
import re
import sys
from dataclasses import dataclass
from datetime import date
from typing import Optional

import httpx

from scrapers.core.errors import log_error
from scrapers.core.fetcher import USER_AGENT
from scrapers.core.pipeline import DocRef, MAGISTERIUM_ROOT, run_pipeline
from scrapers.core.rate_limit import GLOBAL_LIMITER

PHASE = "phase-6-curie"
VATICAN_DOMAIN = "www.vatican.va"
VATICAN_BASE = "https://www.vatican.va"

# End-of-pre-V2 boundary: death of Pius XII. Documents issued on or before
# this date are filed under the pre-V2 "saint-office" folder.
PRE_V2_BOUNDARY = date(1958, 10, 9)

# Language preference on vatican.va. `lt` is a legacy latin code used on
# pre-~2000 documents (alongside `la`). We keep both at the top of the list.
LANG_ORDER = ("la", "lt", "it", "fr", "en", "sp", "ge", "po", "pl")
# Canonical tag for latin legacy-lang (`lt` → `la` in meta).
LANG_CANONICAL = {"lt": "la", "po": "pt"}


@dataclass(frozen=True)
class Dicaster:
    key: str                       # env-var key
    slug: str                      # URL slug under /roman_curia/congregations/
    auteur_post_v2: str            # value of meta.auteur for post-V2 docs
    auteur_pre_v2: str             # value of meta.auteur for pre-V2 docs
    folder_pre_v2: str             # subfolder under A-pre-vatican-ii/curie-romaine
    folder_post_v2: str            # subfolder under C-post-vatican-ii/curie-romaine
    # How to discover: "doc_doc_index" uses the unified doc_doc_index.htm page,
    # "landing" scrapes the main index.htm, "cclergy" additionally walks
    # Italian thematic sub-indexes.
    discover_mode: str
    extra_indexes: tuple[str, ...] = ()


DICASTERES: list[Dicaster] = [
    Dicaster(
        key="ddf",
        slug="cfaith",
        auteur_post_v2="Dicastère pour la doctrine de la foi",
        auteur_pre_v2="Saint-Office",
        folder_pre_v2="saint-office",
        folder_post_v2="ddf-ex-cdf",
        discover_mode="doc_doc_index",
    ),
    Dicaster(
        key="culte-divin",
        slug="ccdds",
        auteur_post_v2="Congrégation pour le culte divin et la discipline des sacrements",
        auteur_pre_v2="Congrégation des rites",
        folder_pre_v2="rites",
        folder_post_v2="culte-divin",
        discover_mode="landing",
    ),
    Dicaster(
        key="clerge",
        slug="cclergy",
        auteur_post_v2="Congrégation pour le clergé",
        auteur_pre_v2="Congrégation du concile",
        folder_pre_v2="autres",
        folder_post_v2="clerge",
        discover_mode="cclergy",
        extra_indexes=(
            "index_it_doc_ufficiali_presbiteri.htm",
            "index_it_diac_docuff.htm",
            "index_it_giub_prebiteri.htm",
            "index_it_giub_diaconi.htm",
            "index_it_pres_ainterv.htm",
        ),
    ),
    Dicaster(
        key="propaganda",
        slug="cevang",
        auteur_post_v2="Congrégation pour l'évangélisation des peuples",
        auteur_pre_v2="Sacrée Congrégation de la Propagande",
        folder_pre_v2="propaganda-fide",
        folder_post_v2="autres-dicasteres",
        discover_mode="landing",
    ),
]
DICASTERES_BY_KEY = {d.key: d for d in DICASTERES}


# --- HTTP helpers ---------------------------------------------------------

async def _http_get_text(url: str) -> Optional[str]:
    """Rate-limited GET returning text, or None on non-200 / network error."""
    await GLOBAL_LIMITER.acquire(VATICAN_DOMAIN)
    try:
        async with httpx.AsyncClient(
            http2=True, follow_redirects=True, timeout=60.0,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "la,it,fr,en;q=0.8"},
        ) as client:
            r = await client.get(url)
        if r.status_code != 200:
            return None
        return r.text
    except Exception as e:  # noqa: BLE001
        log_error(
            source=VATICAN_DOMAIN, url=url, phase=PHASE,
            message=f"discover-fetch: {type(e).__name__}: {e}",
        )
        return None


# --- Filename parsing ------------------------------------------------------

# Matches e.g.
#   documents/rc_con_cfaith_doc_19660318_istr-matrimoni-misti_it.html
#   documents/rc_ddf_doc_20240705_decreto-opus-angelorum_la.html
#   documents/rc_cdf_doc_20200624_responsum-nota-battesimo_en.html
#   documents/rc_con_ccdds_doc_20230220_rescriptum_en.html
# Pattern breakdown:
#   prefix = rc_{something}_{doctoken}  (something ∈ con_cfaith, cdf, ddf, con_ccdds, ...)
#   doctoken = doc | pro | dpc | etc.
#   date = 8 digits
#   slug = ...
#   lang = 2 letters
FILENAME_RE = re.compile(
    r"(?P<prefix>rc_[a-z_]+)_(?P<doctoken>[a-z]+)_"
    r"(?P<date>\d{8})_(?P<slug>[a-z0-9\-]+)_(?P<lang>[a-z]{2})\.html$"
)

# Match any href that ends with `documents/<leaf>` where <leaf> is the
# standardized Roman Curia filename. We don't care about the scheme / prefix.
HREF_DOC_RE = re.compile(
    r'href="(?P<href>[^"]*?documents/'
    r'(?P<leaf>rc_[a-z_]+_[a-z]+_\d{8}_[a-z0-9\-]+_[a-z]{2}\.html))"',
    re.IGNORECASE,
)


def _try_parse_date(ds: str) -> Optional[date]:
    """Parse an 8-digit date that may be YYYYMMDD (modern) or DDMMYYYY (older)."""
    if len(ds) != 8 or not ds.isdigit():
        return None
    # Try YYYYMMDD first when the leading 4 digits look plausible as a year.
    if ds[:2] in ("19", "20", "21"):
        try:
            return date(int(ds[0:4]), int(ds[4:6]), int(ds[6:8]))
        except ValueError:
            pass
    try:
        return date(int(ds[4:8]), int(ds[2:4]), int(ds[0:2]))
    except ValueError:
        return None


@dataclass(frozen=True)
class DocEntry:
    """One document's base identity (date + slug), with all languages seen."""
    prefix: str                    # e.g. "rc_con_cfaith_doc"
    doctoken: str                  # "doc", "pro", "dpc"
    date: date
    slug: str                      # incipit-like slug from filename
    langs: dict[str, str]          # lang-code → absolute URL

    @property
    def stem(self) -> str:
        """Stable identity key: prefix+doctoken+date+slug (no lang)."""
        return f"{self.prefix}_{self.doctoken}_{self.date.strftime('%Y%m%d')}_{self.slug}"


# --- Discovery ------------------------------------------------------------

async def _discover_from_index(index_url: str, congr_slug: str) -> dict[str, DocEntry]:
    """Fetch `index_url`, return {stem: DocEntry} grouping by (prefix,date,slug)."""
    text = await _http_get_text(index_url)
    if not text:
        return {}
    return _parse_entries(text, congr_slug)


def _parse_entries(text: str, congr_slug: str) -> dict[str, DocEntry]:
    """Extract all document links from an HTML page, grouped by document identity."""
    entries: dict[str, DocEntry] = {}
    for m in HREF_DOC_RE.finditer(text):
        leaf = m.group("leaf")
        fm = FILENAME_RE.search(leaf)
        if not fm:
            continue
        prefix = fm.group("prefix")
        doctoken = fm.group("doctoken")
        ds = fm.group("date")
        slug = fm.group("slug")
        raw_lang = fm.group("lang")
        d = _try_parse_date(ds)
        if d is None:
            continue
        # Sanity bound: exclude nonsense dates (URL mis-parse).
        if d.year < 1850 or d.year > 2100:
            continue
        lang = LANG_CANONICAL.get(raw_lang, raw_lang)
        # Build the canonical absolute URL — always on vatican.va
        # under /roman_curia/congregations/{slug}/documents/{leaf}
        url = (
            f"{VATICAN_BASE}/roman_curia/congregations/{congr_slug}"
            f"/documents/{leaf}"
        )
        # Dedup key uses the *canonical* ISO date + slug, so two URL variants
        # of the same document (e.g. .../doc_14091994_foo vs .../doc_19940914_foo,
        # both pointing to the same file — vatican.va serves aliases for some
        # older documents) collapse to one entry.
        key = f"{prefix}_{doctoken}_{d.isoformat()}_{slug}"
        entry = entries.get(key)
        if entry is None:
            entry = DocEntry(prefix=prefix, doctoken=doctoken, date=d,
                             slug=slug, langs={})
            entries[key] = entry
        # Prefer the first-seen URL for a given lang (they should all be equal)
        # but canonicalize to the slug-based path above for stability.
        entry.langs.setdefault(lang, url)
    return entries


async def _discover_ddf() -> dict[str, DocEntry]:
    """DDF uses the comprehensive doc_doc_index.htm."""
    index_url = f"{VATICAN_BASE}/roman_curia/congregations/cfaith/doc_doc_index.htm"
    return await _discover_from_index(index_url, "cfaith")


async def _discover_landing(dic: Dicaster) -> dict[str, DocEntry]:
    index_url = f"{VATICAN_BASE}/roman_curia/congregations/{dic.slug}/index.htm"
    return await _discover_from_index(index_url, dic.slug)


async def _discover_cclergy(dic: Dicaster) -> dict[str, DocEntry]:
    """cclergy landing + a few italian thematic sub-indexes."""
    all_entries: dict[str, DocEntry] = {}
    # Main landing page
    landing = await _discover_landing(dic)
    all_entries.update(landing)
    # Extra thematic indexes (italian side has the most complete lists)
    for sub in dic.extra_indexes:
        url = f"{VATICAN_BASE}/roman_curia/congregations/{dic.slug}/{sub}"
        extra = await _discover_from_index(url, dic.slug)
        for k, v in extra.items():
            if k in all_entries:
                # Merge language sets (some documents appear on multiple indexes).
                merged = {**v.langs, **all_entries[k].langs}
                all_entries[k] = DocEntry(
                    prefix=v.prefix, doctoken=v.doctoken, date=v.date,
                    slug=v.slug, langs=merged,
                )
            else:
                all_entries[k] = v
    return all_entries


# --- Classification helpers -----------------------------------------------

# Regex-based classifiers: infer document *type* from the filename slug and
# doctoken. These are best-effort; the raw meta.type is the most specific tag
# we can produce without fetching each document.
# Order matters: first-match wins.
TYPE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"instruct", re.I), "instruction"),
    (re.compile(r"(declarat|dichiarazione|dichiaraz)", re.I), "declaration"),
    (re.compile(r"(notific|notifica)", re.I), "notification"),
    (re.compile(r"(decret|decreto)", re.I), "decret"),
    (re.compile(r"(rescript|rescritto)", re.I), "rescrit"),
    (re.compile(r"(respons|responsa|dubia|dubio|dubium)", re.I), "responsum"),
    (re.compile(r"(normae|norme)", re.I), "normes"),
    (re.compile(r"(epist|lettera|lettre|letter|litterae)", re.I), "lettre"),
    (re.compile(r"(motu-?proprio)", re.I), "motu-proprio"),
    (re.compile(r"(discors|discorso|discours)", re.I), "discours"),
    (re.compile(r"(direttor|directoire|directory)", re.I), "directoire"),
    (re.compile(r"(profession|professio)", re.I), "profession-de-foi"),
    (re.compile(r"(catech|catechism)", re.I), "catechisme"),
    (re.compile(r"(convention|accord)", re.I), "convention"),
    (re.compile(r"(formula|ratio|agenda)", re.I), "instruction"),
]


def _infer_type(slug: str, doctoken: str) -> str:
    for pat, t in TYPE_PATTERNS:
        if pat.search(slug):
            return t
    if doctoken == "pro":
        return "profil-institutionnel"
    if doctoken == "dpc":
        return "document"
    return "document"


def _pick_lang(entry: DocEntry) -> str:
    """Choose the best language for a DocEntry per LANG_ORDER."""
    canonical_langs = set(entry.langs.keys())
    for lg in LANG_ORDER:
        # LANG_ORDER still contains the legacy `lt`; but entry.langs is
        # already canonicalized (`lt`→`la`, `po`→`pt`). Only test canonical.
        canonical = LANG_CANONICAL.get(lg, lg)
        if canonical in canonical_langs:
            return canonical
    return next(iter(canonical_langs))


def _incipit_from_slug(slug: str) -> str:
    # Replace dashes, title-case first word. This is a heuristic; the
    # true incipit often requires reading the document.
    words = slug.replace("_", "-").split("-")
    # Drop generic leading tokens.
    drops = {
        "instructio", "instruction", "decreto", "decretum", "declaratio",
        "dichiarazione", "notificazione", "notification", "lettera", "lettre",
        "letter", "nota", "responsum", "responsa", "rescriptum", "rescritto",
        "norme", "normae", "formula", "ratio", "motu-proprio",
    }
    while words and words[0].lower() in drops:
        words.pop(0)
    if not words:
        words = slug.split("-")
    return " ".join(words).strip().capitalize()


# --- DocRef construction --------------------------------------------------

def _build_ref(dic: Dicaster, entry: DocEntry) -> DocRef:
    is_pre_v2 = entry.date <= PRE_V2_BOUNDARY
    if is_pre_v2:
        target_dir = (
            MAGISTERIUM_ROOT / "A-pre-vatican-ii" / "curie-romaine"
            / dic.folder_pre_v2
        )
        auteur = dic.auteur_pre_v2
        periode = "pre-vatican-ii"
    else:
        target_dir = (
            MAGISTERIUM_ROOT / "C-post-vatican-ii" / "curie-romaine"
            / dic.folder_post_v2
        )
        # For DDF, use the institutional name in force at the date of issue:
        #  - Holy Office until 1965-12-07
        #  - CDF from 1965-12-07 to 2022-03-05
        #  - DDF from 2022-03-05 (Praedicate Evangelium)
        if dic.key == "ddf":
            if entry.date < date(1965, 12, 7):
                auteur = "Saint-Office"
            elif entry.date < date(2022, 3, 5):
                auteur = "Congrégation pour la doctrine de la foi"
            else:
                auteur = "Dicastère pour la doctrine de la foi"
        else:
            auteur = dic.auteur_post_v2
        periode = "post-vatican-ii"

    lang = _pick_lang(entry)
    url = entry.langs[lang]

    doc_type = _infer_type(entry.slug, entry.doctoken)
    slug = f"{entry.date.isoformat()}_{entry.slug}_{doc_type[:8]}"
    incipit = _incipit_from_slug(entry.slug)

    return DocRef(
        url=url,
        target_dir=target_dir,
        slug=slug,
        lang=lang,
        meta_hints={
            "incipit": incipit,
            "titre_fr": None,
            "auteur": auteur,
            "periode": periode,
            "type": doc_type,
            "date": entry.date,
            # Curial documents are ordinary magisterium unless explicitly
            # confirmed/approved by the pope "in forma specifica". We tag all
            # as magistere-ordinaire; a later enrichment pass can upgrade
            # individual documents after reading the text.
            "autorite_magisterielle": "magistere-ordinaire",
            "langue_originale": lang,
            "langues_disponibles": sorted(entry.langs.keys()),
            "sujets": [],
        },
    )


async def discover(dic: Dicaster) -> list[DocRef]:
    mode = dic.discover_mode
    if mode == "doc_doc_index":
        entries = await _discover_ddf()
    elif mode == "cclergy":
        entries = await _discover_cclergy(dic)
    else:
        entries = await _discover_landing(dic)

    return [_build_ref(dic, e) for e in entries.values()]


# --- main -----------------------------------------------------------------

def _selected_dicasteries() -> list[Dicaster]:
    env = os.environ.get("PHASE6_DICASTERES", "").strip()
    if not env or env.lower() == "all":
        return list(DICASTERES)
    keys = [k.strip() for k in env.split(",") if k.strip()]
    out: list[Dicaster] = []
    for k in keys:
        if k in DICASTERES_BY_KEY:
            out.append(DICASTERES_BY_KEY[k])
        else:
            print(
                f"warning: unknown dicaster key '{k}' "
                f"(known: {list(DICASTERES_BY_KEY)})",
                file=sys.stderr,
            )
    return out or list(DICASTERES)


def _report_breakdown(dic_key: str, refs: list[DocRef]) -> None:
    if not refs:
        print(f"  {dic_key}: 0 documents")
        return
    pre = [r for r in refs if r.meta_hints.get("periode") == "pre-vatican-ii"]
    post = [r for r in refs if r.meta_hints.get("periode") == "post-vatican-ii"]
    lang_count: dict[str, int] = {}
    for r in refs:
        lang_count[r.lang] = lang_count.get(r.lang, 0) + 1
    print(
        f"  {dic_key}: total={len(refs)} pre-V2={len(pre)} post-V2={len(post)} "
        f"langs={dict(sorted(lang_count.items(), key=lambda kv: -kv[1]))}"
    )


async def main() -> int:
    refresh = os.environ.get("REFRESH") == "1"
    dicasteries = _selected_dicasteries()
    print(f"Phase 6 — dicastères: {[d.key for d in dicasteries]}")

    all_refs: list[DocRef] = []
    for dic in dicasteries:
        refs = await discover(dic)
        _report_breakdown(dic.key, refs)
        all_refs.extend(refs)

    print(f"Phase 6 — total {len(all_refs)} documents to fetch")
    if not all_refs:
        return 0

    # vatican.va rate-limits to 1 req/2s. Keep moderate concurrency for
    # pipeline overhead overlap only.
    result = await run_pipeline(
        all_refs, phase=PHASE, refresh=refresh, concurrency=8,
    )
    print(
        f"Phase 6 done: ok={result.n_ok} skipped={result.n_skipped} "
        f"errors={result.n_errors}"
    )
    return 0 if result.n_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
