"""Phase 3 — solemn magisterial documents of pre-Vatican II popes (1846-1958).

Scope : Pie IX → Pie XII. Source: vatican.va. Discovery scrapes each pope's
per-type index (or landing page for Pie IX whose sub-indices don't exist),
extracts href patterns, then for each document picks a language variant in
order `/la/` → `/it/` → `/fr/` → `/en/` to honor the "original language first"
rule.

Probing strategy : the domain rate-limit (1 req/2s) makes probing all 4
variants per doc prohibitively slow. For popes Leo XIII → Pie XII we
therefore assume `/la/` exists (empirically true for essentially every
document) and fetch it directly. A second pass can recover any 404s in
italian afterwards by reading the errors log. For Pie IX, the legacy URL
pattern encodes the language in the filename prefix itself (`encyclica-`
= la, `enciclica-` = it, etc.), so we pick the best language per document
by grouping duplicate entries from the pope's landing page.

By default only encyclicals are scraped. Other types (apostolic constitutions,
motu proprio, apostolic letters, apostolic exhortations) can be enabled via
PHASE3_TYPES environment variable (comma-separated list of keys from
DOCTYPES below, or "all"). See main().

Naming :
    slug = YYYY-MM-DD_{incipit-slug}_{type-short}
    target_dir = magisterium/A-pre-vatican-ii/papes/{folder}/{type-plural}

The pipeline (scrapers.core.pipeline) is used unchanged.
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

PHASE = "phase-3-papes-pre-v2"
VATICAN_DOMAIN = "www.vatican.va"
VATICAN_BASE = "https://www.vatican.va"

# Candidate language order: original language (la) first, then it/fr/en.
LANG_ORDER = ["la", "it", "fr", "en"]


@dataclass(frozen=True)
class Pope:
    slug: str               # URL slug on vatican.va
    folder: str             # local folder under papes/
    auteur: str             # human name for meta.auteur


POPES: list[Pope] = [
    Pope("pius-ix",     "1846-pie-ix",     "Pie IX"),
    Pope("leo-xiii",    "1878-leon-xiii",  "Léon XIII"),
    Pope("pius-x",      "1903-pie-x",      "Pie X"),
    Pope("benedict-xv", "1914-benoit-xv",  "Benoît XV"),
    Pope("pius-xi",     "1922-pie-xi",     "Pie XI"),
    Pope("pius-xii",    "1939-pie-xii",    "Pie XII"),
]


@dataclass(frozen=True)
class DocType:
    key: str                # short key used by PHASE3_TYPES env var
    vatican_path: str       # path segment on vatican.va (e.g. "encyclicals")
    folder: str             # local folder name under the pope
    type_meta: str          # value of meta.type
    type_short: str         # suffix used in slug
    # Filename token used in standard hf_* pattern (e.g. "enc", "apc", "mp",
    # "apl", "exh"). For Pie IX (legacy non-hf pattern) this is also the
    # prefix we match in the filename.
    hf_tokens: tuple[str, ...]
    # Regex pattern on filename to identify Pie IX legacy docs of this type.
    # Leading slash included; evaluated against the leaf filename only.
    legacy_prefixes: tuple[str, ...]


DOCTYPES: list[DocType] = [
    DocType(
        key="encyclicals",
        vatican_path="encyclicals",
        folder="encycliques",
        type_meta="encyclique",
        type_short="enc",
        hf_tokens=("enc",),
        # Pie IX encyclical prefixes: Italian "enciclica-", Latin "encyclica-",
        # also "epistola-encyclica-" used on a few late-period docs.
        legacy_prefixes=("enciclica-", "encyclica-", "epistola-encyclica-"),
    ),
    DocType(
        key="apost_constitutions",
        vatican_path="apost_constitutions",
        folder="constitutions-apostoliques",
        type_meta="constitution-apostolique",
        type_short="const",
        hf_tokens=("apc", "apl"),  # Varies; we filter by index not by token
        legacy_prefixes=("constitutio-", "bolla-", "bulla-", "costituzione-"),
    ),
    DocType(
        key="motu_proprio",
        vatican_path="motu_proprio",
        folder="motu-proprio",
        type_meta="motu-proprio",
        type_short="mp",
        hf_tokens=("motu-proprio", "mp"),
        legacy_prefixes=("motu-proprio-",),
    ),
    DocType(
        key="apost_letters",
        vatican_path="apost_letters",
        folder="lettres-apostoliques",
        type_meta="lettre-apostolique",
        type_short="apl",
        hf_tokens=("apl",),
        # NOTE: when enabling apost_letters for Pie IX, beware that
        # "epistola-" also matches "epistola-encyclica-" (an encyclical). If
        # scraping both types at once, run encyclicals first and dedupe by
        # leaf. For now only encyclicals are scraped by default.
        legacy_prefixes=("litterae-apostolicae-", "lettera-apostolica-", "epistola-"),
    ),
    DocType(
        key="apost_exhortations",
        vatican_path="apost_exhortations",
        folder="exhortations-apostoliques",
        type_meta="exhortation-apostolique",
        type_short="exh",
        hf_tokens=("exh",),
        legacy_prefixes=("esortazione-apostolica-", "exhortatio-apostolica-"),
    ),
]
DOCTYPES_BY_KEY: dict[str, DocType] = {dt.key: dt for dt in DOCTYPES}


# --- Index discovery ------------------------------------------------------

HREF_RE = re.compile(
    r'href="(/content/(?P<popeslug>[a-z\-]+)/(?P<lang>[a-z]{2})'
    r'/(?P<section>[a-z_]+)/documents/(?P<leaf>hf_[^"]+\.html))"'
)
# Pie IX: legacy links don't go through /section/documents but
# /content/pius-ix/{lang}/documents/{filename}.html
PIUS_IX_HREF_RE = re.compile(
    r'href="(/content/pius-ix/(?P<lang>[a-z]{2})/documents/'
    r'(?P<leaf>[a-z0-9\-]+\.html))"'
)

# Standard hf_ filename : hf_{pope}_{type}_{DDMMYYYY}_{slug}.html
HF_FILENAME_RE = re.compile(
    r"^hf_(?P<pope>[a-z0-9\-]+)_(?P<type>[a-z]+)_"
    r"(?P<date>\d{8})_(?P<slug>[a-z0-9\-]+)\.html$"
)

# Pie IX legacy filename :
# {prefix}-{slug}-{day}-{month-name-or-number}-{yyyy}.html
# Month names in italian (gennaio..dicembre) AND latin (januarii..decembris,
# possibly with or without ending 's'/'ii').
IT_MONTHS = {
    "gennaio": 1, "febbraio": 2, "marzo": 3, "aprile": 4, "maggio": 5,
    "giugno": 6, "luglio": 7, "agosto": 8, "settembre": 9, "ottobre": 10,
    "novembre": 11, "dicembre": 12,
}
LA_MONTHS = {
    "ianuarii": 1, "januarii": 1, "februarii": 2, "martii": 3, "aprilis": 4,
    "maii": 5, "iunii": 6, "junii": 6, "iulii": 7, "julii": 7, "augusti": 8,
    "septembris": 9, "octobris": 10, "novembris": 11, "decembris": 12,
}
FR_MONTHS = {
    "janvier": 1, "fevrier": 2, "février": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "aout": 8, "août": 8, "septembre": 9,
    "octobre": 10, "novembre": 11, "decembre": 12, "décembre": 12,
}
ALL_MONTHS = {**IT_MONTHS, **LA_MONTHS, **FR_MONTHS}


async def _http_get_text(url: str) -> Optional[str]:
    """Rate-limited plain GET returning decoded text, or None on non-200."""
    await GLOBAL_LIMITER.acquire(VATICAN_DOMAIN)
    try:
        async with httpx.AsyncClient(
            http2=True, follow_redirects=True, timeout=60.0,
            headers={"User-Agent": USER_AGENT, "Accept-Language": "la,fr,en,it;q=0.8"},
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


def _parse_hf_filename(leaf: str) -> Optional[tuple[date, str]]:
    m = HF_FILENAME_RE.match(leaf)
    if not m:
        return None
    ds = m.group("date")  # 8 digits, either DDMMYYYY (most popes) or
    # YYYYMMDD (used for Pius XI onwards, partially).
    d = _try_parse_date(ds)
    if d is None:
        return None
    return d, m.group("slug")


def _try_parse_date(ds: str) -> Optional[date]:
    """Parse an 8-digit date that may be DDMMYYYY or YYYYMMDD."""
    if len(ds) != 8 or not ds.isdigit():
        return None
    # Try YYYYMMDD first when the leading 4 digits look like a plausible year
    # in the pre-Vatican II era (1846-1958). Fall back to DDMMYYYY otherwise.
    if ds.startswith(("18", "19", "20")):
        try:
            return date(int(ds[0:4]), int(ds[4:6]), int(ds[6:8]))
        except ValueError:
            pass
    try:
        return date(int(ds[4:8]), int(ds[2:4]), int(ds[0:2]))
    except ValueError:
        return None


def _parse_pius_ix_filename(leaf: str, prefixes: tuple[str, ...]) -> Optional[tuple[date, str, str]]:
    """Return (date, incipit_slug, matched_prefix) or None.

    Format examples :
        enciclica-qui-pluribus-9-novembre-1846.html
        encyclica-quanta-cura-8-decembris-1864.html
        epistola-encyclica-gravibus-ecclesiae-24-decembris-1874.html
        litterae-apostolicae-graves-ac-diuturnae-23-martii-1875.html
    """
    name = leaf[:-5] if leaf.endswith(".html") else leaf
    matched = None
    for pref in prefixes:
        if name.startswith(pref):
            matched = pref
            break
    if matched is None:
        return None
    rest = name[len(matched):]
    parts = rest.rsplit("-", 3)
    # Expect at least [slug, DD, month, YYYY]
    if len(parts) < 4:
        return None
    slug_part, day_s, month_s, year_s = parts
    month_s = month_s.lower()
    if month_s not in ALL_MONTHS:
        return None
    try:
        d = date(int(year_s), ALL_MONTHS[month_s], int(day_s))
    except ValueError:
        return None
    return d, slug_part, matched


# --- Per-pope discovery ---------------------------------------------------

async def discover(pope: Pope, doctype: DocType) -> list[DocRef]:
    """Build DocRefs for one (pope, doctype) pair."""
    refs: list[DocRef] = []
    target_dir = (
        MAGISTERIUM_ROOT / "A-pre-vatican-ii" / "papes"
        / pope.folder / doctype.folder
    )

    if pope.slug == "pius-ix":
        # Pie IX has no /la/encyclicals/index.html. Use the language landing
        # page (all lang landings aggregate the same link list).
        landing_url = f"{VATICAN_BASE}/content/pius-ix/la.html"
        text = await _http_get_text(landing_url)
        if not text:
            text = await _http_get_text(f"{VATICAN_BASE}/content/pius-ix/it.html")
        if not text:
            log_error(source=VATICAN_DOMAIN, url=landing_url, phase=PHASE,
                      message=f"pius-ix landing unreachable for {doctype.key}")
            return refs

        # For Pie IX, the filename prefix is language-specific: `enciclica-`
        # is italian, `encyclica-` is latin. We prefer latin when present,
        # else italian, else the first language we saw. We aggregate by
        # (date, incipit-slug) to collapse italian+latin duplicates.
        found: dict[tuple[str, str], dict[str, tuple[str, str]]] = {}
        # key: (date_iso, incipit_slug) → {lang: (leaf, prefix)}

        for m in PIUS_IX_HREF_RE.finditer(text):
            leaf = m.group("leaf")
            # Determine language: `encyclica-` = la, `enciclica-`/italian
            # prefixes = it. Default per URL lang attr of the href.
            href_lang = m.group("lang")
            parsed = _parse_pius_ix_filename(leaf, doctype.legacy_prefixes)
            if not parsed:
                continue
            d, incipit_slug, matched = parsed
            # Infer language: if the prefix looks latin ("encyclica",
            # "litterae-apostolicae", "constitutio", "epistola-encyclica"),
            # tag as latin. Italian prefixes ("enciclica", "lettera-…",
            # "bolla", "costituzione") as italian.
            latin_prefixes = {
                "encyclica-", "epistola-encyclica-", "litterae-apostolicae-",
                "constitutio-", "exhortatio-apostolica-", "motu-proprio-",
            }
            if matched in latin_prefixes:
                inferred_lang = "la"
            else:
                inferred_lang = href_lang  # usually "it"
            key = (d.isoformat(), incipit_slug)
            entry = found.setdefault(key, {})
            entry[inferred_lang] = (leaf, matched)

        for (date_iso, incipit_slug), by_lang in found.items():
            # Pick language by preference order
            chosen = None
            for lg in LANG_ORDER:
                if lg in by_lang:
                    chosen = lg
                    break
            if not chosen:
                # pick anything
                chosen = next(iter(by_lang))
            leaf, _matched = by_lang[chosen]
            # The URL in the index was on the href_lang path; if we inferred
            # "la" from a latin-prefix filename, that file actually lives
            # under whichever lang section vatican serves it. Look back at
            # index to find the real href path for this leaf.
            url_path = _find_href_path(text, leaf) or f"/content/pius-ix/{chosen}/documents/{leaf}"
            url = VATICAN_BASE + url_path
            d = date.fromisoformat(date_iso)
            slug = f"{date_iso}_{incipit_slug}_{doctype.type_short}"
            refs.append(_make_docref(
                url=url, lang=chosen, target_dir=target_dir, slug=slug,
                incipit_slug=incipit_slug, pope=pope, doctype=doctype, d=d,
            ))
        return refs

    # Standard pope: discover through the dedicated index.
    index_url = f"{VATICAN_BASE}/content/{pope.slug}/la/{doctype.vatican_path}/index.html"
    text = await _http_get_text(index_url)
    if text is None:
        for lg in ("it", "fr", "en"):
            index_url = f"{VATICAN_BASE}/content/{pope.slug}/{lg}/{doctype.vatican_path}/index.html"
            text = await _http_get_text(index_url)
            if text is not None:
                break
    if text is None:
        # Section doesn't exist for this pope; silent skip (e.g. apost_exhortations
        # for Leo XIII, benedict-xv).
        return refs

    seen: set[str] = set()
    for m in HREF_RE.finditer(text):
        if m.group("popeslug") != pope.slug:
            continue
        if m.group("section") != doctype.vatican_path:
            continue
        leaf = m.group("leaf")
        if leaf in seen:
            continue
        seen.add(leaf)
        parsed = _parse_hf_filename(leaf)
        if not parsed:
            continue
        d, incipit_slug = parsed
        # We assume `/la/` is available for every document of these popes on
        # vatican.va (this holds in practice for Leo XIII → Pie XII). The
        # fetcher will log + skip if any individual doc is missing in latin;
        # a second pass can then recover those in italian.
        url = f"{VATICAN_BASE}/content/{pope.slug}/la/{doctype.vatican_path}/documents/{leaf}"
        slug = f"{d.isoformat()}_{incipit_slug}_{doctype.type_short}"
        refs.append(_make_docref(
            url=url, lang="la", target_dir=target_dir, slug=slug,
            incipit_slug=incipit_slug, pope=pope, doctype=doctype, d=d,
        ))
    return refs


def _find_href_path(text: str, leaf: str) -> Optional[str]:
    """Find the first href in `text` whose path ends with `/{leaf}`."""
    m = re.search(r'href="(/content/pius-ix/[a-z]{2}/documents/' + re.escape(leaf) + ')"', text)
    return m.group(1) if m else None


def _make_docref(
    url: str, lang: str, target_dir, slug: str, incipit_slug: str,
    pope: Pope, doctype: DocType, d: date,
) -> DocRef:
    incipit = incipit_slug.replace("-", " ").strip().capitalize()
    return DocRef(
        url=url,
        target_dir=target_dir,
        slug=slug,
        lang=lang,
        meta_hints={
            "incipit": incipit,
            "titre_fr": None,  # enrich later
            "auteur": pope.auteur,
            "periode": "pre-vatican-ii",
            "type": doctype.type_meta,
            "date": d,
            "autorite_magisterielle": "magistere-ordinaire-universel",
            "langue_originale": "la" if lang == "la" else lang,
            "langues_disponibles": [lang],
            "sujets": [],
        },
    )


# --- main -----------------------------------------------------------------

def _selected_types() -> list[DocType]:
    env = os.environ.get("PHASE3_TYPES", "encyclicals").strip()
    if env.lower() == "all":
        return list(DOCTYPES)
    keys = [k.strip() for k in env.split(",") if k.strip()]
    out = []
    for k in keys:
        if k in DOCTYPES_BY_KEY:
            out.append(DOCTYPES_BY_KEY[k])
        else:
            print(f"warning: unknown doctype key '{k}' (known: {list(DOCTYPES_BY_KEY)})", file=sys.stderr)
    return out or [DOCTYPES_BY_KEY["encyclicals"]]


async def main() -> int:
    refresh = os.environ.get("REFRESH") == "1"
    only_pope = os.environ.get("PHASE3_POPE")  # optional debug scope
    types = _selected_types()
    popes = [p for p in POPES if not only_pope or p.slug == only_pope]

    print(f"Phase 3 — popes: {[p.slug for p in popes]}, types: {[t.key for t in types]}")

    all_refs: list[DocRef] = []
    for pope in popes:
        for dt in types:
            refs = await discover(pope, dt)
            print(f"  discover {pope.slug}/{dt.key}: {len(refs)} docs")
            all_refs.extend(refs)

    print(f"Phase 3 — total {len(all_refs)} documents to fetch")
    if not all_refs:
        return 0

    # The fetcher already rate-limits per-domain to 1 req/2s. Concurrency>1
    # only helps across domains; here we stay on vatican.va, so effective
    # throughput is ~30 docs/min regardless of concurrency. We still keep
    # some concurrency for pipeline overhead overlap.
    result = await run_pipeline(all_refs, phase=PHASE, refresh=refresh, concurrency=8)
    print(
        f"Phase 3 done: ok={result.n_ok} skipped={result.n_skipped} "
        f"errors={result.n_errors}"
    )
    return 0 if result.n_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
