"""Scraper for gcatholic.org (Gabriel Chow, since 1997).

Produces ``clerge/_raw/gcatholic.jsonl`` — one JSON line per bishop record
scraped from gcatholic's alphabetical "living prelates" index.

Site structure
--------------
gcatholic.org indexes living prelates in two complementary ways:

1. **``/hierarchy/data/prelates-<N>.htm``** (N=1..57) — a compact alphabetical
   list of *all* living prelates. Each prelate is one ``<tr>`` row containing
   little more than name + current position + a cross-link to a richer page.
2. **``/hierarchy/data/bishops-<XX>.htm``** (XX in {A, AG, AM, …}) — the same
   prelates grouped by surname prefix, but with a **rich** ``<tr id="NNNN">``
   row holding birth/ordination/consecration dates, religious institute,
   positions held, country flag, etc.

There is no per-bishop URL: ``/p/<id>`` is a two-step JS redirect that lands
on ``bishops-XX#NNNN`` (anchor inside an alphabetical page).

We crawl in two phases:

* **Discovery**: walk ``prelates-1..57`` to collect the set of
  ``bishops-XX`` pages that actually contain prelate rows.
* **Extraction**: fetch each ``bishops-XX.htm`` once, cache, parse
  every ``<tr id="NNNN">`` row.

The ``source_id`` is the integer in ``id="NNNN"`` (gcatholic's stable
permalink number). It's what other gcatholic links use cross-site.

Polite settings:
- 1 request / second (DomainRateLimiter)
- Identifiable User-Agent
- HTML cached under ``clerge/_raw/_gc_cache/``
- Append-only JSONL, deduplicated by ``source_id`` so the run is resumable

Doctrinal note: gcatholic does **not** index FSSPX / sedevacantist / Old-Catholic
bishops (only prelates in communion with Rome). That asymmetry is by design
on their side; the reconciliation phase compensates from wikidata + tradi
sources.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from urllib.parse import urljoin

import httpx
from selectolax.parser import HTMLParser

from scrapers.core.rate_limit import DomainRateLimiter

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BASE_URL = "https://gcatholic.org/hierarchy/data/"
DOMAIN = "gcatholic.org"
USER_AGENT = "MagisteriumArchiver/1.0 (+https://github.com/realitix/catholique)"
HTTP_TIMEOUT = 60.0

REPO_ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = REPO_ROOT / "clerge" / "_raw"
CACHE_DIR = OUT_DIR / "_gc_cache"
OUT_JSONL = OUT_DIR / "gcatholic.jsonl"

# 1 request per second — be polite to a volunteer-run site.
RATE_LIMITER = DomainRateLimiter(min_interval=1.0)

# Living-prelates index spans 57 paginated pages (verified 2026-05).
PRELATES_PAGES = 57

# Regex helpers
RE_BISHOPS_PAGE = re.compile(r"bishops-[A-Z]+", re.IGNORECASE)
RE_TR_ID = re.compile(r'<tr\s+id="(\d+)"', re.IGNORECASE)
RE_TAG = re.compile(r"<[^>]+>")
RE_DATE = re.compile(r"(\d{4})\.(\d{2})\.(\d{2})")
RE_YEAR_MONTH = re.compile(r"(\d{4})\.(\d{2})")
RE_YEAR = re.compile(r"\b(\d{4})\b")
RE_NUMERIC_ID = re.compile(r"(\d{1,7})")


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class GCPosition:
    """A single line of office held."""

    title: str | None = None       # "Bishop", "Auxiliary Bishop", "Titular Bishop"…
    diocese: str | None = None     # "Buenos Aires"
    diocese_code: str | None = None  # internal gcatholic slug, e.g. "buen0"
    diocese_kind: str | None = None  # "Metropolitan Archdiocese", "Titular Episcopal See"…
    country: str | None = None     # ISO-2 from flag URL ("AR") or country label
    start: str | None = None       # YYYY-MM-DD
    end: str | None = None         # YYYY-MM-DD, or "..." → None
    raw_range: str | None = None   # the original "(1975.05.28 – 1992.04.10)" string

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v not in (None, "", [])}


@dataclass
class GCRecord:
    source: str = "gcatholic.org"
    source_id: str = ""             # the integer id from <tr id="NNNN">
    source_url: str = ""            # permalink /p/<id>
    index_page: str = ""            # the bishops-XX.htm where the row was scraped
    name: str | None = None         # "Guillermo Leaden"
    name_full: str | None = None    # the unstripped name string (with title etc.)
    title_prefix: str | None = None  # "Bishop", "Archbishop", "Cardinal"…
    surname: str | None = None
    religious_institute: str | None = None    # "S.D.B."
    religious_institute_label: str | None = None  # "Salesians of Saint John Bosco (Salesians)"
    religious_institute_code: str | None = None   # "008"
    motto: str | None = None
    motto_lang: str | None = None
    country: str | None = None       # ISO-2 country code from top flag
    country_label: str | None = None  # "Argentina"
    birth_date: str | None = None
    birth_place: str | None = None
    ordination_priest_date: str | None = None
    consecration_date: str | None = None
    elevation_cardinal_date: str | None = None
    death_date: str | None = None
    age_at_death: int | None = None
    positions: list[dict] = field(default_factory=list)
    # Cross-refs (gcatholic doesn't expose Wikidata / catholic-hierarchy
    # routinely on these rows, but if a future enrichment adds them we
    # already have a slot.)
    wikidata_qid: str | None = None
    ch_code: str | None = None
    raw_html_path: str | None = None  # cache file the row was parsed from
    fetched_at: str | None = None

    def to_dict(self) -> dict:
        out: dict = {}
        for k, v in self.__dict__.items():
            if v in (None, "", []):
                continue
            out[k] = v
        return out


# ---------------------------------------------------------------------------
# HTTP / cache layer
# ---------------------------------------------------------------------------


async def _http_get(client: httpx.AsyncClient, url: str) -> bytes:
    await RATE_LIMITER.acquire(DOMAIN)
    r = await client.get(url)
    r.raise_for_status()
    return r.content


async def fetch_cached(
    client: httpx.AsyncClient,
    page_name: str,
    *,
    refresh: bool = False,
) -> tuple[str, Path]:
    """Fetch ``<page_name>.htm``, cache, return (html_text, cache_path).

    ``page_name`` is a stem (no .htm), e.g. ``"prelates-3"`` or
    ``"bishops-DEL"``.
    """
    cache_path = CACHE_DIR / f"{page_name}.htm"
    if cache_path.exists() and not refresh:
        return cache_path.read_text(encoding="utf-8", errors="replace"), cache_path
    url = urljoin(BASE_URL, f"{page_name}.htm")
    raw = await _http_get(client, url)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(raw)
    return raw.decode("utf-8", errors="replace"), cache_path


# ---------------------------------------------------------------------------
# Phase 1 — discover bishops-XX pages from prelates-N
# ---------------------------------------------------------------------------


async def discover_bishop_pages(
    client: httpx.AsyncClient,
    *,
    refresh: bool = False,
    progress: bool = True,
) -> list[str]:
    """Walk ``prelates-1..57``, collect the set of ``bishops-XX`` stems.

    Returns a sorted list of page stems (without ``.htm``).
    """
    found: set[str] = set()
    for i in range(1, PRELATES_PAGES + 1):
        try:
            html, _ = await fetch_cached(client, f"prelates-{i}", refresh=refresh)
        except httpx.HTTPError as e:
            print(f"[discover] prelates-{i} → {e}", file=sys.stderr)
            continue
        for m in RE_BISHOPS_PAGE.finditer(html):
            found.add(m.group(0))
        if progress and i % 10 == 0:
            print(f"[discover] prelates-{i}/{PRELATES_PAGES} · {len(found)} bishops-XX pages",
                  file=sys.stderr)
    return sorted(found)


# ---------------------------------------------------------------------------
# Phase 2 — parse individual <tr id="..."> rows
# ---------------------------------------------------------------------------


def _strip_tags(s: str) -> str:
    s = RE_TAG.sub(" ", s)
    s = s.replace("&nbsp;", " ").replace("\xa0", " ")
    return re.sub(r"\s+", " ", s).strip()


def _norm_date(s: str | None) -> str | None:
    """Convert gcatholic ``YYYY.MM.DD`` (or ``YYYY.MM`` / ``YYYY``) → ISO."""
    if not s:
        return None
    s = s.strip()
    m = RE_DATE.search(s)
    if m:
        y, mth, d = m.groups()
        return f"{y}-{mth}-{d}"
    m = RE_YEAR_MONTH.search(s)
    if m:
        y, mth = m.groups()
        return f"{y}-{mth}"
    m = RE_YEAR.search(s)
    if m:
        return m.group(1)
    return None


def _split_rows(html: str) -> list[tuple[str, str]]:
    """Return [(id, row_html), …] for every ``<tr id="NNNN">`` row.

    Implementation: find all ``<tr id="…">`` matches in order; the row body
    is everything up to the *next* ``<tr id=…>`` start OR end of document.
    Because the table layout is flat (one prelate = one ``<tr>`` row), this
    over-captures slightly into the closing tags but the parser tolerates
    trailing markup.
    """
    matches = list(RE_TR_ID.finditer(html))
    rows: list[tuple[str, str]] = []
    for i, m in enumerate(matches):
        start = m.start()
        end = matches[i + 1].start() if i + 1 < len(matches) else len(html)
        rows.append((m.group(1), html[start:end]))
    return rows


def parse_row(row_id: str, row_html: str, page_stem: str) -> GCRecord:
    """Parse one ``<tr id="NNNN">…</tr>`` block into a GCRecord."""
    tree = HTMLParser(row_html)
    rec = GCRecord(
        source_id=row_id,
        source_url=f"https://gcatholic.org/p/{row_id}",
        index_page=f"{page_stem}.htm",
        fetched_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )

    # ----- Country (top flag <img class="flag1">) -----
    flag = tree.css_first("img.flag1")
    if flag is not None:
        alt = flag.attributes.get("alt") or ""
        src = flag.attributes.get("src") or ""
        rec.country_label = alt.strip() or None
        # /images/flags/AR2.png → "AR"
        m = re.search(r"/flags/([A-Z]{2})\d?\.", src)
        if m:
            rec.country = m.group(1)

    # ----- Name block (span.zname or span.prel* hierarchy) -----
    zname = tree.css_first("span.zname")
    if zname is not None:
        # title prefix is one of span.prelP / prelB / prelA / prelC…
        prel = zname.css_first("span.prelP, span.prelB, span.prelA, span.prelC, span.prelN")
        if prel is not None:
            full = _strip_tags(prel.html or "")
            rec.name_full = full
            # Strip trailing ", S.D.B." from the prefix span if present
            for kw in ("Pope", "Cardinal", "Patriarch", "Major Archbishop",
                       "Archbishop", "Bishop", "Apostolic Administrator",
                       "Apostolic Prefect", "Apostolic Vicar", "Prelate",
                       "Eparch", "Exarch", "Father", "Brother", "Sister"):
                if full.startswith(kw + " ") or full == kw:
                    rec.title_prefix = kw
                    rec.name = full[len(kw):].strip().lstrip(",").strip() or None
                    break
            else:
                rec.name = full
        else:
            # fallback when there is no prefix-span
            rec.name_full = _strip_tags(zname.html or "")
            rec.name = rec.name_full

        surname_node = zname.css_first("span.znameL")
        if surname_node is not None:
            rec.surname = _strip_tags(surname_node.html or "") or None

        # Religious institute: span.zname → a.zorder
        order_a = zname.css_first("a.zorder")
        if order_a is not None:
            rec.religious_institute = _strip_tags(order_a.html or "") or None
            rec.religious_institute_label = (order_a.attributes.get("title") or "").strip() or None
            href = order_a.attributes.get("href") or ""
            m = re.search(r"/orders/([A-Za-z0-9_-]+)", href)
            if m:
                rec.religious_institute_code = m.group(1)

    # If no zname matched (some popes/cardinals use a flat structure), best-effort
    if rec.name is None:
        # Try p.name first
        pname = tree.css_first("p.name")
        if pname is not None:
            rec.name_full = _strip_tags(pname.html or "")
            rec.name = rec.name_full

    # ----- Motto -----
    motto_a = tree.css_first("a.motto")
    if motto_a is not None:
        rec.motto = _strip_tags(motto_a.html or "") or None
        title = motto_a.attributes.get("title") or ""
        m = re.match(r"Motto in ([A-Za-z]+)", title)
        if m:
            rec.motto_lang = m.group(1).lower()

    # ----- Bio table: Born / Ordained Priest / Consecrated Bishop / Cardinal / Died -----
    for tr in tree.css("table.ntb tr"):
        cells = tr.css("td")
        if len(cells) < 2:
            continue
        label = _strip_tags(cells[0].html or "").rstrip(":").strip()
        val_node = cells[1]
        val_text = _strip_tags(val_node.html or "")
        if not label:
            continue
        lbl_low = label.lower()
        if lbl_low.startswith("born"):
            rec.birth_date = _norm_date(val_text)
            place = val_node.css_first("span.znote")
            if place is not None:
                p_text = _strip_tags(place.html or "")
                # "(Buenos Aires, Argentina)" → "Buenos Aires, Argentina"
                p_text = p_text.strip("() ")
                if p_text:
                    rec.birth_place = p_text
        elif lbl_low.startswith("ordained priest"):
            rec.ordination_priest_date = _norm_date(val_text)
        elif lbl_low.startswith("consecrated bishop") or lbl_low.startswith("ordained bishop"):
            rec.consecration_date = _norm_date(val_text)
        elif lbl_low.startswith("cardinal") or lbl_low.startswith("created cardinal"):
            rec.elevation_cardinal_date = _norm_date(val_text)
        elif lbl_low.startswith("died") or lbl_low.startswith("deceased"):
            rec.death_date = _norm_date(val_text)
            note = val_node.css_first("span.znote")
            if note is not None:
                t = _strip_tags(note.html or "")
                m = re.search(r"\(†\s*(\d+)\)", t)
                if m:
                    try:
                        rec.age_at_death = int(m.group(1))
                    except ValueError:
                        pass

    # ----- Positions table: <p class="indent"> blocks -----
    for p in tree.css("p.indent"):
        pos = _parse_position(p)
        if pos:
            rec.positions.append(pos.to_dict())

    # ----- Cross-refs (gcatholic occasionally embeds external IDs) -----
    for a in tree.css("a"):
        href = a.attributes.get("href") or ""
        if not href:
            continue
        m = re.search(r"wikidata\.org/wiki/(Q\d+)", href)
        if m and rec.wikidata_qid is None:
            rec.wikidata_qid = m.group(1)
            continue
        m = re.search(r"catholic-hierarchy\.org/bishop/(b[a-z0-9_]+)\.html", href)
        if m and rec.ch_code is None:
            rec.ch_code = m.group(1)

    return rec


def _parse_position(p_node) -> GCPosition | None:
    """Parse one ``<p class="indent">`` line into a GCPosition."""
    office_node = p_node.css_first("span.zoffice")
    if office_node is None:
        return None
    pos = GCPosition()
    pos.title = _strip_tags(office_node.html or "") or None
    # Diocese link is the first <a> with class starting "type" (typedioc, typemetr,
    # typet6, typeordi, typeexa, typeapos…)
    diocese_a = None
    for a in p_node.css("a"):
        cls = a.attributes.get("class") or ""
        if cls.startswith("type"):
            diocese_a = a
            break
    if diocese_a is not None:
        pos.diocese = _strip_tags(diocese_a.html or "") or None
        pos.diocese_kind = (diocese_a.attributes.get("title") or "").strip() or None
        href = diocese_a.attributes.get("href") or ""
        # ../../dioceses/diocese/buen0  /  ../../dioceses/former/t1766
        m = re.search(r"/dioceses/(?:diocese|former)/([A-Za-z0-9_-]+)", href)
        if m:
            pos.diocese_code = m.group(1)
    # Country: any a.zcountry within the same <p>
    country_a = p_node.css_first("a.zcountry")
    if country_a is not None:
        href = country_a.attributes.get("href") or ""
        m = re.search(r"/dioceses/country/([A-Z]{2})", href)
        if m:
            pos.country = m.group(1)
    # Date range: last span.znote of the line typically has "(YYYY.MM.DD – YYYY.MM.DD)"
    notes = p_node.css("span.znote")
    if notes:
        last = _strip_tags(notes[-1].html or "")
        if "–" in last or "-" in last or "..." in last:
            pos.raw_range = last.strip("() ") or None
            # Split on em-dash, en-dash, or hyphen
            parts = re.split(r"\s*[–—-]\s*", last.strip("() "), maxsplit=1)
            if len(parts) == 2:
                start_raw, end_raw = parts
                pos.start = _norm_date(start_raw)
                if "..." not in end_raw:
                    pos.end = _norm_date(end_raw)
    return pos


# ---------------------------------------------------------------------------
# Driver
# ---------------------------------------------------------------------------


def _existing_ids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    out: set[str] = set()
    with path.open(encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except json.JSONDecodeError:
                continue
            sid = rec.get("source_id")
            if sid:
                out.add(sid)
    return out


async def run(
    *,
    limit: int | None = None,
    refresh: bool = False,
    progress: bool = True,
) -> int:
    """Run the full pipeline. Returns number of NEW records appended."""
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    seen = _existing_ids(OUT_JSONL) if not refresh else set()
    written = 0

    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT, "Accept-Language": "en,fr;q=0.8"},
        timeout=HTTP_TIMEOUT,
        follow_redirects=True,
        http2=True,
    ) as client:
        pages = await discover_bishop_pages(client, refresh=refresh, progress=progress)
        if progress:
            print(f"[main] {len(pages)} bishops-XX pages discovered", file=sys.stderr)

        t0 = time.monotonic()
        with OUT_JSONL.open("a", encoding="utf-8") as fp:
            for pi, page_stem in enumerate(pages, 1):
                try:
                    html, cache_path = await fetch_cached(
                        client, page_stem, refresh=refresh
                    )
                except httpx.HTTPError as e:
                    print(f"[err] page {page_stem}: {e}", file=sys.stderr)
                    continue
                rows = _split_rows(html)
                new_on_page = 0
                for row_id, row_html in rows:
                    if row_id in seen:
                        continue
                    try:
                        rec = parse_row(row_id, row_html, page_stem)
                        rec.raw_html_path = str(cache_path.relative_to(REPO_ROOT))
                        fp.write(json.dumps(rec.to_dict(), ensure_ascii=False) + "\n")
                    except Exception as exc:  # noqa: BLE001
                        print(f"[err] row {row_id} on {page_stem}: {exc}",
                              file=sys.stderr)
                        continue
                    seen.add(row_id)
                    written += 1
                    new_on_page += 1
                    if limit is not None and written >= limit:
                        break
                fp.flush()
                if progress:
                    elapsed = time.monotonic() - t0
                    rate = pi / elapsed if elapsed else 0
                    print(
                        f"[fetch] {page_stem} ({pi}/{len(pages)}) · "
                        f"{len(rows)} rows · +{new_on_page} new · "
                        f"total +{written} · {rate:.2f} pg/s",
                        file=sys.stderr,
                    )
                if limit is not None and written >= limit:
                    break

    return written


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> int:
    p = argparse.ArgumentParser(
        description="Scrape gcatholic.org living prelates index.",
    )
    p.add_argument(
        "--limit", type=int, default=None,
        help="Stop after N new records (default: full crawl).",
    )
    p.add_argument(
        "--refresh", action="store_true",
        help="Ignore cache + JSONL dedup (full refetch).",
    )
    p.add_argument(
        "--page", action="append",
        help="Restrict to specific bishops-XX page(s) (repeatable, "
             "e.g. --page bishops-LE).",
    )
    args = p.parse_args()

    if args.page:
        # Manual page list: skip discovery, fetch directly.
        async def _run_pages() -> int:
            OUT_DIR.mkdir(parents=True, exist_ok=True)
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            seen = _existing_ids(OUT_JSONL) if not args.refresh else set()
            written = 0
            async with httpx.AsyncClient(
                headers={"User-Agent": USER_AGENT, "Accept-Language": "en,fr;q=0.8"},
                timeout=HTTP_TIMEOUT,
                follow_redirects=True,
            ) as client:
                with OUT_JSONL.open("a", encoding="utf-8") as fp:
                    for page_stem in args.page:
                        html, cache_path = await fetch_cached(
                            client, page_stem, refresh=args.refresh
                        )
                        for row_id, row_html in _split_rows(html):
                            if row_id in seen:
                                continue
                            rec = parse_row(row_id, row_html, page_stem)
                            rec.raw_html_path = str(cache_path.relative_to(REPO_ROOT))
                            fp.write(json.dumps(rec.to_dict(), ensure_ascii=False) + "\n")
                            seen.add(row_id)
                            written += 1
                            if args.limit is not None and written >= args.limit:
                                return written
                    fp.flush()
            return written

        written = asyncio.run(_run_pages())
    else:
        written = asyncio.run(run(limit=args.limit, refresh=args.refresh))

    print(f"[done] wrote {written} new record(s) to {OUT_JSONL}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
