"""Scraper for catholic-hierarchy.org (David M. Cheney, 1996-).

Produces ``clerge/_raw/catholic_hierarchy.jsonl`` — one JSON line per bishop
fiche scraped from ``https://www.catholic-hierarchy.org/bishop/b<code>.html``.

Strategy
--------
1. Walk the alphabetical index
   ``la.html`` (root) → ``la<letter>.html`` (one per first letter)
   → ``la<letter><N>.html`` (paginated; same shape, ~200 names/page).
   Each page yields links of the form ``b<code>.html`` (the bishop fiches).
2. For each unique ``b<code>.html``, fetch + cache the raw HTML, then parse
   into a structured payload (see ``parse_bishop`` for fields extracted).
3. Append-only JSONL output, deduplicated by ``source_id`` so the run is
   resumable: the cache directory short-circuits the network and the JSONL
   short-circuits the parsing.

The site is run by volunteers — be polite: ONE request per second max
(harder than the global 2 s/domain default), identifiable User-Agent.
Cache HTML aggressively so we never refetch a fiche just to retune the
parser.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import re
import sys
import time
from collections.abc import Iterable
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

BASE_URL = "https://www.catholic-hierarchy.org/bishop/"
DOMAIN = "www.catholic-hierarchy.org"
USER_AGENT = "MagisteriumArchiver/1.0 (https://github.com/realitix/catholique)"
HTTP_TIMEOUT = 60.0

REPO_ROOT = Path(__file__).resolve().parents[3]
OUT_DIR = REPO_ROOT / "clerge" / "_raw"
CACHE_DIR = OUT_DIR / "_ch_cache"
OUT_JSONL = OUT_DIR / "catholic_hierarchy.jsonl"

# 1 request per second — strict.
RATE_LIMITER = DomainRateLimiter(min_interval=1.0)

# HTML on the site is declared iso-8859-1 (and sometimes contains stray
# windows-1252 bytes too — use latin-1 which never raises).
HTML_ENCODING = "iso-8859-1"

# Bishop fiche URLs look like b<code>.html where <code> is [a-z0-9_]+
RE_BISHOP_HREF = re.compile(r'href="(b[a-z0-9_]+\.html)"', re.IGNORECASE)
# Inner-index URLs look like la<rest>.html  /  ll<rest>.html  /  ld<rest>.html
RE_INDEX_HREF = re.compile(r'href="(l[ald][a-z]?[a-z0-9]*\.html)"', re.IGNORECASE)
# Generic pagination links inside an index page (la<letter><n>.html)
RE_PAGE_HREF = re.compile(r'href="(l[ald][a-z][a-z0-9]*\.html)"', re.IGNORECASE)


# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------


@dataclass
class Position:
    title: str | None = None
    start: str | None = None
    end: str | None = None


@dataclass
class Event:
    date: str | None = None
    age: str | None = None
    event: str | None = None
    title: str | None = None


@dataclass
class BishopRecord:
    source: str = "catholic-hierarchy.org"
    source_id: str = ""
    source_url: str = ""
    name: str | None = None
    title_prefix: str | None = None  # "Archbishop", "Pope", "Bishop", "Cardinal"…
    suffixes: list[str] = field(default_factory=list)  # religious order suffixes
    deceased: bool = False
    birth_date: str | None = None
    birth_place: str | None = None
    death_date: str | None = None
    death_place: str | None = None
    ordination_priest_date: str | None = None
    ordination_priest_place: str | None = None
    consecration_date: str | None = None
    consecration_place: str | None = None
    consecrator_principal_ch_code: str | None = None
    consecrated_by_label: str | None = None
    co_consecrator_ch_codes: list[str] = field(default_factory=list)
    co_consecrators_labels: list[str] = field(default_factory=list)
    positions: list[dict] = field(default_factory=list)
    events: list[dict] = field(default_factory=list)
    episcopal_lineage_codes: list[str] = field(default_factory=list)
    episcopal_lineage_labels: list[str] = field(default_factory=list)
    viaf_id: str | None = None
    wikidata_id: str | None = None
    raw_html_path: str | None = None
    fetched_at: str | None = None

    def to_dict(self) -> dict:
        return {k: v for k, v in self.__dict__.items() if v not in (None, [], "")}


# ---------------------------------------------------------------------------
# HTTP
# ---------------------------------------------------------------------------


async def _http_get(client: httpx.AsyncClient, url: str) -> bytes:
    await RATE_LIMITER.acquire(DOMAIN)
    r = await client.get(url)
    r.raise_for_status()
    return r.content


async def fetch_cached(
    client: httpx.AsyncClient,
    code: str,
    refresh: bool = False,
) -> tuple[str, Path]:
    """Fetch ``b<code>.html``, cache to disk, return (text, path)."""
    cache_path = CACHE_DIR / f"{code}.html"
    if cache_path.exists() and not refresh:
        return cache_path.read_text(encoding=HTML_ENCODING, errors="replace"), cache_path
    url = urljoin(BASE_URL, f"{code}.html")
    raw = await _http_get(client, url)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(raw)
    return raw.decode(HTML_ENCODING, errors="replace"), cache_path


async def fetch_index_page(client: httpx.AsyncClient, name: str) -> str:
    """Fetch an index page (e.g. ``la.html``, ``lab.html``, ``lab2.html``).

    Indexes are also cached so a re-crawl is purely local.
    """
    cache_path = CACHE_DIR / "_index" / name
    if cache_path.exists():
        return cache_path.read_text(encoding=HTML_ENCODING, errors="replace")
    url = urljoin(BASE_URL, name)
    raw = await _http_get(client, url)
    cache_path.parent.mkdir(parents=True, exist_ok=True)
    cache_path.write_bytes(raw)
    return raw.decode(HTML_ENCODING, errors="replace")


# ---------------------------------------------------------------------------
# Index crawl
# ---------------------------------------------------------------------------


def _bishop_codes_in(html: str) -> set[str]:
    """Return the set of bishop slugs (e.g. ``blefe``) referenced by the page.

    The slug *includes* the leading ``b`` so it matches the URL filename
    minus the ``.html`` extension.  This is what we store as ``source_id``.
    """
    out: set[str] = set()
    for m in RE_BISHOP_HREF.finditer(html):
        href = m.group(1)
        code = href[:-5]  # drop the '.html' extension
        if code == "bvacant":
            continue
        out.add(code)
    return out


def _sub_index_links(html: str, prefix: str) -> set[str]:
    """Return every l*-style index URL that starts with ``prefix``."""
    out: set[str] = set()
    for m in RE_PAGE_HREF.finditer(html):
        href = m.group(1)
        if href.startswith(prefix):
            out.add(href)
    return out


async def crawl_index(
    client: httpx.AsyncClient,
    max_codes: int | None = None,
    progress: bool = True,
) -> list[str]:
    """Discover every bishop code.

    Walks ``la.html`` → ``la<letter>.html`` → ``la<letter><n>.html``.
    Returns a sorted list of unique codes.
    """
    discovered: set[str] = set()
    root = await fetch_index_page(client, "la.html")
    # First level: links like lab.html, lac.html, lad.html, …
    first_level = sorted({
        href
        for href in _sub_index_links(root, "la")
        if href != "la.html" and re.fullmatch(r"la[a-z]\.html", href)
    })
    if progress:
        print(f"[index] root → {len(first_level)} letter pages", file=sys.stderr)

    for i, letter_page in enumerate(first_level, 1):
        html = await fetch_index_page(client, letter_page)
        discovered |= _bishop_codes_in(html)
        # Pagination: la<letter>2.html, la<letter>3.html, …
        prefix = letter_page[:-5]  # 'lab' for 'lab.html'
        page_re = re.compile(rf"{re.escape(prefix)}\d+\.html$")
        pagination = sorted({
            href for href in _sub_index_links(html, prefix) if page_re.match(href)
        })
        for sub in pagination:
            html2 = await fetch_index_page(client, sub)
            discovered |= _bishop_codes_in(html2)
            if max_codes is not None and len(discovered) >= max_codes:
                break
        if progress:
            print(
                f"[index] {letter_page} ({i}/{len(first_level)}) "
                f"+{len(pagination)} paginated · total codes={len(discovered)}",
                file=sys.stderr,
            )
        if max_codes is not None and len(discovered) >= max_codes:
            break

    return sorted(discovered)


# ---------------------------------------------------------------------------
# Bishop fiche parser
# ---------------------------------------------------------------------------


def _strip_tags(s: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"<[^>]+>", "", s)).strip()


def _strip_tags_keep_brackets(s: str) -> str:
    """Like _strip_tags but normalise nbsp and the † cross marker."""
    s = s.replace("&#8224;", "").replace("†", "")
    s = s.replace("&nbsp;", " ").replace("\xa0", " ")
    return _strip_tags(s)


def _meta_charset_decode(raw: bytes) -> str:
    """Decode using the encoding declared in the meta tag if any."""
    head = raw[:2048].decode("ascii", errors="replace").lower()
    m = re.search(r'charset=([a-z0-9_-]+)', head)
    enc = m.group(1) if m else HTML_ENCODING
    try:
        return raw.decode(enc, errors="replace")
    except LookupError:
        return raw.decode(HTML_ENCODING, errors="replace")


def parse_bishop(code: str, html: str) -> BishopRecord:
    """Parse a single ``b<code>.html`` document into a BishopRecord."""
    # `code` already contains the leading 'b' (it's the URL slug minus '.html')
    rec = BishopRecord(
        source_id=code,
        source_url=urljoin(BASE_URL, f"{code}.html"),
        fetched_at=datetime.now(UTC).isoformat(timespec="seconds"),
    )

    # Strip <script>/<style> so they don't pollute matching
    cleaned = re.sub(r"<script[^>]*>.*?</script>", "", html, flags=re.DOTALL | re.IGNORECASE)
    cleaned = re.sub(r"<style[^>]*>.*?</style>", "", cleaned, flags=re.DOTALL | re.IGNORECASE)

    # ----- Title / name -----
    m = re.search(r"<h1[^>]*>(.+?)</h1>", cleaned, re.DOTALL | re.IGNORECASE)
    if m:
        h1 = m.group(1)
        # h1 looks like: "Archbishop Louis-François-Bienaimé-Amedée <b>Lefèvre</b>, O.F.M. †"
        # Strip the † marker (HTML entity or unicode).
        cleaned_h1 = h1.replace("&#8224;", "").replace("†", "")
        full = _strip_tags(cleaned_h1)
        # title prefix = first word; rest = name
        # Possible prefixes: Pope, Cardinal, Archbishop, Bishop, Patriarch, Father…
        # The title sometimes includes a parenthetical given name (Pope Benedict XVI (Joseph Ratzinger))
        rec.name = full
        # Detect prefix from a small whitelist
        for prefix in (
            "Pope",
            "Archbishop",
            "Patriarch",
            "Cardinal",
            "Bishop",
            "Father",
            "Sister",
            "Brother",
        ):
            if full.startswith(prefix + " "):
                rec.title_prefix = prefix
                rec.name = full[len(prefix) + 1 :].strip()
                break
        # Religious suffixes after the surname (", O.F.M.", ", S.J.", …)
        suf = re.search(r",\s*((?:[A-Z]\.?){1,4}(?:\.[A-Z]\.?){0,4}\.?(?:\s*[A-Z]\.?)*)\s*$", rec.name)
        if suf:
            rec.suffixes = [suf.group(1).strip()]

    # ----- Deceased flag -----
    if re.search(r"<h2[^>]*><i>Deceased</i></h2>", cleaned, re.IGNORECASE):
        rec.deceased = True

    # ----- Microdata: birth/death/external IDs -----
    m = re.search(r'itemprop="birthDate"\s+datetime="([^"]+)"', cleaned)
    if m:
        rec.birth_date = _normalise_date(m.group(1))
    m = re.search(r'itemprop="deathDate"\s+datetime="([^"]+)"', cleaned)
    if m:
        rec.death_date = _normalise_date(m.group(1))
        rec.deceased = True
    m = re.search(r'href="https://viaf\.org/viaf/(\d+)"', cleaned)
    if m:
        rec.viaf_id = m.group(1)
    m = re.search(r'href="https://hub\.toolforge\.org/(Q\d+)"', cleaned)
    if m:
        rec.wikidata_id = m.group(1)

    # ----- Events table -----
    rec.events, rec.positions, rec.ordination_priest_date, rec.consecration_date = (
        _parse_events_table(cleaned)
    )

    # ----- Places table -----
    places = _parse_places_table(cleaned)
    rec.birth_place = places.get("Birth Place")
    rec.death_place = places.get("Death Place")
    rec.ordination_priest_place = places.get("Ordained Priest")
    rec.consecration_place = places.get("Ordained Bishop")

    # ----- Principal Consecrator -----
    pc_code, pc_label = _parse_principal_consecrator(cleaned)
    rec.consecrator_principal_ch_code = pc_code
    rec.consecrated_by_label = pc_label

    # ----- Co-Consecrators -----
    rec.co_consecrator_ch_codes, rec.co_consecrators_labels = _parse_co_consecrators(cleaned)

    # ----- Episcopal lineage -----
    rec.episcopal_lineage_codes, rec.episcopal_lineage_labels = _parse_lineage(cleaned)

    return rec


_MONTHS = {
    "Jan": "01",
    "Feb": "02",
    "Mar": "03",
    "Apr": "04",
    "May": "05",
    "Jun": "06",
    "Jul": "07",
    "Aug": "08",
    "Sep": "09",
    "Sept": "09",
    "Oct": "10",
    "Nov": "11",
    "Dec": "12",
}


def _normalise_date(s: str) -> str | None:
    s = s.strip()
    if not s:
        return None
    # ISO-like: 1927-4-16 → 1927-04-16 ; year-only stays as year-only
    iso = re.fullmatch(r"(\d{4})(?:-(\d{1,2}))?(?:-(\d{1,2}))?", s)
    if iso:
        y, mth, d = iso.groups()
        if mth and d:
            return f"{y}-{int(mth):02d}-{int(d):02d}"
        if mth:
            return f"{y}-{int(mth):02d}"
        return y
    # Human form: "8 Jul 1890"  or  "Jul 1890"  or  "1890"
    m = re.fullmatch(r"(\d{1,2})\s+([A-Za-z]+)\s+(\d{4})", s)
    if m:
        d, mn, y = m.groups()
        mm = _MONTHS.get(mn[:4]) or _MONTHS.get(mn[:3])
        if mm:
            return f"{y}-{mm}-{int(d):02d}"
    m = re.fullmatch(r"([A-Za-z]+)\s+(\d{4})", s)
    if m:
        mn, y = m.groups()
        mm = _MONTHS.get(mn[:4]) or _MONTHS.get(mn[:3])
        if mm:
            return f"{y}-{mm}"
    m = re.fullmatch(r"(\d{4})", s)
    if m:
        return m.group(1)
    return s  # best-effort, keep as-is


def _parse_events_table(html: str) -> tuple[list[dict], list[dict], str | None, str | None]:
    """Parse the central "Events" table.

    Returns:
        events: list of {date, age, event, title}
        positions: derived list of {title, start, end} from Appointed/Resigned/Ceased
        first ordination-priest date
        first ordination-bishop date
    """
    m = re.search(
        r"<h2[^>]*>\s*Events\s*</h2>.*?(<table[^>]*>.*?</table>)",
        html,
        re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return [], [], None, None
    table = m.group(1)
    rows = re.findall(r"<tr[^>]*>(.+?)</tr>", table, re.DOTALL | re.IGNORECASE)
    events: list[dict] = []
    # Detect the column layout from the header row.  Two layouts exist:
    #   Date | Age | Event | Title   (most modern fiches)
    #   Date | Event | Title         (older/sparser fiches)
    headers: list[str] = []
    for row in rows:
        cells = re.findall(r"<t[hd][^>]*>(.*?)</t[hd]>", row, re.DOTALL | re.IGNORECASE)
        if not cells:
            continue
        text_cells = [_strip_tags_keep_brackets(c) for c in cells]
        # The first non-empty row that contains "Date" as the first cell is
        # the header; remember its column names.
        if text_cells and text_cells[0].lower() == "date" and not events:
            headers = [c.lower() for c in text_cells]
            continue
        has_age = "age" in headers if headers else len(text_cells) >= 4
        if has_age:
            while len(text_cells) < 4:
                text_cells.append("")
            raw_date, age, ev, title = text_cells[:4]
        else:
            while len(text_cells) < 3:
                text_cells.append("")
            raw_date, ev, title = text_cells[:3]
            age = ""
        events.append({
            "date": _normalise_date(raw_date),
            "age": age or None,
            "event": ev or None,
            "title": title or None,
        })

    ordained_priest = next(
        (e["date"] for e in events if e["event"] == "Ordained Priest"),
        None,
    )
    ordained_bishop = next(
        (e["date"] for e in events if e["event"] == "Ordained Bishop"),
        None,
    )

    # Derive positions: an "Appointed" with a title starts a position; the
    # next "Resigned"/"Ceased"/"Succeeded"/"Died" on the same title ends it.
    positions: dict[str, dict] = {}
    for e in events:
        if not e.get("title"):
            continue
        key = e["title"]
        if e["event"] == "Appointed":
            positions.setdefault(key, {"title": key, "start": e["date"], "end": None})
        elif e["event"] in {"Resigned", "Ceased", "Succeeded", "Died"}:
            if key in positions:
                positions[key]["end"] = e["date"]
            else:
                positions[key] = {"title": key, "start": None, "end": e["date"]}

    return events, list(positions.values()), ordained_priest, ordained_bishop


def _parse_places_table(html: str) -> dict[str, str]:
    """Parse the small "Event / Place" table that follows the Events table.

    The site emits malformed HTML for this table (missing ``</td>`` and even
    ``</table>``) so we work line-by-line on the raw ``<tr>...<td>label<td>value``
    pattern.
    """
    out: dict[str, str] = {}
    # Each row looks like:
    #   <tr><td align=right>Birth Place<td>Cherbourg, <a ...>France</a>
    # before the next <tr> or </table>.
    pattern = re.compile(
        r"<tr[^>]*>\s*<t[hd][^>]*>(?P<label>[^<]+?)</?(?:t[hd]|td)[^>]*>"
        r"(?P<value>.*?)(?=<tr|</table|<h2|</body)",
        re.DOTALL | re.IGNORECASE,
    )
    # Restrict to the section starting at "Birth Place" so we don't pick up
    # the Events table accidentally.
    start = html.find("Birth Place")
    if start < 0:
        # Some fiches have Death Place without Birth Place
        start = html.find("Death Place")
    if start < 0:
        return out
    # Back up to the enclosing <table> to anchor the first row.
    table_start = html.rfind("<table", 0, start)
    section = html[table_start if table_start >= 0 else start : start + 5000]
    for m in pattern.finditer(section):
        label = _strip_tags_keep_brackets(m.group("label"))
        value = _strip_tags_keep_brackets(m.group("value"))
        if label and value:
            out[label] = value
    return out


def _parse_principal_consecrator(html: str) -> tuple[str | None, str | None]:
    """Extract the principal consecrator's CH code and display label."""
    m = re.search(
        r"Principal Consecrator[^<]*:?\s*<ul>(.+?)</ul>",
        html,
        re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return None, None
    block = m.group(1)
    li = re.search(
        r"<li[^>]*>\s*<a\s+href=\"(b[a-z0-9_]+)\.html\"[^>]*>(.+?)</a>",
        block,
        re.DOTALL | re.IGNORECASE,
    )
    if not li:
        # Sometimes there's a Pope without href, e.g. "Pope Pius X (1884)"
        text = _strip_tags_keep_brackets(block)
        return None, text or None
    return li.group(1), _strip_tags_keep_brackets(li.group(2))


def _parse_co_consecrators(html: str) -> tuple[list[str], list[str]]:
    m = re.search(
        r"Co-Consecrator[s]?:?\s*<ul>(.+?)</ul>",
        html,
        re.DOTALL | re.IGNORECASE,
    )
    if not m:
        return [], []
    block = m.group(1)
    codes: list[str] = []
    labels: list[str] = []
    for li in re.finditer(
        r"<li[^>]*>\s*(?:<a\s+href=\"(b[a-z0-9_]+)\.html\"[^>]*>(.+?)</a>|([^<]+))",
        block,
        re.DOTALL | re.IGNORECASE,
    ):
        code, link_text, plain = li.group(1), li.group(2), li.group(3)
        if code:
            codes.append(code)
            labels.append(_strip_tags_keep_brackets(link_text))
        elif plain and plain.strip():
            labels.append(plain.strip())
    return codes, labels


def _parse_lineage(html: str) -> tuple[list[str], list[str]]:
    """Parse the "Episcopal Lineage / Apostolic Succession" list.

    The list is a flat <ul> with one <li> per ancestor.  Most ancestors are
    links ``<a href="bcode.html">``; a few are popes listed without a link
    but with a redirect via ``Pope Pius X (1884)<br>(<a href="bsartogm.html">``.
    """
    m = re.search(
        r"Episcopal Lineage[^<]*</?[^>]*>\s*(?:<[^>]+>\s*)*<ul>(.+?)</ul>",
        html,
        re.DOTALL | re.IGNORECASE,
    )
    if not m:
        # Fallback: find the substring start and grab to the next </ul>
        idx = html.find("Episcopal Lineage")
        if idx < 0:
            return [], []
        sub = html[idx : idx + 50000]
        m = re.search(r"<ul>(.+?)</ul>", sub, re.DOTALL | re.IGNORECASE)
        if not m:
            return [], []
    block = m.group(1)
    items = re.split(r"<li[^>]*>", block, flags=re.IGNORECASE)[1:]
    codes: list[str] = []
    labels: list[str] = []
    for it in items:
        # Each <li> may have a direct link OR a "Pope X<br>(<a href=bcode.html>…)"
        link = re.search(r'<a\s+href="(b[a-z0-9_]+)\.html"', it, re.IGNORECASE)
        if link:
            codes.append(link.group(1))
        label = _strip_tags_keep_brackets(it)
        # Trim everything past the first newline-ish; keep one-liner
        label = re.split(r"\s{2,}|\n", label, maxsplit=1)[0].strip()
        if label:
            labels.append(label)
    return codes, labels


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


def parse_cached_file(code: str, path: Path) -> BishopRecord:
    """Re-parse a cached HTML file without hitting the network."""
    raw = path.read_bytes()
    html = _meta_charset_decode(raw)
    return parse_bishop(code, html)


async def run(
    *,
    limit: int | None = None,
    refresh: bool = False,
    only_codes: Iterable[str] | None = None,
    progress: bool = True,
) -> int:
    """Crawl + parse + append to ``catholic_hierarchy.jsonl``.

    Returns the number of *new* records written.
    """
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    seen = _existing_ids(OUT_JSONL) if not refresh else set()
    written = 0

    async with httpx.AsyncClient(
        headers={"User-Agent": USER_AGENT, "Accept-Language": "en,fr;q=0.8"},
        timeout=HTTP_TIMEOUT,
        follow_redirects=True,
    ) as client:
        if only_codes is not None:
            codes = list(only_codes)
        else:
            codes = await crawl_index(client, max_codes=limit, progress=progress)

        if limit is not None:
            codes = codes[:limit]

        t0 = time.monotonic()
        with OUT_JSONL.open("a", encoding="utf-8") as fp:
            for i, code in enumerate(codes, 1):
                if code in seen:
                    continue
                try:
                    html, cache_path = await fetch_cached(client, code, refresh=refresh)
                    rec = parse_bishop(code, html)
                    rec.raw_html_path = str(cache_path.relative_to(REPO_ROOT))
                    fp.write(json.dumps(rec.to_dict(), ensure_ascii=False) + "\n")
                    fp.flush()
                    written += 1
                    seen.add(code)
                except Exception as exc:  # noqa: BLE001 — log and continue
                    print(f"[err] {code}: {exc}", file=sys.stderr)
                if progress and i % 25 == 0:
                    elapsed = time.monotonic() - t0
                    rate = i / elapsed if elapsed else 0
                    print(
                        f"[fetch] {i}/{len(codes)} · "
                        f"+{written} new · {rate:.2f} req/s",
                        file=sys.stderr,
                    )

    return written


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _cli() -> int:
    p = argparse.ArgumentParser(
        description="Scrape catholic-hierarchy.org bishop fiches.",
    )
    p.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Stop after N bishop codes (default: full crawl).",
    )
    p.add_argument(
        "--refresh",
        action="store_true",
        help="Ignore cache + JSONL dedup (full refetch).",
    )
    p.add_argument(
        "--code",
        action="append",
        help="Skip the index crawl and fetch only these codes (repeatable).",
    )
    args = p.parse_args()
    written = asyncio.run(
        run(limit=args.limit, refresh=args.refresh, only_codes=args.code)
    )
    print(f"[done] wrote {written} new record(s) to {OUT_JSONL}", file=sys.stderr)
    return 0


if __name__ == "__main__":
    raise SystemExit(_cli())
