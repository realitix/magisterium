"""Phase 1bis — Wikidata SPARQL scraper for Catholic priests (presbyters).

Pulls every human (Q5) whose occupation (P106) is Catholic priest (Q250867)
**and** is NOT also a Catholic bishop (Q611644) — those are already covered
by ``wikidata.py``.

For each priest we try to extract:

- canonical names (fr/en/la), short description
- birth / death dates and places
- nationality
- candidate ordaining bishop(s) — Wikidata exposes P1598 (consecrated by) on
  many "priests" who are in fact bishops missing the Q611644 occupation tag;
  we keep these as ``ordinateur_qids`` so phase 4 can decide.
- religious institute / society (P611)
- image (P18)
- positions held (P39) + start/end qualifiers (cures, recteurs, etc.).
- Wikipedia FR sitelink

We filter out rows lacking *both* a birth date and any positional dates —
those entries are too thin to anchor a Q/R page.

Paginates by birth-year bucket like the bishop scraper, with a "noDoB" bucket
for entries without ``P569``.

Output: ``clerge/_raw/wikidata_pretres.jsonl`` (one priest per line).

After the SPARQL phase, we cross-reference each ``ordinateur_qids`` entry with
the existing ``clerge/eveques/*.yaml`` corpus (indexed by ``qids.wikidata``)
and write the resolved slugs under ``ordinateur_slugs``.

Idempotent:
  - Existing ``source_id`` entries in the JSONL are skipped.
  - Raw SPARQL responses are cached under ``clerge/_raw/_wikidata_pretres_cache/``.

Usage::

    uv run python -m scrapers.clerge.sources.wikidata_pretres
    uv run python -m scrapers.clerge.sources.wikidata_pretres --limit 500
    uv run python -m scrapers.clerge.sources.wikidata_pretres --refresh
    uv run python -m scrapers.clerge.sources.wikidata_pretres --buckets "1901-1920,noDoB"
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import re
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import httpx

ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = ROOT / "clerge" / "_raw"
CACHE_DIR = RAW_DIR / "_wikidata_pretres_cache"
OUT_PATH = RAW_DIR / "wikidata_pretres.jsonl"
EVEQUES_DIR = ROOT / "clerge" / "eveques"

ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = (
    "MagisteriumArchiver/1.0 "
    "(https://github.com/realitix/catholique; contact via repo)"
)

MIN_INTERVAL = 0.25
SPARQL_TIMEOUT = 90.0

PRIEST_OCCUPATION = "Q250867"
BISHOP_OCCUPATION = "Q611644"

# Catholic priest QID stays the canonical class. We *explicitly* exclude
# anyone tagged as bishop (Q611644), archbishop (Q49476), cardinal (Q45722).
# An anglican/orthodox "priest" doesn't carry Q250867 by convention on
# Wikidata, but to be safe we also exclude P140 set to non-catholic religions
# when present.
EXCLUDE_OCCUPATIONS = {
    BISHOP_OCCUPATION,   # Catholic bishop
    "Q49476",            # archbishop
    "Q45722",            # cardinal
    "Q47481344",         # auxiliary bishop
    "Q170790",           # patriarch
}

# Non-catholic religions explicitly excluded when P140 is set. Catholic
# subclasses propagate as Q9592 in practice, but we accept any priest that
# either has no P140 or whose P140 is in the allowed set below. We trust the
# P106=Q250867 ("Catholic priest") tag as the primary filter.
NONCATHOLIC_RELIGIONS = {
    "Q35032",   # Eastern Orthodox Church
    "Q5043",    # Christianity (too generic — keep only if no other signal)
    "Q33203",   # Anglicanism
    "Q23540",   # Protestantism
    "Q101849",  # Lutheranism
    "Q5891",    # Buddhism
    "Q9268",    # Judaism
    "Q432",     # Islam
}

# Birth-year buckets. Priests cluster heavily in 1800-1950. Sub-bucket size
# tuned so each query stays under the 60s public timeout and below
# SOFT_BUCKET_LIMIT (~9k rows).
BIRTH_BUCKETS: list[tuple[int | None, int | None]] = [
    (None, None),     # no birth date (~4.5k)
    (None, 1200),
    (1201, 1400),
    (1401, 1500),
    (1501, 1600),
    (1601, 1700),
    (1701, 1800),
    (1801, 1850),
    (1851, 1880),
    (1881, 1900),
    (1901, 1910),
    (1911, 1920),
    (1921, 1930),
    (1931, 1940),
    (1941, 1950),
    (1951, 1960),
    (1961, 1970),
    (1971, 1980),
    (1981, 2025),
]


# ---------------------------------------------------------------------------
# SPARQL query construction
# ---------------------------------------------------------------------------

# Note: we *don't* filter out bishops in SPARQL (that'd require a NOT EXISTS
# clause that combines poorly with GROUP BY on the bishop side and times out
# regularly on dense buckets). We exclude post-fetch on the occupations set,
# which we also retrieve.
_BASE_QUERY = """
SELECT ?p
       (SAMPLE(?nameFr) AS ?nameFr)
       (SAMPLE(?nameLa) AS ?nameLa)
       (SAMPLE(?nameEn) AS ?nameEn)
       (SAMPLE(?descFr) AS ?descFr)
       (SAMPLE(?descEn) AS ?descEn)
       (SAMPLE(?dob) AS ?dob)
       (SAMPLE(?dod) AS ?dod)
       (SAMPLE(?birthPlace) AS ?birthPlace)
       (SAMPLE(?deathPlace) AS ?deathPlace)
       (GROUP_CONCAT(DISTINCT ?nationality;separator="|") AS ?nationalities)
       (GROUP_CONCAT(DISTINCT ?occupation;separator="|") AS ?occupations)
       (GROUP_CONCAT(DISTINCT ?ordinator;separator="|") AS ?ordinators)
       (SAMPLE(?image) AS ?image)
       (GROUP_CONCAT(DISTINCT ?order;separator="|") AS ?orders)
       (GROUP_CONCAT(DISTINCT ?religion;separator="|") AS ?religions)
       (SAMPLE(?wpFr) AS ?wpFr)
WHERE {
  ?p wdt:P31 wd:Q5 ;
     wdt:P106 wd:Q250867 .
  ##BIRTH_FILTER##
  OPTIONAL { ?p rdfs:label ?nameFr . FILTER(LANG(?nameFr) = "fr") }
  OPTIONAL { ?p rdfs:label ?nameLa . FILTER(LANG(?nameLa) = "la") }
  OPTIONAL { ?p rdfs:label ?nameEn . FILTER(LANG(?nameEn) = "en") }
  OPTIONAL { ?p schema:description ?descFr . FILTER(LANG(?descFr) = "fr") }
  OPTIONAL { ?p schema:description ?descEn . FILTER(LANG(?descEn) = "en") }
  OPTIONAL { ?p wdt:P569 ?dob . }
  OPTIONAL { ?p wdt:P570 ?dod . }
  OPTIONAL { ?p wdt:P19 ?birthPlace . }
  OPTIONAL { ?p wdt:P20 ?deathPlace . }
  OPTIONAL { ?p wdt:P27 ?nationality . }
  OPTIONAL { ?p wdt:P106 ?occupation . }
  OPTIONAL { ?p wdt:P1598 ?ordinator . }
  OPTIONAL { ?p wdt:P18 ?image . }
  OPTIONAL { ?p wdt:P611 ?order . }
  OPTIONAL { ?p wdt:P140 ?religion . }
  OPTIONAL {
    ?wpFr schema:about ?p ;
          schema:isPartOf <https://fr.wikipedia.org/> .
  }
}
GROUP BY ?p
"""


def _birth_filter(low: int | None, high: int | None) -> str:
    if low is None and high is None:
        return "FILTER NOT EXISTS { ?p wdt:P569 ?_anyDob . }"
    parts = ["?p wdt:P569 ?_filterDob ."]
    if low is not None:
        parts.append(f"FILTER(YEAR(?_filterDob) >= {low})")
    if high is not None:
        parts.append(f"FILTER(YEAR(?_filterDob) <= {high})")
    return "\n  ".join(parts)


def build_query(low: int | None, high: int | None) -> str:
    return _BASE_QUERY.replace("##BIRTH_FILTER##", _birth_filter(low, high))


# Detail enrichment: positions (P39) with date qualifiers + ordaining priest
# role (P3831 = Q12477887 if anyone ever fills it).
_DETAILS_QUERY = """
SELECT ?p ?position ?positionLabel ?start ?end ?role ?location
WHERE {
  VALUES ?p { ##QIDS## }
  ?p p:P39 ?st .
  ?st ps:P39 ?position .
  OPTIONAL { ?st pq:P580 ?start . }
  OPTIONAL { ?st pq:P582 ?end . }
  OPTIONAL { ?st pq:P3831 ?role . }
  OPTIONAL { ?st pq:P276 ?location . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "fr,en". }
}
"""


def build_details_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return _DETAILS_QUERY.replace("##QIDS##", values)


# Ordinator role qualifier (Q12477887 = ordaining priest, Q18442817 = principal
# consecrator) on P1598.
_ORDINATOR_ROLE_QUERY = """
SELECT ?p ?ordinator ?role ?date
WHERE {
  VALUES ?p { ##QIDS## }
  ?p p:P1598 ?st .
  ?st ps:P1598 ?ordinator .
  OPTIONAL { ?st pq:P3831 ?role . }
  OPTIONAL { ?st pq:P585 ?date . }
}
"""


def build_ordinator_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return _ORDINATOR_ROLE_QUERY.replace("##QIDS##", values)


# ---------------------------------------------------------------------------
# SPARQL client (cache + rate limit + retries)
# ---------------------------------------------------------------------------


@dataclass
class SparqlClient:
    client: httpx.AsyncClient
    last_call: float = 0.0
    refresh: bool = False

    async def query(self, sparql: str, cache_key: str) -> dict[str, Any]:
        cache_path = CACHE_DIR / f"{cache_key}.json"
        if not self.refresh and cache_path.exists():
            return json.loads(cache_path.read_text(encoding="utf-8"))
        wait = MIN_INTERVAL - (time.monotonic() - self.last_call)
        if wait > 0:
            await asyncio.sleep(wait)
        attempt = 0
        while True:
            attempt += 1
            self.last_call = time.monotonic()
            try:
                resp = await self.client.get(
                    ENDPOINT,
                    params={"query": sparql, "format": "json"},
                    headers={
                        "Accept": "application/sparql-results+json",
                        "User-Agent": USER_AGENT,
                    },
                    timeout=SPARQL_TIMEOUT,
                )
            except (httpx.ReadTimeout, httpx.ConnectTimeout, httpx.RemoteProtocolError) as exc:
                if attempt >= 5:
                    raise
                backoff = 2 ** attempt
                print(f"  [retry {attempt}] {type(exc).__name__}; sleep {backoff}s", file=sys.stderr)
                await asyncio.sleep(backoff)
                continue
            if resp.status_code == 429 or resp.status_code >= 500:
                if attempt >= 5:
                    resp.raise_for_status()
                retry_after = float(resp.headers.get("Retry-After", 2 ** attempt))
                print(f"  [retry {attempt}] HTTP {resp.status_code}; sleep {retry_after}s", file=sys.stderr)
                await asyncio.sleep(retry_after)
                continue
            resp.raise_for_status()
            data = resp.json()
            CACHE_DIR.mkdir(parents=True, exist_ok=True)
            cache_path.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
            return data


# ---------------------------------------------------------------------------
# Parsing helpers
# ---------------------------------------------------------------------------


def _qid(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]


def _split_concat(value: str) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split("|") if v.strip()]


def _date(value: str | None) -> str | None:
    if not value:
        return None
    v = value.lstrip("+")
    return v[:10] if "T" in v else v


ORDAINING_PRIEST_ROLE = "Q12477887"


def _record_from_binding(b: dict[str, Any]) -> dict[str, Any]:
    qid = _qid(b["p"]["value"])
    names: dict[str, str] = {}
    for lang_key, var in (("fr", "nameFr"), ("la", "nameLa"), ("en", "nameEn")):
        if var in b and b[var].get("value"):
            names[lang_key] = b[var]["value"]
    description: dict[str, str] = {}
    for lang_key, var in (("fr", "descFr"), ("en", "descEn")):
        if var in b and b[var].get("value"):
            description[lang_key] = b[var]["value"]

    ordinator_uris = _split_concat(b.get("ordinators", {}).get("value", ""))
    nationality_uris = _split_concat(b.get("nationalities", {}).get("value", ""))
    occupation_uris = _split_concat(b.get("occupations", {}).get("value", ""))
    order_uris = _split_concat(b.get("orders", {}).get("value", ""))
    religion_uris = _split_concat(b.get("religions", {}).get("value", ""))

    return {
        "source": "wikidata-pretres",
        "source_id": qid,
        "names": names,
        "description": description,
        "birth_date": _date(b.get("dob", {}).get("value")),
        "birth_place_qid": _qid(b["birthPlace"]["value"]) if b.get("birthPlace") else None,
        "death_date": _date(b.get("dod", {}).get("value")),
        "death_place_qid": _qid(b["deathPlace"]["value"]) if b.get("deathPlace") else None,
        "nationality_qids": [_qid(u) for u in nationality_uris],
        "occupation_qids": [_qid(u) for u in occupation_uris],
        "ordinateur_qids": [_qid(u) for u in ordinator_uris],
        "ordinateur_qids_role_explicit": [],   # filled by enrichment
        "ordination_date": None,               # filled by enrichment
        "ordination_place_qid": None,          # filled by enrichment
        "image_url": b.get("image", {}).get("value") or None,
        "religious_institute_qids": [_qid(u) for u in order_uris],
        "positions": [],                       # filled by enrichment
        "religion_qids": [_qid(u) for u in religion_uris],
        "religion_qid": _qid(religion_uris[0]) if religion_uris else None,
        "wikipedia_fr_url": b.get("wpFr", {}).get("value") or None,
        "rang": "pretre",
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


def _should_keep(rec: dict[str, Any]) -> tuple[bool, str | None]:
    """Filter: keep priest if (a) not also bishop/archbishop/cardinal AND
    (b) has at least a name + (birth_date OR ordination_date OR any position
    with a start date OR a death date)."""
    occs = set(rec.get("occupation_qids", []))
    if occs & EXCLUDE_OCCUPATIONS:
        return False, "excluded_occupation"
    # Reject explicit non-catholic religion tags
    rels = set(rec.get("religion_qids", []))
    if rels & NONCATHOLIC_RELIGIONS:
        return False, "noncatholic_religion"
    if not rec["names"]:
        return False, "no_name"
    has_date = bool(rec.get("birth_date") or rec.get("death_date") or rec.get("ordination_date"))
    if not has_date:
        # Last chance: look for any position with a start date
        has_date = any(p.get("start") for p in rec.get("positions", []))
    if not has_date:
        return False, "no_date"
    return True, None


# ---------------------------------------------------------------------------
# Cache-key + bucket fetching
# ---------------------------------------------------------------------------


def _cache_key(prefix: str, *parts: Any) -> str:
    raw = f"{prefix}:" + ":".join(str(p) for p in parts)
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    safe = raw.replace(":", "_").replace("/", "_")[:80]
    return f"{safe}_{h}"


SOFT_BUCKET_LIMIT = 9000


async def fetch_bucket(
    sparql: SparqlClient, low: int | None, high: int | None
) -> list[dict[str, Any]]:
    label = f"{low}-{high}" if low is not None or high is not None else "noDoB"
    key = _cache_key("priests", low, high)
    print(f"[bucket {label}] querying…", file=sys.stderr)
    data = await sparql.query(build_query(low, high), key)
    bindings = data["results"]["bindings"]
    print(f"[bucket {label}] -> {len(bindings)} rows", file=sys.stderr)
    if len(bindings) >= SOFT_BUCKET_LIMIT and low is not None and high is not None and high > low:
        mid = (low + high) // 2
        print(f"[bucket {label}] above soft limit, splitting", file=sys.stderr)
        a = await fetch_bucket(sparql, low, mid)
        b = await fetch_bucket(sparql, mid + 1, high)
        seen: dict[str, dict[str, Any]] = {}
        for b_ in (a + b):
            seen[b_["p"]["value"]] = b_
        return list(seen.values())
    return bindings


# ---------------------------------------------------------------------------
# Enrichment: positions + ordinator role
# ---------------------------------------------------------------------------


DETAILS_BATCH_SIZE = 50


def _chunks(seq: list[str], n: int) -> Iterable[list[str]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


async def enrich_with_details(
    sparql: SparqlClient, records: dict[str, dict[str, Any]]
) -> None:
    qids = list(records.keys())

    # --- Positions (P39) ---
    for batch in _chunks(qids, DETAILS_BATCH_SIZE):
        key = _cache_key("positions", batch[0], batch[-1], len(batch))
        print(f"[details positions] {batch[0]}…{batch[-1]} ({len(batch)})", file=sys.stderr)
        try:
            data = await sparql.query(build_details_query(batch), key)
        except httpx.HTTPError as e:
            print(f"  [skip] positions batch failed: {e}", file=sys.stderr)
            continue
        for b in data["results"]["bindings"]:
            p_qid = _qid(b["p"]["value"])
            rec = records.get(p_qid)
            if rec is None:
                continue
            pos_qid = _qid(b["position"]["value"])
            pos_entry = {
                "position_qid": pos_qid,
                "label_fr": b.get("positionLabel", {}).get("value"),
                "start": _date(b.get("start", {}).get("value")),
                "end": _date(b.get("end", {}).get("value")),
                "location_qid": _qid(b["location"]["value"]) if b.get("location") else None,
            }
            rec["positions"].append(pos_entry)
            # The "Catholic priest" P39 with a start date is the closest
            # Wikidata gets to an ordination event.
            if pos_qid == PRIEST_OCCUPATION and pos_entry["start"]:
                if not rec.get("ordination_date"):
                    rec["ordination_date"] = pos_entry["start"]
                if not rec.get("ordination_place_qid") and pos_entry["location_qid"]:
                    rec["ordination_place_qid"] = pos_entry["location_qid"]

    # --- Ordinator roles + dates on P1598 ---
    for batch in _chunks(qids, DETAILS_BATCH_SIZE):
        key = _cache_key("ordinators", batch[0], batch[-1], len(batch))
        print(f"[details ordinators] {batch[0]}…{batch[-1]} ({len(batch)})", file=sys.stderr)
        try:
            data = await sparql.query(build_ordinator_query(batch), key)
        except httpx.HTTPError as e:
            print(f"  [skip] ordinators batch failed: {e}", file=sys.stderr)
            continue
        for b in data["results"]["bindings"]:
            p_qid = _qid(b["p"]["value"])
            rec = records.get(p_qid)
            if rec is None:
                continue
            ord_qid = _qid(b["ordinator"]["value"])
            role_qid = _qid(b["role"]["value"]) if b.get("role") else None
            date = _date(b.get("date", {}).get("value"))
            if role_qid == ORDAINING_PRIEST_ROLE:
                if ord_qid not in rec["ordinateur_qids_role_explicit"]:
                    rec["ordinateur_qids_role_explicit"].append(ord_qid)
                if date and not rec.get("ordination_date"):
                    rec["ordination_date"] = date


# ---------------------------------------------------------------------------
# Cross-reference with eveques/ corpus
# ---------------------------------------------------------------------------


def load_eveques_index() -> dict[str, str]:
    """Build QID -> slug map from clerge/eveques/*.yaml.

    We do a *line-scan* parse (no YAML lib needed) — the wikidata QID lives on
    a predictable line ``  wikidata: Q...`` under the ``qids:`` block. Saves
    importing PyYAML and is robust to the simple known layout.
    """
    index: dict[str, str] = {}
    if not EVEQUES_DIR.exists():
        return index
    qid_re = re.compile(r"^\s*wikidata:\s*(Q\d+)\s*$")
    for path in EVEQUES_DIR.iterdir():
        if path.suffix != ".yaml":
            continue
        slug = path.stem
        try:
            for line in path.read_text(encoding="utf-8").splitlines():
                m = qid_re.match(line)
                if m:
                    index.setdefault(m.group(1), slug)
                    break
        except OSError:
            continue
    return index


def resolve_ordinator_slugs(rec: dict[str, Any], qid_to_slug: dict[str, str]) -> None:
    seen: list[str] = []
    for qid in rec.get("ordinateur_qids", []):
        slug = qid_to_slug.get(qid)
        if slug and slug not in seen:
            seen.append(slug)
    rec["ordinateur_slugs"] = seen


# ---------------------------------------------------------------------------
# JSONL I/O (idempotent, append-only)
# ---------------------------------------------------------------------------


def load_existing_qids(path: Path) -> set[str]:
    if not path.exists():
        return set()
    qids: set[str] = set()
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue
            sid = obj.get("source_id")
            if sid:
                qids.add(sid)
    return qids


def append_records(path: Path, records: Iterable[dict[str, Any]]) -> int:
    path.parent.mkdir(parents=True, exist_ok=True)
    n = 0
    with path.open("a", encoding="utf-8") as f:
        for r in records:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")
            n += 1
    return n


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


async def run(limit: int | None, refresh: bool, buckets: list[tuple[int | None, int | None]]) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    existing = load_existing_qids(OUT_PATH)
    if existing:
        print(f"[idempotent] {len(existing)} priests already in {OUT_PATH.name}", file=sys.stderr)

    async with httpx.AsyncClient(http2=True) as client:
        sparql = SparqlClient(client=client, refresh=refresh)

        # 1) Fetch all priest bindings from each birth-year bucket
        new_records: dict[str, dict[str, Any]] = {}
        for low, high in buckets:
            bindings = await fetch_bucket(sparql, low, high)
            for b in bindings:
                qid = _qid(b["p"]["value"])
                if qid in existing or qid in new_records:
                    continue
                new_records[qid] = _record_from_binding(b)
                if limit is not None and len(new_records) >= limit:
                    break
            if limit is not None and len(new_records) >= limit:
                break

        print(f"[total] {len(new_records)} raw priest rows fetched", file=sys.stderr)

        # 2) Enrich each record with positions + ordinator roles
        if new_records:
            await enrich_with_details(sparql, new_records)

        # 3) Cross-reference ordinators with the eveques corpus
        qid_to_slug = load_eveques_index()
        print(f"[xref] {len(qid_to_slug)} eveque QIDs indexed", file=sys.stderr)
        for rec in new_records.values():
            resolve_ordinator_slugs(rec, qid_to_slug)

        # 4) Filter empty / noncatholic / bishop rows
        kept: list[dict[str, Any]] = []
        rejected: dict[str, int] = {}
        for rec in new_records.values():
            keep, reason = _should_keep(rec)
            if keep:
                kept.append(rec)
            else:
                rejected[reason or "?"] = rejected.get(reason or "?", 0) + 1
        if rejected:
            print(f"[filter] rejected: {dict(sorted(rejected.items()))}", file=sys.stderr)
        print(f"[filter] kept {len(kept)} priests", file=sys.stderr)

        # 5) Append
        n_written = append_records(OUT_PATH, kept)
        print(f"[done] appended {n_written} records to {OUT_PATH}", file=sys.stderr)


def parse_buckets(spec: str) -> list[tuple[int | None, int | None]]:
    out: list[tuple[int | None, int | None]] = []
    for part in spec.split(","):
        part = part.strip()
        if part.lower() in {"nodob", "none"}:
            out.append((None, None))
            continue
        if "-" in part:
            lo, hi = part.split("-", 1)
            out.append((int(lo) if lo else None, int(hi) if hi else None))
        else:
            v = int(part)
            out.append((v, v))
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--limit", type=int, default=None, help="stop after N priests (sampling)")
    parser.add_argument("--refresh", action="store_true", help="ignore on-disk SPARQL cache")
    parser.add_argument(
        "--buckets",
        type=str,
        default=None,
        help="custom buckets, e.g. '1901-1910,1911-1920,noDoB' (default: full coverage)",
    )
    args = parser.parse_args()

    buckets = parse_buckets(args.buckets) if args.buckets else BIRTH_BUCKETS
    asyncio.run(run(limit=args.limit, refresh=args.refresh, buckets=buckets))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
