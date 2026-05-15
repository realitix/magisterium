"""Phase 1 — Wikidata SPARQL scraper for Catholic bishops.

Pulls every human (Q5) whose occupation (P106) is Catholic bishop (Q611644).
Paginates by birth-century buckets (with finer sub-buckets when the bucket is
too dense) plus a separate "unknown birth date" bucket, so each SPARQL request
stays well below the 60s public endpoint timeout.

Output: ``clerge/_raw/wikidata.jsonl`` (one bishop per line).

Idempotent:
  - Existing ``source_id`` (QID) entries are loaded and skipped on re-run.
  - Raw JSON responses from the SPARQL endpoint are cached under
    ``clerge/_raw/_wikidata_cache/`` so the script can be replayed without
    hammering query.wikidata.org.

Usage::

    uv run python -m scrapers.clerge.sources.wikidata             # full run
    uv run python -m scrapers.clerge.sources.wikidata --limit 500 # sample
    uv run python -m scrapers.clerge.sources.wikidata --refresh   # ignore cache
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import httpx

ROOT = Path(__file__).resolve().parents[3]
RAW_DIR = ROOT / "clerge" / "_raw"
CACHE_DIR = RAW_DIR / "_wikidata_cache"
OUT_PATH = RAW_DIR / "wikidata.jsonl"

ENDPOINT = "https://query.wikidata.org/sparql"
USER_AGENT = (
    "MagisteriumArchiver/1.0 "
    "(https://github.com/realitix/catholique; contact via repo)"
)

# Rate limit: at most 5 req/s -> min 0.2s spacing. We use 0.25s to be polite.
MIN_INTERVAL = 0.25

# SPARQL endpoint timeout (seconds). Wikidata public limit ~60s.
SPARQL_TIMEOUT = 70.0

# Occupations we accept (P106) — only Catholic bishop is the canonical class.
# We could expand to include Q49476 (archbishop) etc., but ~95% of Catholic
# archbishops also carry Q611644. Keep the seed narrow; phase 4 (reconcile)
# may broaden via catholic-hierarchy.org.
BISHOP_OCCUPATION = "Q611644"

# Birth-year buckets. Ranges are inclusive on both ends. ``None`` = unknown DoB.
# Pre-1500 sees fewer entries, so we use larger buckets there.
BIRTH_BUCKETS: list[tuple[int | None, int | None]] = [
    (None, None),   # bishops with no birth date on Wikidata (~12k entries)
    (None, 1000),
    (1001, 1300),
    (1301, 1500),
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
       (GROUP_CONCAT(DISTINCT ?consecrator;separator="|") AS ?consecrators)
       (SAMPLE(?image) AS ?image)
       (GROUP_CONCAT(DISTINCT ?order;separator="|") AS ?orders)
       (GROUP_CONCAT(DISTINCT ?religion;separator="|") AS ?religions)
       (SAMPLE(?wpFr) AS ?wpFr)
WHERE {
  ?p wdt:P31 wd:Q5 ;
     wdt:P106 wd:Q611644 .
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
  OPTIONAL { ?p wdt:P1598 ?consecrator . }
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
    """Build a SPARQL filter clause for the birth-year bucket."""
    if low is None and high is None:
        # Unknown birth date
        return "FILTER NOT EXISTS { ?p wdt:P569 ?_anyDob . }"
    parts = ["?p wdt:P569 ?_filterDob ."]
    if low is not None:
        parts.append(f"FILTER(YEAR(?_filterDob) >= {low})")
    if high is not None:
        parts.append(f"FILTER(YEAR(?_filterDob) <= {high})")
    return "\n  ".join(parts)


def build_query(low: int | None, high: int | None) -> str:
    return _BASE_QUERY.replace("##BIRTH_FILTER##", _birth_filter(low, high))


# Second-pass query: for a batch of QIDs, fetch P39 positions + their date
# qualifiers + the inferred episcopal consecration date.
_DETAILS_QUERY = """
SELECT ?p ?position ?positionLabel ?start ?end ?role
WHERE {
  VALUES ?p { ##QIDS## }
  ?p p:P39 ?st .
  ?st ps:P39 ?position .
  OPTIONAL { ?st pq:P580 ?start . }
  OPTIONAL { ?st pq:P582 ?end . }
  OPTIONAL { ?st pq:P3831 ?role . }
  SERVICE wikibase:label { bd:serviceParam wikibase:language "fr,en". }
}
"""


def build_details_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return _DETAILS_QUERY.replace("##QIDS##", values)


# Second-pass query: consecrator role qualifier (principal vs co-) on P1598.
_CONSECRATOR_ROLE_QUERY = """
SELECT ?p ?consecrator ?role ?date
WHERE {
  VALUES ?p { ##QIDS## }
  ?p p:P1598 ?st .
  ?st ps:P1598 ?consecrator .
  OPTIONAL { ?st pq:P3831 ?role . }
  OPTIONAL { ?st pq:P585 ?date . }
}
"""


def build_consecrator_query(qids: list[str]) -> str:
    values = " ".join(f"wd:{q}" for q in qids)
    return _CONSECRATOR_ROLE_QUERY.replace("##QIDS##", values)


# ---------------------------------------------------------------------------
# SPARQL client with rate limit + on-disk cache
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
        # Rate limit
        wait = MIN_INTERVAL - (time.monotonic() - self.last_call)
        if wait > 0:
            await asyncio.sleep(wait)
        # Retry on 429 / 5xx with exponential backoff.
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
# Result post-processing
# ---------------------------------------------------------------------------


def _qid(uri: str) -> str:
    return uri.rsplit("/", 1)[-1]


def _split_concat(value: str) -> list[str]:
    if not value:
        return []
    return [v.strip() for v in value.split("|") if v.strip()]


def _date(value: str | None) -> str | None:
    """Wikidata returns ISO 8601 like ``+1947-09-18T00:00:00Z`` or with BC sign."""
    if not value:
        return None
    v = value.lstrip("+")
    # Truncate to date if time is just 00:00:00Z
    return v[:10] if "T" in v else v


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

    consecrator_uris = _split_concat(b.get("consecrators", {}).get("value", ""))
    nationality_uris = _split_concat(b.get("nationalities", {}).get("value", ""))
    order_uris = _split_concat(b.get("orders", {}).get("value", ""))
    religion_uris = _split_concat(b.get("religions", {}).get("value", ""))

    return {
        "source": "wikidata",
        "source_id": qid,
        "names": names,
        "description": description,
        "birth_date": _date(b.get("dob", {}).get("value")),
        "birth_place_qid": _qid(b["birthPlace"]["value"]) if b.get("birthPlace") else None,
        "death_date": _date(b.get("dod", {}).get("value")),
        "death_place_qid": _qid(b["deathPlace"]["value"]) if b.get("deathPlace") else None,
        "nationality_qids": [_qid(u) for u in nationality_uris],
        "consecrator_qids": [_qid(u) for u in consecrator_uris],
        "consecrator_principal_qid": None,  # filled by details pass
        "co_consecrator_qids": [],          # filled by details pass
        "consecration_date": None,          # filled by details pass
        "image_url": b.get("image", {}).get("value") or None,
        "religious_order_qids": [_qid(u) for u in order_uris],
        "positions": [],                    # filled by details pass
        "religion_qids": [_qid(u) for u in religion_uris],
        "religion_qid": _qid(religion_uris[0]) if religion_uris else None,
        "wikipedia_fr_url": b.get("wpFr", {}).get("value") or None,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# ---------------------------------------------------------------------------
# Cache-key helper
# ---------------------------------------------------------------------------


def _cache_key(prefix: str, *parts: Any) -> str:
    raw = f"{prefix}:" + ":".join(str(p) for p in parts)
    h = hashlib.sha1(raw.encode("utf-8")).hexdigest()[:10]
    safe = raw.replace(":", "_").replace("/", "_")[:80]
    return f"{safe}_{h}"


# ---------------------------------------------------------------------------
# Bucket fetching with auto-split on near-limit results
# ---------------------------------------------------------------------------

# Wikidata caps GROUP_CONCAT-style queries at ~10k rows in practice; we split
# if we see more than this in a bucket.
SOFT_BUCKET_LIMIT = 9000


async def fetch_bucket(
    sparql: SparqlClient, low: int | None, high: int | None
) -> list[dict[str, Any]]:
    label = f"{low}-{high}" if low is not None or high is not None else "noDoB"
    key = _cache_key("bishops", low, high)
    print(f"[bucket {label}] querying…", file=sys.stderr)
    data = await sparql.query(build_query(low, high), key)
    bindings = data["results"]["bindings"]
    print(f"[bucket {label}] -> {len(bindings)} rows", file=sys.stderr)
    if len(bindings) >= SOFT_BUCKET_LIMIT and low is not None and high is not None and high > low:
        # Split bucket in half and re-fetch
        mid = (low + high) // 2
        print(f"[bucket {label}] above soft limit, splitting", file=sys.stderr)
        a = await fetch_bucket(sparql, low, mid)
        b = await fetch_bucket(sparql, mid + 1, high)
        # Dedup by QID
        seen: dict[str, dict[str, Any]] = {}
        for b_ in (a + b):
            seen[b_["p"]["value"]] = b_
        return list(seen.values())
    return bindings


# ---------------------------------------------------------------------------
# Detail enrichment (positions + consecrator roles)
# ---------------------------------------------------------------------------


DETAILS_BATCH_SIZE = 50


def _chunks(seq: list[str], n: int) -> Iterable[list[str]]:
    for i in range(0, len(seq), n):
        yield seq[i : i + n]


CONSECRATOR_PRINCIPAL_ROLE = "Q18442817"


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
            rec["positions"].append({
                "position_qid": _qid(b["position"]["value"]),
                "label_fr": b.get("positionLabel", {}).get("value"),
                "start": _date(b.get("start", {}).get("value")),
                "end": _date(b.get("end", {}).get("value")),
            })

    # --- Consecrator roles + consecration dates ---
    for batch in _chunks(qids, DETAILS_BATCH_SIZE):
        key = _cache_key("consecrators", batch[0], batch[-1], len(batch))
        print(f"[details consecrators] {batch[0]}…{batch[-1]} ({len(batch)})", file=sys.stderr)
        try:
            data = await sparql.query(build_consecrator_query(batch), key)
        except httpx.HTTPError as e:
            print(f"  [skip] consecrators batch failed: {e}", file=sys.stderr)
            continue
        for b in data["results"]["bindings"]:
            p_qid = _qid(b["p"]["value"])
            rec = records.get(p_qid)
            if rec is None:
                continue
            cons_qid = _qid(b["consecrator"]["value"])
            role_qid = _qid(b["role"]["value"]) if b.get("role") else None
            date = _date(b.get("date", {}).get("value"))
            if role_qid == CONSECRATOR_PRINCIPAL_ROLE:
                rec["consecrator_principal_qid"] = cons_qid
            elif role_qid is not None:
                if cons_qid not in rec["co_consecrator_qids"]:
                    rec["co_consecrator_qids"].append(cons_qid)
            if date and not rec["consecration_date"]:
                rec["consecration_date"] = date

    # --- Infer consecration_date from earliest episcopal P39 start when missing ---
    EPISCOPAL_POSITIONS = {
        "Q948657",      # évêque titulaire / titular bishop
        "Q611644",      # catholic bishop
        "Q43229",       # (sometimes used)
        "Q29182",       # bishop
        "Q49476",       # archbishop
        "Q50362553",    # titular archbishop
        "Q1144713",     # catholic bishop (alt)
        "Q15253909",    # vicaire apostolique
        "Q1993358",     # administrator apostolic
    }
    for rec in records.values():
        if rec["consecration_date"]:
            continue
        starts = [
            p["start"] for p in rec["positions"]
            if p.get("start") and p.get("position_qid") in EPISCOPAL_POSITIONS
        ]
        if starts:
            rec["consecration_date"] = min(starts)


# ---------------------------------------------------------------------------
# Idempotent JSONL I/O
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
# Main entrypoint
# ---------------------------------------------------------------------------


async def run(limit: int | None, refresh: bool, buckets: list[tuple[int | None, int | None]]) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    existing = load_existing_qids(OUT_PATH)
    if existing:
        print(f"[idempotent] {len(existing)} bishops already in {OUT_PATH.name}", file=sys.stderr)

    async with httpx.AsyncClient(http2=True) as client:
        sparql = SparqlClient(client=client, refresh=refresh)

        # 1) Fetch all bishop bindings from each birth-year bucket
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

        print(f"[total] {len(new_records)} new bishops to enrich", file=sys.stderr)

        # 2) Enrich each record with positions + consecrator roles
        if new_records:
            await enrich_with_details(sparql, new_records)

        # 3) Append to JSONL
        n_written = append_records(OUT_PATH, new_records.values())
        print(f"[done] appended {n_written} records to {OUT_PATH}", file=sys.stderr)


def parse_buckets(spec: str) -> list[tuple[int | None, int | None]]:
    """Parse ``--buckets`` arg: comma-separated ``low-high`` or ``noDoB``."""
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
    parser.add_argument("--limit", type=int, default=None, help="stop after N bishops (sampling)")
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
