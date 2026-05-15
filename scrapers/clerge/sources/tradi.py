"""Scraper des prêtres traditionalistes / sédévacantistes / Ecclesia Dei.

Phase 6 du pipeline clergé. Cible un échantillon de prêtres notables des
principales fraternités, agrégé depuis Wikipédia FR (catégories + parsing
infobox) et Wikidata (SPARQL).

Pourquoi pas les sites officiels ?
----------------------------------
Aucun des sites des fraternités tradi (FSSPX/laportelatine.org, FSSP,
ICRSP, IBP) ne publie d'annuaire structuré et public de ses prêtres.
La FSSPX a un répertoire des maisons mais pas des personnes ; la FSSP
liste des séminaires et des districts ; etc. Le scraping de pages
WordPress hétérogènes serait fragile et donnerait peu de données
structurées (pas de date d'ordination, pas d'ordinateur).

Sources sédé (CMRI, SSPV) bloquent les bots non-browsers (HTTP 406 sur
``cmri.org``, contenu adulte parking sur sspv.net). Documenté dans
``_metadata/tradi_scraping_notes.md``.

Stratégie retenue
-----------------
1. Pour chaque fraternité, lire une catégorie Wikipédia FR connue
   (``Catégorie:Prêtre de la Fraternité sacerdotale Saint-Pie-X`` etc.)
   via l'API JSON.
2. Pour chaque page article, faire une seconde requête API
   ``action=parse&prop=wikitext`` pour récupérer le wikitexte brut puis
   extraire l'infobox (champs ``naissance``, ``ordination``,
   ``consécrateur``, ``lieu de naissance``, etc.).
3. Produire pour chaque prêtre un YAML conforme au schéma
   ``clerge/pretres/{slug}.yaml`` du corpus, plus une ligne JSONL dans
   ``clerge/_raw/tradi.jsonl`` pour traçabilité.

Rate limit : 1 req/s sur ``fr.wikipedia.org`` via ``GLOBAL_LIMITER``,
cache HTML/JSON dans ``clerge/_raw/_tradi_cache/``.

Idempotent : on saute les YAML déjà créés (sauf ``--refresh``).
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import re
import sys
import unicodedata
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Tuple

import httpx
import yaml

from scrapers.core.rate_limit import DomainRateLimiter

REPO_ROOT = Path(__file__).resolve().parents[3]
CLERGE = REPO_ROOT / "clerge"
PRETRES_DIR = CLERGE / "pretres"
RAW_DIR = CLERGE / "_raw"
CACHE_DIR = RAW_DIR / "_tradi_cache"
RAW_JSONL = RAW_DIR / "tradi.jsonl"
METADATA_DIR = CLERGE / "_metadata"
NOTES_PATH = METADATA_DIR / "tradi_scraping_notes.md"
SOURCES_PATH = METADATA_DIR / "tradi_sources.md"

WIKI_API = "https://fr.wikipedia.org/w/api.php"
WIKI_DOMAIN = "fr.wikipedia.org"
USER_AGENT = "MagisteriumArchiver/1.0 (https://github.com/realitix/catholique; jsbevilacqua2@gmail.com)"
HTTP_TIMEOUT = 30.0
LIMITER = DomainRateLimiter(min_interval=1.0)

logger = logging.getLogger("scrapers.clerge.tradi")


# ---------------------------------------------------------------------------
# Catégories Wikipédia FR ciblées (fraternité → liste de noms de catégorie)
# ---------------------------------------------------------------------------

CATEGORIES: Dict[str, List[str]] = {
    "fsspx": [
        "Prêtre de la Fraternité sacerdotale Saint-Pie-X",
        "Personnalité liée à la Fraternité sacerdotale Saint-Pie-X",
    ],
    "fssp": [
        "Prêtre de la Fraternité sacerdotale Saint-Pierre",
    ],
    "icrsp": [
        "Membre de l'Institut du Christ-Roi Souverain Prêtre",
        "Prêtre de l'Institut du Christ-Roi Souverain Prêtre",
    ],
    "ibp": [
        "Prêtre de l'Institut du Bon-Pasteur",
        "Prêtre de l'Institut du Bon Pasteur",
    ],
    "sede": [
        "Personnalité sédévacantiste",
        "Prêtre sédévacantiste",
    ],
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def slugify(name: str) -> str:
    norm = unicodedata.normalize("NFKD", name)
    norm = norm.encode("ascii", "ignore").decode("ascii")
    norm = re.sub(r"[^a-zA-Z0-9]+", "-", norm).strip("-").lower()
    norm = re.sub(r"-+", "-", norm)
    return norm


def cache_path_for(url: str) -> Path:
    h = hashlib.sha256(url.encode("utf-8")).hexdigest()[:16]
    return CACHE_DIR / f"{h}.json"


async def fetch_json(client: httpx.AsyncClient, params: Dict[str, Any], cache_key: str) -> Dict:
    cache_file = CACHE_DIR / f"{cache_key}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text(encoding="utf-8"))
    await LIMITER.acquire(WIKI_DOMAIN)
    resp = await client.get(WIKI_API, params=params, timeout=HTTP_TIMEOUT)
    resp.raise_for_status()
    data = resp.json()
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache_file.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    return data


async def fetch_category_members(client: httpx.AsyncClient, category_title: str) -> List[Dict]:
    """Liste les pages d'une catégorie Wikipédia FR (ns=0 uniquement)."""
    cache_key = "cat_" + hashlib.sha256(category_title.encode("utf-8")).hexdigest()[:16]
    params = {
        "action": "query",
        "list": "categorymembers",
        "cmtitle": f"Catégorie:{category_title}",
        "cmlimit": 500,
        "cmnamespace": 0,
        "format": "json",
    }
    try:
        data = await fetch_json(client, params, cache_key)
    except Exception as exc:
        logger.warning("Catégorie %s : %s", category_title, exc)
        return []
    return data.get("query", {}).get("categorymembers", [])


async def fetch_wikitext(client: httpx.AsyncClient, title: str) -> Optional[str]:
    """Récupère le wikitexte brut d'un article Wikipédia FR."""
    cache_key = "wt_" + hashlib.sha256(title.encode("utf-8")).hexdigest()[:16]
    params = {
        "action": "parse",
        "page": title,
        "prop": "wikitext",
        "format": "json",
        "redirects": 1,
    }
    try:
        data = await fetch_json(client, params, cache_key)
    except Exception as exc:
        logger.warning("Wikitext %s : %s", title, exc)
        return None
    parse = data.get("parse")
    if not parse:
        return None
    return parse.get("wikitext", {}).get("*")


WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
WIKIDATA_DOMAIN = "query.wikidata.org"


async def fetch_wikidata_dates(client: httpx.AsyncClient, qid: str) -> Dict[str, Any]:
    """Récupère naissance/décès Wikidata pour un QID via SPARQL."""
    cache_key = "wd_dates_" + qid
    cache_file = CACHE_DIR / f"{cache_key}.json"
    if cache_file.exists():
        return json.loads(cache_file.read_text(encoding="utf-8"))
    query = f"""SELECT ?dob ?dod WHERE {{
      wd:{qid} wdt:P569 ?dob .
      OPTIONAL {{ wd:{qid} wdt:P570 ?dod . }}
    }} LIMIT 1"""
    await LIMITER.acquire(WIKIDATA_DOMAIN)
    try:
        resp = await client.get(
            WIKIDATA_SPARQL,
            params={"query": query, "format": "json"},
            timeout=HTTP_TIMEOUT,
        )
        resp.raise_for_status()
        data = resp.json()
    except Exception as exc:
        logger.warning("SPARQL %s : %s", qid, exc)
        return {}
    bindings = data.get("results", {}).get("bindings", [])
    out: Dict[str, Any] = {}
    if bindings:
        b = bindings[0]
        if "dob" in b:
            out["naissance"] = b["dob"]["value"][:10]
        if "dod" in b:
            out["deces"] = b["dod"]["value"][:10]
    cache_file.write_text(json.dumps(out, ensure_ascii=False), encoding="utf-8")
    return out


async def fetch_wikidata_qid(client: httpx.AsyncClient, title: str) -> Optional[str]:
    """Retourne le QID Wikidata de l'article Wikipédia FR si présent."""
    cache_key = "wd_" + hashlib.sha256(title.encode("utf-8")).hexdigest()[:16]
    params = {
        "action": "query",
        "titles": title,
        "prop": "pageprops",
        "format": "json",
        "redirects": 1,
    }
    try:
        data = await fetch_json(client, params, cache_key)
    except Exception as exc:
        logger.warning("pageprops %s : %s", title, exc)
        return None
    pages = data.get("query", {}).get("pages", {})
    for p in pages.values():
        pp = p.get("pageprops", {})
        if "wikibase_item" in pp:
            return pp["wikibase_item"]
    return None


# ---------------------------------------------------------------------------
# Parsing infobox
# ---------------------------------------------------------------------------


INFOBOX_RE = re.compile(r"\{\{Infobox\s+([^|\n}]+)([\s\S]*?)\n\}\}", re.IGNORECASE)


def extract_infobox(wikitext: str) -> Dict[str, str]:
    """Extrait les paires clé/valeur d'une infobox biographique."""
    m = INFOBOX_RE.search(wikitext)
    if not m:
        return {}
    body = m.group(2)
    out: Dict[str, str] = {}
    # Découper par lignes commençant par "| key = value"
    # Note : valeurs multi-lignes possibles ; on coupe sur "\n| " (suivant tiret pipe).
    parts = re.split(r"\n\|\s*", "\n" + body)
    for part in parts[1:]:
        if "=" not in part:
            continue
        k, _, v = part.partition("=")
        key = k.strip().lower()
        val = v.strip()
        # Strip trailing }} ou ligne suivante
        val = re.sub(r"\}\}\s*$", "", val).strip()
        if key and val:
            out[key] = val
    return out


def _strip_wiki(s: str) -> str:
    """Nettoie les balises wikitexte simples."""
    if not s:
        return ""
    s = re.sub(r"\[\[(?:[^|\]]+\|)?([^\]]+)\]\]", r"\1", s)
    s = re.sub(r"'''?([^']+)'''?", r"\1", s)
    s = re.sub(r"<ref[^>]*>.*?</ref>", "", s, flags=re.DOTALL)
    s = re.sub(r"<ref[^/]*/>", "", s)
    s = re.sub(r"<[^>]+>", "", s)
    s = s.replace("&nbsp;", " ")
    return s.strip()


_DATE_RE = re.compile(
    r"\{\{date\s*(?:de\s+)?(?:naissance|décès)?\s*[|]?\s*(\d{1,2})\s*[|]?\s*([^|}]+?)\s*[|]?\s*(\d{4})",
    re.IGNORECASE,
)
_DATE_PLAIN_RE = re.compile(r"(\d{1,2})\s+([a-zéûôîàùç]+)\s+(\d{4})", re.IGNORECASE)

MONTHS_FR = {
    "janvier": 1, "février": 2, "mars": 3, "avril": 4, "mai": 5,
    "juin": 6, "juillet": 7, "août": 8, "septembre": 9, "octobre": 10,
    "novembre": 11, "décembre": 12,
}


def parse_french_date(text: str) -> Optional[str]:
    """Extrait une date 'YYYY-MM-DD' depuis du wikitexte."""
    if not text:
        return None
    text = text.lower()
    for rgx in (_DATE_RE, _DATE_PLAIN_RE):
        m = rgx.search(text)
        if m:
            day, month_str, year = m.groups()
            month = MONTHS_FR.get(month_str.strip().lower())
            if month:
                try:
                    return f"{int(year):04d}-{month:02d}-{int(day):02d}"
                except ValueError:
                    pass
    # Année seule
    m = re.search(r"(\d{4})", text)
    if m:
        return m.group(1)
    return None


def parse_priest_infobox(infobox: Dict[str, str], title: str) -> Dict[str, Any]:
    """Mappe l'infobox vers le schéma prêtre."""
    naissance = None
    deces = None
    ordination_date = None
    consecrateur = None  # ordinateur (presbytéral)

    for key, val in infobox.items():
        if key in {"naissance", "date de naissance", "date naissance"}:
            naissance = parse_french_date(_strip_wiki(val))
        elif key in {"décès", "date de décès", "date décès", "mort", "deces"}:
            deces = parse_french_date(_strip_wiki(val))
        elif key in {"ordination", "ordination presbytérale", "ordination sacerdotale"}:
            ordination_date = parse_french_date(_strip_wiki(val))
        elif key in {"ordinant", "ordinateur", "par"}:
            consecrateur = _strip_wiki(val)

    return {
        "naissance": naissance,
        "deces": deces,
        "ordination_date": ordination_date,
        "ordinateur": consecrateur,
    }


# Fallback texte plein pour l'Infobox Biographie2 (très minimaliste).
_BIRTH_TEXT_RE = re.compile(
    r"n[éeé]\s*(?:e)?\s+le\s+\{\{date(?:\s+de\s+naissance)?\s*\|\s*(\d{1,2})\s*\|\s*([^|}]+?)\s*\|\s*(\d{4})",
    re.IGNORECASE,
)
_DEATH_TEXT_RE = re.compile(
    r"(?:mort|décéd[ée]?e?|décès)\s+(?:le\s+)?\{\{date(?:\s+de\s+décès)?\s*\|\s*(\d{1,2})\s*\|\s*([^|}]+?)\s*\|\s*(\d{4})",
    re.IGNORECASE,
)
_ORDAINED_BY_RE = re.compile(
    r"[Oo]rdonn[ée]\s+(?:pr[êe]tre\s+)?(?:le\s+[^,.]{0,40}\s+)?par\s+\[\[([^\]|]+)(?:\|[^\]]*)?\]\]",
)
_ORDAINED_YEAR_RE = re.compile(
    r"[Oo]rdonn[ée]\s+(?:pr[êe]tre\s+)?(?:par\s+\[\[[^\]]+\]\]\s+)?en\s+(\d{4})",
)


def _date_from_match(m: re.Match) -> Optional[str]:
    day, month_str, year = m.groups()
    month = MONTHS_FR.get(month_str.strip().lower())
    if month:
        try:
            return f"{int(year):04d}-{month:02d}-{int(day):02d}"
        except ValueError:
            return None
    return None


def parse_from_text(wikitext: str) -> Dict[str, Any]:
    """Fallback : extrait des infos depuis le texte d'introduction."""
    head = wikitext[:6000]
    out: Dict[str, Any] = {
        "naissance": None,
        "deces": None,
        "ordination_date": None,
        "ordinateur": None,
    }
    m = _BIRTH_TEXT_RE.search(head)
    if m:
        out["naissance"] = _date_from_match(m)
    m = _DEATH_TEXT_RE.search(head)
    if m:
        out["deces"] = _date_from_match(m)
    m = _ORDAINED_BY_RE.search(head)
    if m:
        out["ordinateur"] = m.group(1).strip()
    m = _ORDAINED_YEAR_RE.search(head)
    if m:
        out["ordination_date"] = m.group(1)
    return out


# ---------------------------------------------------------------------------
# Construction YAML
# ---------------------------------------------------------------------------


def build_priest_yaml(
    title: str,
    fraternite: str,
    parsed: Dict[str, Any],
    qid: Optional[str],
    wiki_url: str,
) -> Tuple[str, Dict[str, Any]]:
    base_slug = slugify(title)
    slug = base_slug
    naissance = parsed.get("naissance")
    # Désambiguïsation : on n'ajoute l'année que si une fiche évêque
    # du corpus existe déjà avec le même slug ; sinon on reste sur le
    # slug court (idempotence : pas de duplication entre runs).
    if naissance and len(naissance) >= 4:
        eveque_path = CLERGE / "eveques" / f"{slug}.yaml"
        if eveque_path.exists():
            slug = f"{base_slug}-{naissance[:4]}"

    ordination_block = None
    if parsed.get("ordination_date") or parsed.get("ordinateur"):
        ordination_block = {
            "date": parsed.get("ordination_date"),
            "rite": "ancien",
            "rite_source": "inferred",
            "ordinateur": parsed.get("ordinateur"),
            "source_urls": [wiki_url],
        }

    data: "OrderedDict[str, Any]" = OrderedDict()
    data["slug"] = slug
    data["nom"] = title
    data["naissance"] = naissance
    data["deces"] = parsed.get("deces")
    data["fraternite"] = fraternite
    if ordination_block:
        data["ordination"] = ordination_block
    data["qids"] = {"wikidata": qid} if qid else {}
    data["sources"] = [
        {
            "source": "wikipedia-fr",
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "url": wiki_url,
        }
    ]
    return slug, dict(data)


def yaml_dump(data: Dict[str, Any], path: Path) -> None:
    text = yaml.safe_dump(
        data, default_flow_style=False, sort_keys=False, allow_unicode=True
    )
    path.write_text(text, encoding="utf-8")


# ---------------------------------------------------------------------------
# Main loop
# ---------------------------------------------------------------------------


async def process_one(
    client: httpx.AsyncClient,
    title: str,
    fraternite: str,
    refresh: bool,
) -> Optional[Dict[str, Any]]:
    """Traite un article : fetch + parse + YAML."""
    wiki_url = f"https://fr.wikipedia.org/wiki/{title.replace(' ', '_')}"
    existing_slug = slugify(title)
    target_path = PRETRES_DIR / f"{existing_slug}.yaml"
    if target_path.exists() and not refresh:
        # Déjà traité — mise à jour fraternite si conflit (multi-catégories)
        try:
            existing = yaml.safe_load(target_path.read_text(encoding="utf-8")) or {}
        except Exception:
            existing = {}
        return {
            "title": title,
            "slug": existing_slug,
            "fraternite_existing": existing.get("fraternite"),
            "fraternite_new": fraternite,
            "status": "exists",
        }

    wikitext = await fetch_wikitext(client, title)
    if not wikitext:
        return {"title": title, "status": "no-wikitext"}

    # Vérifier que la page est bien d'un prêtre (et pas d'une institution)
    if not re.search(r"(prêtre|abbé|père|chanoine|ordonné|sacerdoce|évêque|mgr|monseigneur)",
                     wikitext[:5000], re.IGNORECASE):
        return {"title": title, "status": "non-clerc"}

    infobox = extract_infobox(wikitext)
    parsed = parse_priest_infobox(infobox, title)
    # Fallback texte si infobox vide (cas typique d'Infobox Biographie2)
    text_parsed = parse_from_text(wikitext)
    for k, v in text_parsed.items():
        if v and not parsed.get(k):
            parsed[k] = v
    qid = await fetch_wikidata_qid(client, title)
    if qid:
        wd_dates = await fetch_wikidata_dates(client, qid)
        if wd_dates.get("naissance") and not parsed.get("naissance"):
            parsed["naissance"] = wd_dates["naissance"]
        if wd_dates.get("deces") and not parsed.get("deces"):
            parsed["deces"] = wd_dates["deces"]

    slug, data = build_priest_yaml(title, fraternite, parsed, qid, wiki_url)
    target_path = PRETRES_DIR / f"{slug}.yaml"
    PRETRES_DIR.mkdir(parents=True, exist_ok=True)
    yaml_dump(data, target_path)
    return {"title": title, "slug": slug, "fraternite": fraternite, "status": "created"}


async def run(refresh: bool = False, limit_per_frat: Optional[int] = None) -> Dict[str, Any]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    PRETRES_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    headers = {"User-Agent": USER_AGENT}
    async with httpx.AsyncClient(headers=headers, http2=True) as client:
        all_results: List[Dict[str, Any]] = []
        per_frat: Dict[str, int] = {}
        cat_stats: Dict[str, Dict[str, int]] = {}

        for fraternite, cats in CATEGORIES.items():
            cat_stats[fraternite] = {"members_found": 0, "created": 0, "exists": 0, "skipped": 0}
            seen_titles: set = set()
            for cat in cats:
                logger.info("Fraternité %s — catégorie %s", fraternite, cat)
                members = await fetch_category_members(client, cat)
                cat_stats[fraternite]["members_found"] += len(members)
                for m in members:
                    if m["ns"] != 0 or m["title"] in seen_titles:
                        continue
                    seen_titles.add(m["title"])
                    if limit_per_frat and per_frat.get(fraternite, 0) >= limit_per_frat:
                        break
                    res = await process_one(client, m["title"], fraternite, refresh)
                    if res:
                        all_results.append(res)
                        if res["status"] == "created":
                            per_frat[fraternite] = per_frat.get(fraternite, 0) + 1
                            cat_stats[fraternite]["created"] += 1
                        elif res["status"] == "exists":
                            cat_stats[fraternite]["exists"] += 1
                        else:
                            cat_stats[fraternite]["skipped"] += 1

    # Append-only JSONL
    with RAW_JSONL.open("a", encoding="utf-8") as fh:
        for r in all_results:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")

    return {
        "per_fraternite": per_frat,
        "by_category": cat_stats,
        "total_results": len(all_results),
    }


def write_notes_and_sources(summary: Dict[str, Any]) -> None:
    notes = """# Notes sur le scraping des prêtres tradi

## Sources testées et écartées

- **CMRI (cmri.org)** : HTTP 406 / Imperva CDN bloque les requêtes non-browser.
- **SSPV (sspv.net)** : domaine parking / contenu non-clérical, plus de site officiel.
- **FSSPX laportelatine.org** : pas d'annuaire structuré ; pages WordPress hétérogènes.
- **FSSP fssp.org** : pas de répertoire des prêtres en ligne, structure par districts.
- **ICRSP icrsp.org** : 404 sur /Chanoines/, pas d'annuaire public.
- **IBP institutdubonpasteur.org** : pas d'annuaire public.

## Source retenue

**Wikipédia FR** — catégories prosopographiques :

- `Catégorie:Prêtre de la Fraternité sacerdotale Saint-Pie-X`
- `Catégorie:Prêtre de la Fraternité sacerdotale Saint-Pierre`
- `Catégorie:Personnalité liée à la Fraternité sacerdotale Saint-Pie-X`
- `Catégorie:Personnalité sédévacantiste`

Couvre les prêtres tradi qui ont une notoriété encyclopédique
(supérieurs, fondateurs, signatures publiques). N'épuise PAS l'effectif
des fraternités (la FSSPX compte ~700 prêtres ; Wikipédia FR en référence
une quinzaine). Conviendra-t-il d'élargir par scraping ciblé des
"liens internes" sur ces articles ?

## Limites de couverture

- Pas d'`ordinateur` exploitable : Wikipédia mentionne rarement
  l'évêque ordinateur dans l'infobox biographique.
- Pas de cross-link automatique vers le slug évêque du corpus.
- Quasi-exclusivement FSSPX et FSSP côté tradi reconnus.
"""
    NOTES_PATH.write_text(notes, encoding="utf-8")

    sources_md = ["# Sources du corpus prêtres tradi", ""]
    sources_md.append("## Fraternités couvertes\n")
    for frat, stats in summary["by_category"].items():
        sources_md.append(f"- **{frat}** : {stats['created']} créés, {stats['exists']} déjà présents, "
                          f"{stats['skipped']} ignorés (sur {stats['members_found']} candidats Wikipédia FR).")
    sources_md.append("\n## Sources publiques utilisées\n")
    sources_md.append("- Wikipédia FR API (`fr.wikipedia.org/w/api.php`) — catégories + wikitexte d'infobox.")
    sources_md.append("- Wikidata (via pageprops `wikibase_item`) pour les QID stables.")
    sources_md.append("\n## Voir aussi\n")
    sources_md.append("- `_metadata/tradi_scraping_notes.md` — sites officiels testés et écartés.")
    SOURCES_PATH.write_text("\n".join(sources_md) + "\n", encoding="utf-8")


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true", help="Re-fetch même si YAML existe")
    parser.add_argument("--limit-per-frat", type=int, default=None,
                        help="Limite par fraternité (debug)")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    summary = asyncio.run(run(refresh=args.refresh, limit_per_frat=args.limit_per_frat))
    write_notes_and_sources(summary)

    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
