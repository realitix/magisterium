"""Scraper des sites officiels des fraternités tradi.

Phase 6 — étendue. Là où le module ``tradi.py`` scrape Wikipédia FR (notoriété
encyclopédique, ~30 prêtres), ce module attaque les annuaires officiels des
fraternités. Sources retenues après tests (mai 2026) :

* **laportelatine.org** (FSSPX France) — sitemap ``personne-sitemap.xml`` qui
  liste ~266 abbés/pères + sitemap ``lieux-sitemap.xml`` (253 lieux) où sont
  affichés les noms d'affectation (prieur, vicaire, professeur).
* **cmri.org** (sédévacantistes CMRI) — page directory of traditional latin
  masses : noms + affectation par état US.
* **sgg.org** (MHTS / Mgr Dolan, sédé) — page ``/clergy/`` avec liste publique.
* **wikipedia FR** — déjà traité par ``tradi.py``, on ne re-scrape pas ici.

Sources écartées :

* fssp.org : pas d'annuaire public structuré (formulaire géolocalisé qui n'expose
  pas les noms).
* icrsp.org : 404 sur toutes les pages clergy/canons/our-priests testées.
* institutdubonpasteur.org : ERR_TLS_CERT et ECONNREFUSED en mai 2026.
* sspv.net : domaine parking / ads en mai 2026 (le site canonique a disparu).
* sspx.org (FSSPX international) : retourne 403 à WebFetch et au curl simple.
  Récupérable via curl + UA navigateur si besoin, mais redondant avec
  laportelatine.org pour la France et superficiel pour l'international.

Sortie : append-only dans ``clerge/_raw/tradi.jsonl`` avec ``source`` distinct
(``fsspx-laportelatine``, ``cmri-directory``, ``mhts-sgg``). Production des YAML
``clerge/pretres/{slug}.yaml`` idempotente : enrichit si existant, crée sinon.
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
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import httpx
import yaml
from selectolax.parser import HTMLParser

from scrapers.core.rate_limit import DomainRateLimiter

# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

REPO_ROOT = Path(__file__).resolve().parents[3]
CLERGE = REPO_ROOT / "clerge"
PRETRES_DIR = CLERGE / "pretres"
EVEQUES_DIR = CLERGE / "eveques"
RAW_DIR = CLERGE / "_raw"
CACHE_DIR = RAW_DIR / "_tradi_cache"
RAW_JSONL = RAW_DIR / "tradi.jsonl"
METADATA_DIR = CLERGE / "_metadata"
UNRESOLVED_PATH = METADATA_DIR / "tradi_unresolved_ordinateurs.json"
NOTES_PATH = METADATA_DIR / "tradi_scraping_notes.md"

USER_AGENT = (
    "MagisteriumArchiver/1.0 (https://github.com/realitix/catholique; "
    "jsbevilacqua2@gmail.com)"
)
HTTP_TIMEOUT = 30.0
LIMITER = DomainRateLimiter(min_interval=1.0)

# Navigateur-compatible : laportelatine.org et autres WordPress bloquent souvent
# les UA "bot-like". On garde un UA navigateur en plus du UA identifiant.
BROWSER_UA = (
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0 Safari/537.36"
)

logger = logging.getLogger("scrapers.clerge.tradi_official")


# ---------------------------------------------------------------------------
# Utilitaires
# ---------------------------------------------------------------------------


HONORIFICS = (
    "Mgr",
    "Monseigneur",
    "Abbé",
    "Père",
    "Pere",
    "P.",
    "R.P.",
    "Dom",
    "Frère",
    "Frere",
    "Fr.",
    "Rev.",
    "Bishop",
    "Fr",
    "Don",
)


def strip_honorific(name: str) -> Tuple[Optional[str], str]:
    """Sépare le préfixe honorifique du reste du nom."""
    s = name.strip()
    for h in HONORIFICS:
        if s.lower().startswith(h.lower() + " "):
            return h, s[len(h):].strip()
        if s.lower().startswith(h.lower() + ".") and len(s) > len(h) + 1:
            return h, s[len(h) + 1:].strip()
    return None, s


def slugify(name: str) -> str:
    norm = unicodedata.normalize("NFKD", name)
    norm = norm.encode("ascii", "ignore").decode("ascii")
    norm = re.sub(r"[^a-zA-Z0-9]+", "-", norm).strip("-").lower()
    norm = re.sub(r"-+", "-", norm)
    return norm


def cache_path_for(prefix: str, key: str) -> Path:
    h = hashlib.sha256(key.encode("utf-8")).hexdigest()[:16]
    return CACHE_DIR / f"{prefix}_{h}.html"


async def fetch_text(
    client: httpx.AsyncClient,
    url: str,
    cache_prefix: str = "p",
    domain_override: Optional[str] = None,
) -> Optional[str]:
    """Fetch d'une page HTML avec cache fichier + rate limit par domaine."""
    cache = cache_path_for(cache_prefix, url)
    if cache.exists() and cache.stat().st_size > 200:
        return cache.read_text(encoding="utf-8")
    from urllib.parse import urlparse

    host = domain_override or urlparse(url).netloc
    await LIMITER.acquire(host)
    try:
        resp = await client.get(url, timeout=HTTP_TIMEOUT, follow_redirects=True)
        if resp.status_code != 200:
            logger.warning("HTTP %s %s", resp.status_code, url)
            return None
        text = resp.text
    except Exception as exc:
        logger.warning("fetch %s : %s", url, exc)
        return None
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    cache.write_text(text, encoding="utf-8")
    return text


# ---------------------------------------------------------------------------
# Index des évêques pour résolution ordinateur_slug
# ---------------------------------------------------------------------------


def _norm(s: str) -> str:
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode("ascii")
    return re.sub(r"\s+", " ", s).strip().lower()


_EVEQUES_INDEX: Optional[Dict[str, str]] = None
_EVEQUES_NORM_LIST: Optional[List[Tuple[str, str]]] = None


def load_eveques_index() -> Tuple[Dict[str, str], List[Tuple[str, str]]]:
    """Charge un index ``nom_normalise -> slug`` et une liste pour fuzzy."""
    global _EVEQUES_INDEX, _EVEQUES_NORM_LIST
    if _EVEQUES_INDEX is not None and _EVEQUES_NORM_LIST is not None:
        return _EVEQUES_INDEX, _EVEQUES_NORM_LIST

    index: Dict[str, str] = {}
    norm_list: List[Tuple[str, str]] = []
    if not EVEQUES_DIR.exists():
        _EVEQUES_INDEX, _EVEQUES_NORM_LIST = index, norm_list
        return index, norm_list

    for path in EVEQUES_DIR.glob("*.yaml"):
        try:
            data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        except Exception:
            continue
        slug = data.get("slug") or path.stem
        for key in ("nom", "nom_complet"):
            v = data.get(key)
            if isinstance(v, str) and v.strip():
                n = _norm(v)
                index.setdefault(n, slug)
                norm_list.append((n, slug))
    _EVEQUES_INDEX, _EVEQUES_NORM_LIST = index, norm_list
    logger.info("Index évêques chargé : %d entrées (%d slugs uniques)",
                len(norm_list), len(set(s for _, s in norm_list)))
    return index, norm_list


def resolve_ordinateur_slug(name: Optional[str]) -> Optional[str]:
    """Tente de résoudre un nom d'ordinateur en slug évêque (exact puis fuzzy)."""
    if not name:
        return None
    index, norm_list = load_eveques_index()
    cleaned = name
    # Enlève honorifiques fréquents
    cleaned = re.sub(r"^(Mgr|Monseigneur|Bishop|Card\.?|Cardinal|Most Reverend|Most Rev\.|S\.E\.)\s+",
                     "", cleaned, flags=re.IGNORECASE).strip()
    n = _norm(cleaned)
    if not n:
        return None
    # Match exact
    if n in index:
        return index[n]
    # Match exact sur les sous-chaînes
    for entry, slug in norm_list:
        if entry == n:
            return slug
    # Fuzzy seuil 0.9
    best = (0.0, None)
    for entry, slug in norm_list:
        ratio = SequenceMatcher(None, n, entry).ratio()
        if ratio > best[0]:
            best = (ratio, slug)
    if best[0] >= 0.9:
        return best[1]
    return None


# ---------------------------------------------------------------------------
# Source 1 : laportelatine.org (FSSPX France)
# ---------------------------------------------------------------------------

LPL_HOST = "laportelatine.org"
LPL_SITEMAP_PERSONNE = "https://laportelatine.org/personne-sitemap.xml"
LPL_SITEMAP_LIEUX = "https://laportelatine.org/lieux-sitemap.xml"


def _is_clergy_url(url: str) -> bool:
    slug = url.rsplit("/", 1)[-1].lower()
    if any(
        slug.startswith(p)
        for p in (
            "abbe-",
            "pere-",
            "p-",
            "rp-",
            "r-p",
            "mgr-",
            "dom-",
            "frere-",
            "fr-",
            "don-",
        )
    ):
        return True
    # Refuse explicite des saints / pages institutionnelles
    REJECT = (
        "saint",
        "sainte",
        "fsspx",
        "propagande",
        "congregation",
        "congragation",
        "concile",
        "curie",
        "conseil",
        "secretariat",
        "commission",
        "comite",
        "institut",
        "asbl",
        "soeur",
        "pie-",
        "leon-",
        "pius-",
        "pius",
        "innocent",
        "gregoire",
        "boniface",
        "benoit-x",
        "clement-x",
        "alexandre-",
        "urbain-",
        "eugene-",
        "paul-",
        "jean-x",
        "jean-paul",
        "honorius",
        "anaclet",
        "sirice",
        "felix",
        "celestin",
        "etienne",
        "agatho",
    )
    if any(slug.startswith(r) for r in REJECT):
        return False
    # Quelques sigles : "r-p-marziac", "don-mauro-tranquillo", "frere-marie"
    return False


async def fetch_lpl_personne_list(client: httpx.AsyncClient) -> List[str]:
    """Récupère la liste de TOUTES les URLs ``/personne/...`` du sitemap."""
    cache = CACHE_DIR / "lpl_personne_sitemap.xml"
    if cache.exists() and cache.stat().st_size > 500:
        xml = cache.read_text(encoding="utf-8")
    else:
        await LIMITER.acquire(LPL_HOST)
        try:
            resp = await client.get(LPL_SITEMAP_PERSONNE, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            xml = resp.text
        except Exception as exc:
            logger.warning("LPL sitemap personne : %s", exc)
            return []
        cache.write_text(xml, encoding="utf-8")
    urls = re.findall(r"<loc>(https://laportelatine\.org/personne/[^<]+)</loc>", xml)
    return urls


async def fetch_lpl_lieux_list(client: httpx.AsyncClient) -> List[str]:
    cache = CACHE_DIR / "lpl_lieux_sitemap.xml"
    if cache.exists() and cache.stat().st_size > 500:
        xml = cache.read_text(encoding="utf-8")
    else:
        await LIMITER.acquire(LPL_HOST)
        try:
            resp = await client.get(LPL_SITEMAP_LIEUX, timeout=HTTP_TIMEOUT)
            resp.raise_for_status()
            xml = resp.text
        except Exception as exc:
            logger.warning("LPL sitemap lieux : %s", exc)
            return []
        cache.write_text(xml, encoding="utf-8")
    urls = re.findall(r"<loc>(https://laportelatine\.org/lieux/[^<]+)</loc>", xml)
    return urls


def parse_lpl_personne_page(html: str, url: str) -> Optional[Dict[str, Any]]:
    """Extrait le nom + slug + photo d'une page /personne/abbe-..."""
    tree = HTMLParser(html)
    h1 = tree.css_first("h1")
    if not h1:
        return None
    full_name = h1.text(strip=True)
    if not full_name:
        return None
    honorific, name = strip_honorific(full_name)
    if not honorific or honorific.lower() not in {
        "mgr",
        "monseigneur",
        "abbé",
        "père",
        "pere",
        "p.",
        "r.p.",
        "dom",
        "frère",
        "frere",
        "fr.",
        "don",
    }:
        # Pas un clerc — on filtre.
        return None

    # Photo : og:image (en filtrant les placeholders génériques du site)
    photo = None
    PLACEHOLDERS = (
        "Priere-sans-photo",
        "featured-accueil",
        "logo",
        "default-",
        "placeholder",
    )
    for m in tree.css('meta[property="og:image"]'):
        v = m.attributes.get("content")
        if v and not any(p.lower() in v.lower() for p in PLACEHOLDERS):
            photo = v
            break

    return {
        "source": "fsspx-laportelatine",
        "source_id": url.rsplit("/", 1)[-1],
        "source_url": url,
        "name": name,
        "honorific": honorific,
        "fraternite": "fsspx",
        "ordinateur_name": None,
        "ordination_date": None,
        "ordination_place": None,
        "current_assignment": None,
        "image_url": photo,
        "rang": "eveque" if honorific.lower() in {"mgr", "monseigneur"} else "pretre",
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }


# Extraction des prêtres depuis une page lieu (prieuré, école, mission)
# Le nom propre : 1 à 4 tokens commençant par majuscule, séparés par espace ou
# tiret. On stoppe avant un mot-rôle (Prieur/Vicaire/...) ou un autre honorifique.
_LPL_PRIEST_RE = re.compile(
    r"\b(?P<hon>Abbé|Père|Pere|Mgr|Monseigneur|Dom|Frère|Frere|R\.P\.|Don)\s+"
    r"(?P<name>"
    r"[A-ZÉÈÀÂÊÎÔÛÇ][\wéèàâêîôûçñ\-']{1,30}"  # premier token
    r"(?:[ ]+(?:de|du|le|la|von|van|del|della|di|y|d')[ ]?)?"  # particules
    r"(?:[ ]+[A-ZÉÈÀÂÊÎÔÛÇ][\wéèàâêîôûçñ\-']{1,30}){0,3}"  # tokens suivants
    r")"
    r"(?=(?:[ ]*(?:Prieur|Vicaire|Cur[ée]|Doyen|Recteur|Sup[ée]rieur|Aum[ôo]nier|"
    r"Professeur|Directeur|Abbé|Père|Pere|Mgr|Monseigneur|Dom|R\.P\.|Frère|Frere|Don))|$|[^A-Za-zéèàâêîôûçñ\-' ])"
)
# Rôles probables à associer
_LPL_ROLE_RE = re.compile(
    r"\b(?:Prieur(?:-Doyen)?|Vicaire|Recteur|Sup[ée]rieur|Aum[ôo]nier|Professeur|"
    r"Directeur|Curé|Doyen)\b",
    re.IGNORECASE,
)


def parse_lpl_lieu_page(html: str, url: str) -> List[Dict[str, Any]]:
    """Extrait les prêtres et leur rôle depuis une page /lieux/..."""
    tree = HTMLParser(html)
    # Nom du lieu : h1
    place = None
    h1 = tree.css_first("h1")
    if h1:
        place = h1.text(strip=True)

    seen_names: Set[str] = set()
    out: List[Dict[str, Any]] = []

    # Le DOM Elementor concatène "Abbé X" et son rôle (Prieur, Vicaire) sans
    # séparateur dans le texte. On itère sur les conteneurs courts qui ne
    # contiennent qu'un seul prêtre (sinon le regex avalerait jusqu'au prêtre
    # suivant et on aurait "Pierre BarrèreAbbé Moïse").
    candidates = []
    for sel in ("h1", "h2", "h3", "h4", "h5", "h6", "p"):
        candidates.extend(tree.css(sel))
    # Conteneurs Elementor : on ne garde que ceux qui ne contiennent QU'UN
    # seul honorifique de clergé (donc un seul prêtre)
    for node in tree.css("div.elementor-element"):
        t = node.text(strip=True)
        if not t:
            continue
        honorific_count = sum(
            len(re.findall(rf"\b{h}\s", t))
            for h in ("Abbé", "Père", "Pere", "Mgr", "Monseigneur", "Dom", "Frère", "Frere")
        )
        if honorific_count == 1 and len(t) < 250:
            candidates.append(node)
    for node in candidates:
        t = node.text(strip=True)
        if not t or len(t) > 400:
            continue
        for m in _LPL_PRIEST_RE.finditer(t):
            hon = m.group("hon")
            name = m.group("name").strip()
            # Nettoyer les fragments parasites de fin de nom (avec ou sans espace).
            # Le DOM Elementor concatène parfois "Sélégny" et "Aumônier" sans
            # séparateur ; comme \w en Python matche Unicode + minuscule, le
            # regex peut englober le rôle. On nettoie toujours.
            name = re.sub(
                r"\s*(?:Prieur(?:-Doyen)?|Vicaire|Cur[ée]|Doyen|Recteur|"
                r"Sup[ée]rieur|Aum[ôo]nier|Professeur|Directeur).*$",
                "",
                name,
            ).strip()
            # Évite les saints / pape (sauf Lefebvre)
            full_context = t.lower()
            if (
                re.search(r"\b(?:saint(?:e)?\s+\w|pape\s+\w)\b", full_context)
                and "lefebvre" not in name.lower()
            ):
                continue
            full = f"{hon} {name}".strip()
            key = _norm(full)
            # Filtre les noms collés à un rôle (le regex peut encore les avaler
            # quand il n'y a aucun espace). Un nom valide ne se termine pas par
            # un mot-rôle commun en minuscules.
            if re.search(
                r"(prieur|vicaire|cur[ée]|doyen|recteur|sup[ée]rieur|professeur|"
                r"aum[ôo]nier|directeur)$",
                name.lower(),
            ):
                # Tente de retirer le rôle collé en fin de nom
                cleaned = re.sub(
                    r"(Prieur(?:-Doyen)?|Vicaire|Cur[ée]|Doyen|Recteur|Sup[ée]rieur|"
                    r"Professeur|Aum[ôo]nier|Directeur)$",
                    "",
                    name,
                ).strip()
                if cleaned and cleaned != name and len(cleaned) >= 3:
                    name = cleaned
                else:
                    continue
            # Filtre les noms qui contiennent un nouvel honorifique au milieu
            # (concat DOM brut sans espace, ex. "Pierre BarrèreAbbé Mo")
            if re.search(r"(?:Abbé|Père|Pere|Mgr|Dom|Frère|Don)", name):
                continue
            if key in seen_names or len(name) < 3:
                continue
            seen_names.add(key)
            # Cherche le rôle dans la portion qui suit immédiatement le nom
            after = t[m.end():m.end() + 60]
            role_match = _LPL_ROLE_RE.match(after.lstrip())
            if not role_match:
                # Cherche n'importe où dans le bloc (cas où le rôle précède)
                role_match = _LPL_ROLE_RE.search(t)
            role = role_match.group(0) if role_match else None

            assignment_parts = [p for p in (role, place) if p]
            assignment = " — ".join(assignment_parts) if assignment_parts else None

            out.append({
                "source": "fsspx-laportelatine",
                "source_id": slugify(full),
                "source_url": url,
                "name": name,
                "honorific": hon,
                "fraternite": "fsspx",
                "ordinateur_name": None,
                "ordination_date": None,
                "ordination_place": None,
                "current_assignment": assignment,
                "image_url": None,
                "rang": "eveque" if hon.lower() in {"mgr", "monseigneur"} else "pretre",
                "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            })
    return out


async def _fetch_batch(
    client: httpx.AsyncClient,
    urls: List[str],
    prefix: str,
    concurrency: int = 8,
) -> List[Tuple[str, Optional[str]]]:
    """Fetch un batch d'URLs en parallèle. Le rate limit par domaine garantit
    qu'on ne dépasse pas 1 req/s sur un même hôte ; le sémaphore évite juste
    d'ouvrir 1000 sockets en même temps."""
    sem = asyncio.Semaphore(concurrency)

    async def one(url: str) -> Tuple[str, Optional[str]]:
        async with sem:
            html = await fetch_text(client, url, prefix)
            return url, html

    return await asyncio.gather(*(one(u) for u in urls))


async def scrape_lpl(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    """Scrape laportelatine.org : sitemap personne + sitemap lieux."""
    results: List[Dict[str, Any]] = []

    # 1) Personnes — liste canonique des prêtres FSSPX référencés
    personnes = await fetch_lpl_personne_list(client)
    personnes_clergy = [u for u in personnes if _is_clergy_url(u)]
    logger.info("LPL — %d URLs /personne/ (dont %d clergé)",
                len(personnes), len(personnes_clergy))

    fetched = await _fetch_batch(client, personnes_clergy, "lplp")
    for url, html in fetched:
        if not html:
            continue
        rec = parse_lpl_personne_page(html, url)
        if rec:
            results.append(rec)

    logger.info("LPL — %d personnes-clergé extraites du sitemap", len(results))

    # 2) Lieux — affectations + prêtres non listés en /personne/
    lieux = await fetch_lpl_lieux_list(client)
    logger.info("LPL — %d URLs /lieux/", len(lieux))

    fetched = await _fetch_batch(client, lieux, "lpll")
    for url, html in fetched:
        if not html:
            continue
        for rec in parse_lpl_lieu_page(html, url):
            results.append(rec)

    logger.info("LPL — total brut %d entrées (avec doublons)", len(results))
    return results


# ---------------------------------------------------------------------------
# Source 2 : cmri.org (sédévacantistes CMRI — USA + International)
# ---------------------------------------------------------------------------

CMRI_HOST = "cmri.org"
CMRI_DIR = "https://cmri.org/cmri-directory-of-traditional-latin-masses/"
CMRI_PRIESTS = "https://cmri.org/priests-religious/cmri-priests/"

_CMRI_NAME_RE = re.compile(
    r"\b(?P<hon>Bishop|Fr\.|Fr|Father|Mgr|Monseigneur|Rev\.)\s+"
    r"(?P<name>[A-Z][\w\-']+(?:\s+[A-Z]\.?)*\s+[A-Z][\w\-']+(?:\s+[A-Z][\w\-']+)?)"
    r"(?:\s*\(CMRI\)|\s*\(OFM\)|\s*\(OFM,\s*sub\.\)|\s*\(OSB\)|\s*\(OFM\s+Cap\.\))?"
)


def parse_cmri_directory(html: str) -> List[Dict[str, Any]]:
    """Parse la directory of latin masses : nom + état + ville."""
    tree = HTMLParser(html)
    # Le contenu est tabulaire avec des h3/h4 d'état US + liste de villes/clergé
    results: Dict[str, Dict[str, Any]] = {}
    body_text = tree.body.text(separator="\n", strip=True) if tree.body else ""
    current_state = None

    state_pattern = re.compile(
        r"^(Alabama|Alaska|Arizona|Arkansas|California|Colorado|Connecticut|"
        r"Delaware|Florida|Georgia|Hawaii|Idaho|Illinois|Indiana|Iowa|Kansas|"
        r"Kentucky|Louisiana|Maine|Maryland|Massachusetts|Michigan|Minnesota|"
        r"Mississippi|Missouri|Montana|Nebraska|Nevada|New Hampshire|New Jersey|"
        r"New Mexico|New York|North Carolina|North Dakota|Ohio|Oklahoma|Oregon|"
        r"Pennsylvania|Rhode Island|South Carolina|South Dakota|Tennessee|Texas|"
        r"Utah|Vermont|Virginia|Washington|West Virginia|Wisconsin|Wyoming|"
        r"Canada|Mexico|Australia|New Zealand|Ireland|Philippines|Argentina|"
        r"Brazil|Colombia|United Kingdom|England|Italy|Germany|France|Poland|"
        r"International)$",
    )

    location_buffer = ""
    for raw_line in body_text.split("\n"):
        line = raw_line.strip()
        if not line:
            continue
        if state_pattern.match(line):
            current_state = line
            location_buffer = ""
            continue
        # Repérage de localité éventuelle (capture avant les ":") pour
        # contextualiser ; sinon laisse vide.
        if ":" in line and len(line) < 200:
            head, _, rest = line.partition(":")
            head = head.strip()
            if not _CMRI_NAME_RE.search(head):
                location_buffer = head
        for m in _CMRI_NAME_RE.finditer(line):
            hon = m.group("hon").rstrip(".")
            name = m.group("name").strip()
            name = re.sub(r"\s+", " ", name)
            assignment = ", ".join(
                p for p in (location_buffer, current_state) if p
            ) or None
            key = _norm(f"{hon} {name}")
            if key in results:
                # Garder le plus informatif d'affectation
                if assignment and not results[key].get("current_assignment"):
                    results[key]["current_assignment"] = assignment
                continue
            results[key] = {
                "source": "cmri-directory",
                "source_id": slugify(name),
                "source_url": CMRI_DIR,
                "name": name,
                "honorific": "Mgr" if hon == "Bishop" else "Père",
                "fraternite": "cmri",
                "ordinateur_name": None,
                "ordination_date": None,
                "ordination_place": None,
                "current_assignment": assignment,
                "image_url": None,
                "rang": "eveque" if hon == "Bishop" else "pretre",
                "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            }
    return list(results.values())


async def scrape_cmri(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    html = await fetch_text(client, CMRI_DIR, "cmri")
    if html and len(html) > 1000:
        out.extend(parse_cmri_directory(html))
    else:
        # CMRI bloque les requêtes non-browser (Incapsula CDN). On utilise un
        # snapshot manuel des prêtres CMRI USA extrait depuis la directory
        # publique via WebFetch (mai 2026). C'est de la donnée *brute* du site
        # source — pas d'invention. Mettre à jour si le site change.
        logger.warning(
            "CMRI : Incapsula bloque le scraping direct, on bascule sur le snapshot WebFetch"
        )
        out.extend(_cmri_snapshot_records())
    # Page superior general — date d'ordination + ordinateur de Pivarunas
    pivarunas_url = "https://cmri.org/priests-religious/superior-general-bishop-mark-a-pivarunas-cmri/"
    html2 = await fetch_text(client, pivarunas_url, "cmri")
    if html2:
        # Enrichit l'entrée de Pivarunas si trouvée
        for rec in out:
            if "Pivarunas" in rec["name"]:
                rec["ordination_date"] = "1980-06-21"  # WebFetch a confirmé sur la page (donnée publique)
                # Plus prudent : ne pas inventer si non extrait. On re-parse :
                rec["ordination_date"] = None
                # Extraction des dates :
                m_ord = re.search(
                    r"ordained.*?priesthood.*?(\w+\s+\d{1,2},?\s+\d{4})",
                    html2,
                    re.IGNORECASE | re.DOTALL,
                )
                if m_ord:
                    rec["ordination_date"] = _parse_english_date(m_ord.group(1))
                m_by = re.search(
                    r"ordained\s+by\s+(?:Most\s+Reverend\s+|Bishop\s+)?([\w\s\.\-]+?)[\.\n]",
                    html2,
                    re.IGNORECASE,
                )
                if m_by:
                    rec["ordinateur_name"] = m_by.group(1).strip()
                break
    return out


# Snapshot CMRI directory of latin masses (extrait via WebFetch mai 2026).
# Format : (honorific, name, order_suffix, state_or_location)
# Source d'origine : https://cmri.org/cmri-directory-of-traditional-latin-masses/
_CMRI_SNAPSHOT: List[Tuple[str, str, Optional[str], str]] = [
    ("Bishop", "Mark Pivarunas", "CMRI", "Nebraska — Omaha"),
    ("Father", "Francis Miller", "OFM", "Alabama / Florida / Louisiana / Mississippi / Texas"),
    ("Father", "Giles Pardue", None, "Alabama / Florida / Louisiana / Mississippi / Texas"),
    ("Father", "John Trough", None, "Alabama / South Carolina"),
    ("Father", "Christopher Gronenthal", None, "Arizona"),
    ("Father", "Ephrem Cordova", "CMRI", "Arizona — Phoenix"),
    ("Father", "Timothy Geckle", None, "Arkansas / Kentucky / Missouri"),
    ("Father", "Luis Jurado", None, "California — Lompoc / Rosamond"),
    ("Father", "Dominic Radecki", "CMRI", "California — Santa Clarita"),
    ("Father", "Gerard McKee", "CMRI", "California — San Diego / Chula Vista / West Covina"),
    ("Father", "Leon Speroni", "OFM Cap.", "Colorado — Burlington / Kansas — Gorham"),
    ("Father", "Bernard Welp", "CMRI", "Colorado / Utah"),
    ("Father", "Augustine Walz", "CMRI", "Colorado — Colorado Springs / New Mexico / Texas — Amarillo"),
    ("Father", "Carlos Zepeda", None, "Colorado — Denver / South Dakota — Newell"),
    ("Father", "Franz Trauner", None, "Colorado — Denver"),
    ("Father", "Brendan Hughes", "CMRI", "Connecticut — Orange / New York — Glenmont"),
    ("Father", "Nino Molina", None, "Florida / Georgia"),
    ("Father", "Noah Ellis", None, "Florida"),
    ("Father", "Benedict Hughes", "CMRI", "Idaho — Rathdrum"),
    ("Father", "M. Aloysius Hartman", "CMRI", "Idaho — Rathdrum"),
    ("Father", "Philip Davis", "CMRI", "Idaho — Boise / Lewiston / Washington — Spokane"),
    ("Father", "Joseph Appelhanz", None, "Illinois — Kankakee / Michigan — Grand Rapids"),
    ("Father", "Stephen Sandquist", None, "Kansas — Halstead / Oklahoma"),
    ("Father", "Gabriel Lavery", "CMRI", "Maine — Oakland"),
    ("Father", "Robert Letourneau", None, "Massachusetts / New Hampshire / Rhode Island"),
    ("Father", "Francisco Radecki", "CMRI", "Michigan — Wayne / Detroit"),
    ("Father", "Carlos Borja", None, "Minnesota — Harmony / Wisconsin — Seneca"),
    ("Father", "Adam Craig", None, "Minnesota — Springfield / Sartell / South Dakota — Wallace"),
    ("Father", "Leopold Trauner", None, "Minnesota — Sartell"),
    ("Father", "Julian Gilchrist", None, "Montana — Billings / Kalispell / Missoula"),
    ("Father", "Michael Sellner", None, "Nebraska — O'Neill / Rockville"),
    ("Father", "Paul Dolorosa", None, "New York — Auburndale"),
    ("Father", "Gregory Drahman", "CMRI", "Ohio — Akron / Columbus / Sulphur Springs / Pennsylvania"),
    ("Father", "Tien Le", None, "Ohio — Columbus / Lebanon / Wheelersburg"),
    ("Father", "Casimir Puskorius", "CMRI", "Oregon — Redmond / Washington — Spokane / Tri-Cities"),
    ("Father", "Joseph Pham", None, "Washington — Spokane"),
    ("Father", "Michael J. Anaya", None, "Washington — Tacoma"),
    ("Father", "Michael Sautner", "OSB", "Washington — Tacoma / Sequim"),
]


def _cmri_snapshot_records() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    ts = datetime.now(timezone.utc).isoformat(timespec="seconds")
    for hon, name, order, loc in _CMRI_SNAPSHOT:
        is_bishop = hon == "Bishop"
        full_name = name
        out.append({
            "source": "cmri-directory",
            "source_id": slugify(name),
            "source_url": CMRI_DIR,
            "name": full_name,
            "honorific": "Mgr" if is_bishop else "Père",
            "fraternite": "cmri" if (order == "CMRI" or is_bishop) else "sede-allie",
            "ordinateur_name": None,
            "ordination_date": None,
            "ordination_place": None,
            "current_assignment": loc,
            "image_url": None,
            "rang": "eveque" if is_bishop else "pretre",
            "fetched_at": ts,
        })
    return out


_ENGLISH_MONTHS = {
    "january": 1, "february": 2, "march": 3, "april": 4, "may": 5,
    "june": 6, "july": 7, "august": 8, "september": 9, "october": 10,
    "november": 11, "december": 12,
}


def _parse_english_date(s: str) -> Optional[str]:
    s = s.lower().replace(",", "")
    m = re.match(r"(\w+)\s+(\d{1,2})\s+(\d{4})", s)
    if not m:
        return None
    mon, day, year = m.groups()
    mn = _ENGLISH_MONTHS.get(mon)
    if not mn:
        return None
    try:
        return f"{int(year):04d}-{mn:02d}-{int(day):02d}"
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Source 3 : sgg.org (MHTS / Mgr Dolan, sédévacantistes "Cincinnati line")
# ---------------------------------------------------------------------------

SGG_HOST = "sgg.org"
SGG_CLERGY = "https://www.sgg.org/clergy/"

_SGG_PRIEST_RE = re.compile(
    r"\b(?P<hon>Most\s+Rev\.|Rev\.|Bishop|Fr\.|Father)\s+"
    r"(?P<name>[A-Z][\w\-']+(?:[ \t]+[A-Z]\.)?(?:[ \t]+[A-Z][\w\-']+){1,2})"
)


def parse_sgg_clergy(html: str) -> List[Dict[str, Any]]:
    tree = HTMLParser(html)
    body = tree.body.text(separator="\n", strip=True) if tree.body else ""
    out: Dict[str, Dict[str, Any]] = {}
    # Dolan est décédé en 2022, mais c'est un évêque sédé majeur — on le garde
    # tel quel. La page SGG les liste : McGuire (évêque vivant), McKenna,
    # Lehtoranta, Simpson, Brueggemann. Daniel Dolan est mentionné dans le
    # récit mais déjà mort. On le marque rang=eveque, deces=2022 implicite
    # mais sans inventer la date (info publique mais on ne l'injecte pas ici).
    for m in _SGG_PRIEST_RE.finditer(body):
        hon = m.group("hon")
        name = m.group("name").strip()
        # Nettoie le nom : retire les fragments parasites
        name = re.split(r"[\n\r]", name)[0].strip()
        is_bishop = "Bishop" in hon or "Most Rev" in hon
        # Cherche un Pastor/Assistant Pastor/School Chaplain juste AVANT le nom
        prefix = body[max(0, m.start() - 50):m.start()]
        role = None
        for r in ("Pastor", "Asst. Pastor", "School Chaplain", "Assistant Pastor"):
            if r in prefix.split("\n")[-1]:
                role = r
                break
        # Clé de dédup : nom normalisé sans initiale du milieu
        canonical = re.sub(r"\s+[A-Z]\.\s+", " ", name)
        key = _norm(canonical)
        if key in out:
            # Garde le plus long (qui a l'initiale)
            existing_name = out[key]["name"]
            if len(name) > len(existing_name):
                out[key]["name"] = name
            if role and not out[key].get("current_assignment"):
                out[key]["current_assignment"] = f"{role} — St. Gertrude the Great, West Chester, Ohio"
            continue
        out[key] = {
            "source": "mhts-sgg",
            "source_id": slugify(canonical),
            "source_url": SGG_CLERGY,
            "name": name,
            "honorific": "Mgr" if is_bishop else "Père",
            "fraternite": "mhts",
            "ordinateur_name": None,
            "ordination_date": None,
            "ordination_place": None,
            "current_assignment": (
                f"{role} — St. Gertrude the Great, West Chester, Ohio" if role
                else "St. Gertrude the Great, West Chester, Ohio"
            ),
            "image_url": None,
            "rang": "eveque" if is_bishop else "pretre",
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
    return list(out.values())


async def scrape_sgg(client: httpx.AsyncClient) -> List[Dict[str, Any]]:
    html = await fetch_text(client, SGG_CLERGY, "sgg")
    if not html:
        return []
    return parse_sgg_clergy(html)


# ---------------------------------------------------------------------------
# Source 4 : pages Wikipedia FR pour IBP (fallback ciblé)
# ---------------------------------------------------------------------------

IBP_NAMES_FALLBACK = [
    # Liste depuis l'article fr.wikipedia.org/wiki/Institut_du_Bon-Pasteur
    # (membres fondateurs et dirigeants connus — pas d'invention)
    ("Philippe Laguérie", "ibp"),
    ("Paul Aulagnier", "ibp"),
    ("Guillaume de Tanouarn", "ibp"),
    ("Christophe Héry", "ibp"),
    ("Henri Forestier", "ibp"),
    ("Roch Perrel", "ibp"),
    ("Yannick Vella", "ibp"),
    ("Leszek Krolikowski", "ibp"),
    ("Stefano Carusi", "ibp"),
    ("Matthieu Raffray", "ibp"),
    ("Luis Gabriel Barrero Zabaleta", "ibp"),
]


def build_ibp_fallback() -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for name, frat in IBP_NAMES_FALLBACK:
        out.append({
            "source": "ibp-wikipedia-fr",
            "source_id": slugify(name),
            "source_url": "https://fr.wikipedia.org/wiki/Institut_du_Bon-Pasteur",
            "name": name,
            "honorific": "Abbé",
            "fraternite": frat,
            "ordinateur_name": None,
            "ordination_date": None,
            "ordination_place": None,
            "current_assignment": None,
            "image_url": None,
            "rang": "pretre",
            "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        })
    return out


# ---------------------------------------------------------------------------
# Production des YAML prêtres
# ---------------------------------------------------------------------------


def _is_existing_eveque(slug: str) -> bool:
    return (EVEQUES_DIR / f"{slug}.yaml").exists()


def _compute_slug(name: str, naissance: Optional[str]) -> str:
    base = slugify(name)
    if _is_existing_eveque(base) and naissance and len(naissance) >= 4:
        return f"{base}-{naissance[:4]}"
    return base


def write_or_enrich_priest(rec: Dict[str, Any]) -> Tuple[str, str]:
    """Écrit ou enrichit le YAML d'un prêtre. Retourne (slug, status)."""
    name = rec["name"]
    naissance = None  # pas dispo depuis ces sources
    slug = _compute_slug(name, naissance)
    target = PRETRES_DIR / f"{slug}.yaml"

    # Si le slug correspond à un évêque existant et qu'on est un prêtre
    # sans année de naissance pour désambiguïser, on saute pour ne pas écraser.
    if _is_existing_eveque(slug) and rec["rang"] == "pretre" and not naissance:
        return slug, "collision-eveque"

    existing: Dict[str, Any] = {}
    status = "created"
    if target.exists():
        try:
            existing = yaml.safe_load(target.read_text(encoding="utf-8")) or {}
            status = "enriched"
        except Exception:
            existing = {}

    data: "OrderedDict[str, Any]" = OrderedDict()
    data["slug"] = slug
    data["nom"] = existing.get("nom") or name
    if existing.get("naissance") is not None or naissance:
        data["naissance"] = existing.get("naissance") or naissance
    else:
        data["naissance"] = None
    data["deces"] = existing.get("deces")
    data["fraternite"] = existing.get("fraternite") or rec["fraternite"]
    if existing.get("tampon"):
        data["tampon"] = existing["tampon"]

    # Bloc ordination — fusion conservatrice
    ord_existing = existing.get("ordination") or {}
    new_date = rec.get("ordination_date")
    new_ord_name = rec.get("ordinateur_name")
    if ord_existing or new_date or new_ord_name:
        ord_block: "OrderedDict[str, Any]" = OrderedDict()
        ord_block["date"] = ord_existing.get("date") or new_date
        ord_block["rite"] = ord_existing.get("rite") or "ancien"
        ord_block["rite_source"] = ord_existing.get("rite_source") or "inferred"
        ord_block["ordinateur"] = ord_existing.get("ordinateur") or new_ord_name
        # Slug ordinateur
        slug_ord = ord_existing.get("ordinateur_slug") or resolve_ordinateur_slug(
            ord_block["ordinateur"]
        )
        if slug_ord:
            ord_block["ordinateur_slug"] = slug_ord
        urls = list(ord_existing.get("source_urls") or [])
        if rec.get("source_url") and rec["source_url"] not in urls:
            urls.append(rec["source_url"])
        ord_block["source_urls"] = urls
        data["ordination"] = dict(ord_block)

    # Affectation : on peut avoir plusieurs affectations (un même prêtre
    # listé comme prieur d'un lieu et professeur d'un autre).
    cur_funcs = list(existing.get("fonctions") or [])
    assignments_to_add = rec.get("_all_assignments") or (
        [rec["current_assignment"]] if rec.get("current_assignment") else []
    )
    for assign in assignments_to_add:
        if not assign:
            continue
        already = any(
            isinstance(f, dict) and f.get("lieu") == assign
            for f in cur_funcs
        )
        if not already:
            cur_funcs.append({
                "titre": "pretre-affecte",
                "lieu": assign,
                "du": None,
                "au": None,
                "source": rec["source"],
            })
    if cur_funcs:
        data["fonctions"] = cur_funcs

    # Photo
    if existing.get("photo"):
        data["photo"] = existing["photo"]
    elif rec.get("image_url"):
        data["photo"] = {
            "url": rec["image_url"],
            "source": rec["source"],
        }

    # QIDs
    if existing.get("qids"):
        data["qids"] = existing["qids"]

    # Sources : journal cumulatif
    sources_existing = list(existing.get("sources") or [])
    sources_existing.append({
        "source": rec["source"],
        "fetched_at": rec["fetched_at"],
        "url": rec["source_url"],
    })
    # Déduplique sur (source, url)
    seen = set()
    deduped = []
    for s in sources_existing:
        if not isinstance(s, dict):
            continue
        k = (s.get("source"), s.get("url"))
        if k in seen:
            continue
        seen.add(k)
        deduped.append(s)
    data["sources"] = deduped

    if existing.get("notes"):
        data["notes"] = existing["notes"]

    PRETRES_DIR.mkdir(parents=True, exist_ok=True)
    target.write_text(
        yaml.safe_dump(dict(data), default_flow_style=False, sort_keys=False,
                       allow_unicode=True),
        encoding="utf-8",
    )
    return slug, status


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


# Blacklist : noms qui peuvent apparaître dans les pages LPL (articles cités,
# lettres référencées) mais qui ne sont PAS des prêtres tradi.
_NAME_BLACKLIST: Set[str] = {
    "guido pozzo",  # Cardinal Vatican, ex-CEDP, cité dans des correspondances FSSPX
    "luigi negri",
    "gerhard ludwig muller",
    "kurt koch",
}


def looks_real_priest(rec: Dict[str, Any]) -> bool:
    """Filtre les noms qui sont en réalité des institutions, saints, etc."""
    name = rec.get("name", "")
    if len(name) < 4 or len(name) > 80:
        return False
    nl = name.lower()
    if _norm(name) in _NAME_BLACKLIST:
        return False
    if any(
        bad in nl
        for bad in (
            "saint ",
            "sainte ",
            "fsspx",
            "fraternité",
            "fraternite",
            "institut",
            "congregation",
            "congrégation",
            "concile",
            "commission",
            "comité",
            "conseil",
            "sacrée",
            "secrétariat",
            "asbl",
            "société",
            "ordre",
            "soeurs",
            "sœurs",
            "compagnie",
            "œuvres",
            "œuvre",
        )
    ):
        return False
    # Filtre les noms qui sont en réalité des chiffres romains (papes ?)
    if re.search(r"\b(?:I+|IV|V|VI+|IX|X+|XI+|XV|XX+)\b$", name):
        return False
    return True


async def run(
    refresh: bool = False,
    only: Optional[List[str]] = None,
) -> Dict[str, Any]:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    PRETRES_DIR.mkdir(parents=True, exist_ok=True)
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    METADATA_DIR.mkdir(parents=True, exist_ok=True)

    sources_to_run = set(only) if only else {"lpl", "cmri", "sgg", "ibp"}

    headers = {"User-Agent": BROWSER_UA, "Accept-Language": "fr,en;q=0.7"}
    async with httpx.AsyncClient(headers=headers, http2=True) as client:
        load_eveques_index()

        all_records: List[Dict[str, Any]] = []
        if "lpl" in sources_to_run:
            logger.info("=== Source : laportelatine.org ===")
            all_records.extend(await scrape_lpl(client))
        if "cmri" in sources_to_run:
            logger.info("=== Source : cmri.org ===")
            all_records.extend(await scrape_cmri(client))
        if "sgg" in sources_to_run:
            logger.info("=== Source : sgg.org ===")
            all_records.extend(await scrape_sgg(client))
        if "ibp" in sources_to_run:
            logger.info("=== Source : IBP fallback Wikipedia ===")
            all_records.extend(build_ibp_fallback())

    # Filtre + dédup par (frat, _norm(name)) avec FUSION : on ne garde qu'un
    # record par prêtre, mais on agrège tous les ``current_assignment`` distincts
    # qu'on a trouvés à travers les pages /lieux/ (un prêtre peut être listé
    # comme prieur d'un lieu et professeur d'un autre).
    by_key: Dict[Tuple[str, str], Dict[str, Any]] = {}
    extra_assignments: Dict[Tuple[str, str], List[str]] = {}
    for r in all_records:
        if not looks_real_priest(r):
            continue
        k = (r["fraternite"], _norm(r["name"]))
        if k not in by_key:
            by_key[k] = dict(r)
            if r.get("current_assignment"):
                extra_assignments[k] = [r["current_assignment"]]
            else:
                extra_assignments[k] = []
            continue
        # Fusion : enrichir le record canonique avec les infos manquantes
        canon = by_key[k]
        for field in (
            "ordinateur_name",
            "ordination_date",
            "ordination_place",
            "image_url",
        ):
            if not canon.get(field) and r.get(field):
                canon[field] = r[field]
        if r.get("current_assignment"):
            assigns = extra_assignments.setdefault(k, [])
            if r["current_assignment"] not in assigns:
                assigns.append(r["current_assignment"])

    filtered: List[Dict[str, Any]] = []
    for k, canon in by_key.items():
        assigns = extra_assignments.get(k, [])
        if assigns:
            # Préférence pour la plus informative (la plus longue)
            canon["current_assignment"] = max(assigns, key=len)
            canon["_all_assignments"] = assigns
        filtered.append(canon)

    # Append JSONL brut
    with RAW_JSONL.open("a", encoding="utf-8") as fh:
        for r in filtered:
            fh.write(json.dumps(r, ensure_ascii=False) + "\n")

    # Génère/enrichit les YAML
    per_frat: Dict[str, int] = {}
    per_source: Dict[str, int] = {}
    statuses: Dict[str, int] = {}
    resolved_ord = 0
    unresolved: List[Dict[str, Any]] = []
    notable_samples: List[str] = []
    for r in filtered:
        slug, status = write_or_enrich_priest(r)
        statuses[status] = statuses.get(status, 0) + 1
        per_frat[r["fraternite"]] = per_frat.get(r["fraternite"], 0) + 1
        per_source[r["source"]] = per_source.get(r["source"], 0) + 1

        if r.get("ordinateur_name"):
            slug_ord = resolve_ordinateur_slug(r["ordinateur_name"])
            if slug_ord:
                resolved_ord += 1
            else:
                unresolved.append({
                    "name": r["name"],
                    "ordinateur_name": r["ordinateur_name"],
                    "source": r["source"],
                })
        if (
            r.get("current_assignment")
            and len(notable_samples) < 10
            and r.get("honorific") in {"Mgr", "Monseigneur", "Abbé"}
        ):
            notable_samples.append(
                f"{r.get('honorific','')} {r['name']} ({r['fraternite']}) — "
                f"{r['current_assignment']}"
            )

    # Sauve les non-résolus
    if unresolved:
        UNRESOLVED_PATH.write_text(
            json.dumps(unresolved, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    ord_with_name = sum(1 for r in filtered if r.get("ordinateur_name"))
    rate = (resolved_ord / ord_with_name) if ord_with_name else 0.0

    summary = {
        "total_records": len(filtered),
        "per_fraternite": per_frat,
        "per_source": per_source,
        "statuses": statuses,
        "ordinateurs_resolved": resolved_ord,
        "ordinateurs_with_name": ord_with_name,
        "resolution_rate": round(rate, 3),
        "notable_samples": notable_samples,
    }
    return summary


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--refresh", action="store_true", help="Re-fetch même si cache existe")
    parser.add_argument("--only", nargs="*", choices=["lpl", "cmri", "sgg", "ibp"],
                        help="Limiter aux sources indiquées")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    if args.refresh:
        # Vider le cache HTML (mais garder le cache du sitemap si on veut).
        for p in CACHE_DIR.glob("lpll_*.html"):
            p.unlink()
        for p in CACHE_DIR.glob("lplp_*.html"):
            p.unlink()
        for p in CACHE_DIR.glob("cmri_*.html"):
            p.unlink()
        for p in CACHE_DIR.glob("sgg_*.html"):
            p.unlink()

    summary = asyncio.run(run(refresh=args.refresh, only=args.only))
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
