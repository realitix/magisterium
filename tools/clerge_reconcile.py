"""Phase 4 — réconciliation cross-sources du corpus clergé.

Fusionne les trois JSONL bruts (`clerge/_raw/wikidata.jsonl`,
`catholic_hierarchy.jsonl`, `gcatholic.jsonl`) en :

- une fiche YAML canonique par évêque (`clerge/eveques/{slug}.yaml`)
- des index JSONL d'arêtes et de recherche (`clerge/_metadata/*.jsonl`)
- une table de mapping cross-sources (`clerge/_metadata/source_mapping.json`)
- un fichier de stats (`clerge/_metadata/stats.json`)

L'outil est **idempotent** : ré-exécuté sur les mêmes inputs, il produit
exactement les mêmes outputs (slugs déterministes, tri stable).

NE PAS écrire de `rite` dans le bloc `sacre` — c'est la phase 5 qui annote.

Usage::

    uv run python -m tools.clerge_reconcile [--dry-run] [--limit N]
                                            [--only-cross-sourced]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any, Iterable

import yaml

ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = ROOT / "clerge" / "_raw"
META_DIR = ROOT / "clerge" / "_metadata"
EVEQUES_DIR = ROOT / "clerge" / "eveques"

WD_PATH = RAW_DIR / "wikidata.jsonl"
CH_PATH = RAW_DIR / "catholic_hierarchy.jsonl"
GC_PATH = RAW_DIR / "gcatholic.jsonl"
OVERRIDES_PATH = META_DIR / "manual_overrides.yaml"


# ---------------------------------------------------------------------------
# Normalisation et mapping de slugs
# ---------------------------------------------------------------------------

_SLUG_KEEP = re.compile(r"[^a-z0-9]+")


def ascii_fold(s: str) -> str:
    """Décompose les accents et garde l'ASCII de base."""
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return s


def normalize_name(s: str) -> str:
    """Forme normalisée pour comparaison heuristique."""
    s = ascii_fold(s).lower()
    # supprime suffixes religieux (O.P., S.J., O.S.B., etc.) et titles
    s = re.sub(r"\b(o\.\s*[a-z]\.\s*[a-z]?\.?|s\.\s*[a-z]\.?|c\.\s*[a-z]\.?\s*[a-z]?\.?)\b", "", s)
    s = re.sub(r"\b(bishop|archbishop|cardinal|patriarch|priest|fr|mgr|monsignor)\b\.?", "", s)
    s = re.sub(r"[^a-z0-9 ]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def slugify(s: str) -> str:
    """Slug ASCII en minuscules avec tirets."""
    s = ascii_fold(s).lower()
    s = _SLUG_KEEP.sub("-", s)
    return s.strip("-")


def pick_name(wd_names: dict[str, str] | None, fallback: str | None = None) -> str | None:
    """Choisit le meilleur nom dans le mapping Wikidata des labels par langue."""
    if wd_names:
        for lang in ("fr", "en", "la", "it", "es", "de"):
            if wd_names.get(lang):
                return wd_names[lang]
        # tout label dispo
        for v in wd_names.values():
            if v:
                return v
    return fallback


# ---------------------------------------------------------------------------
# Dates
# ---------------------------------------------------------------------------

_DATE_RE = re.compile(r"^(-?\d{1,4})(?:-(\d{2}))?(?:-(\d{2}))?")


def parse_date_year(s: str | None) -> int | None:
    if not s:
        return None
    m = _DATE_RE.match(s.strip())
    if not m:
        return None
    try:
        return int(m.group(1))
    except ValueError:
        return None


def normalize_wikidata_date(s: str | None) -> str | None:
    """Wikidata code souvent ``YYYY-01-01`` pour les années seules.

    Si le pattern ``MM-DD == 01-01``, on réduit à l'année (en string) car
    PyYAML interprète ``0700-01-01`` comme un objet date et perd l'info.
    """
    if not s:
        return None
    m = _DATE_RE.match(s.strip())
    if not m:
        return s.strip()
    year, month, day = m.group(1), m.group(2), m.group(3)
    if month == "01" and day == "01":
        return year.lstrip("0") or "0"
    if month and day:
        return f"{year.zfill(4)}-{month}-{day}"
    return year.lstrip("0") or "0"


# ---------------------------------------------------------------------------
# Mapping des ordres religieux Wikidata → fraternité
# ---------------------------------------------------------------------------

# QIDs Wikidata fréquents d'ordres / instituts religieux.
RELIGIOUS_ORDER_QIDS: dict[str, str] = {
    "Q131479": "jesuites",       # Compagnie de Jésus
    "Q131389": "dominicains",    # Ordo Praedicatorum
    "Q131479" : "jesuites",
    "Q131012": "franciscains",   # OFM
    "Q131013": "franciscains",   # OFM Conv
    "Q131013" : "franciscains",
    "Q131814": "capucins",       # OFM Cap
    "Q131380": "benedictins",    # OSB
    "Q131281": "cisterciens",
    "Q131568": "redemptoristes",
    "Q131559": "augustins",
    "Q131676": "spiritains",     # CSSp
    "Q1320565": "lazaristes",
    "Q1641798": "salesiens",
    "Q1129865": "maristes",
    "Q1641864": "oratoriens",
    "Q1373936": "fsspx",         # FSSPX
    "Q1264165": "fssp",          # FSSP
    "Q1373936" : "fsspx",
    "Q2007091": "icrsp",         # Institut du Christ-Roi
    "Q3151172": "ibp",           # Institut du Bon Pasteur
}

# Mapping des codes/sigles GCatholic (religious_institute) → fraternité.
RELIGIOUS_INSTITUTE_SIGLES: dict[str, str] = {
    "S.J.": "jesuites",
    "O.P.": "dominicains",
    "O.F.M.": "franciscains",
    "O.F.M.Conv.": "franciscains",
    "O.F.M.Cap.": "capucins",
    "OFMCap": "capucins",
    "O.S.B.": "benedictins",
    "O.Cist.": "cisterciens",
    "O.C.S.O.": "trappistes",
    "C.SS.R.": "redemptoristes",
    "O.S.A.": "augustins",
    "C.S.Sp.": "spiritains",
    "C.M.": "lazaristes",
    "S.D.B.": "salesiens",
    "S.M.": "maristes",
    "C.O.": "oratoriens",
    "F.S.S.P.X.": "fsspx",
    "FSSPX": "fsspx",
    "F.S.S.P.": "fssp",
    "FSSP": "fssp",
    "I.C.R.": "icrsp",
}


def fraternite_from_wd(qids: Iterable[str]) -> str | None:
    for q in qids:
        if q in RELIGIOUS_ORDER_QIDS:
            return RELIGIOUS_ORDER_QIDS[q]
    return None


def fraternite_from_gc(sigle: str | None) -> str | None:
    if not sigle:
        return None
    return RELIGIOUS_INSTITUTE_SIGLES.get(sigle.strip())


# ---------------------------------------------------------------------------
# Détection « est-ce un évêque catholique ? »
# ---------------------------------------------------------------------------

_EPISC_RE = re.compile(
    r"\b(bishop|archbishop|cardinal|patriarch|pope|prelate|"
    r"eveque|évêque|archeveque|archevêque|patriarche|pape|prélat|prelat)\b",
    re.I,
)

_RANK_RULES: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"\bpope|pape\b", re.I), "pape"),
    (re.compile(r"\bpatriarch", re.I), "patriarche"),
    (re.compile(r"\bcardinal", re.I), "cardinal"),
    (re.compile(r"\barchbishop|archev", re.I), "archeveque"),
    (re.compile(r"\bbishop|évêque|eveque", re.I), "eveque"),
]


def detect_rang(positions: list[dict[str, Any]] | None, fallback_titles: Iterable[str]) -> str:
    """Détermine le rang (pape > patriarche > cardinal > archeveque > eveque)."""
    labels: list[str] = []
    for p in positions or []:
        for k in ("label_fr", "label_en", "title"):
            if p.get(k):
                labels.append(str(p[k]))
    labels.extend([t for t in fallback_titles if t])
    blob = " ".join(labels)
    for rx, rang in _RANK_RULES:
        if rx.search(blob):
            return rang
    return "eveque"


def looks_catholic_bishop(rec: dict[str, Any]) -> bool:
    """Vérifie qu'au moins une position ressemble à un évêché catholique."""
    positions = rec.get("positions") or []
    for p in positions:
        for k in ("label_fr", "label_en", "title"):
            v = p.get(k)
            if v and _EPISC_RE.search(str(v)):
                return True
    # ou bien sa description Wikidata
    desc = rec.get("description") or {}
    for v in desc.values():
        if v and _EPISC_RE.search(str(v)):
            return True
    return False


# ---------------------------------------------------------------------------
# Records intermédiaires
# ---------------------------------------------------------------------------

@dataclass
class Bishop:
    """Évêque réconcilié — agrège les sources sous un slug stable."""

    slug: str = ""
    wikidata_qid: str | None = None
    ch_code: str | None = None
    gc_id: str | None = None
    wd: dict[str, Any] | None = None
    ch: dict[str, Any] | None = None
    gc: dict[str, Any] | None = None
    sources: list[str] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Loaders
# ---------------------------------------------------------------------------

def _iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def load_overrides(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {"eveques": {}, "consecrations": [], "ordinations": []}
    with path.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return {
        "eveques": data.get("eveques") or {},
        "consecrations": data.get("consecrations") or [],
        "ordinations": data.get("ordinations") or [],
    }


# ---------------------------------------------------------------------------
# Heuristique de jointure
# ---------------------------------------------------------------------------

def name_similarity(a: str, b: str) -> float:
    """Score 0..1 entre deux noms (après normalisation)."""
    na, nb = normalize_name(a), normalize_name(b)
    if not na or not nb:
        return 0.0
    return SequenceMatcher(None, na, nb).ratio()


def years_close(a: str | None, b: str | None, slack: int = 1) -> bool:
    ya, yb = parse_date_year(a), parse_date_year(b)
    if ya is None or yb is None:
        return False
    return abs(ya - yb) <= slack


# ---------------------------------------------------------------------------
# Pipeline principal
# ---------------------------------------------------------------------------

def reconcile(
    *,
    limit: int | None = None,
    only_cross_sourced: bool = False,
    dry_run: bool = False,
) -> dict[str, Any]:
    """Pipeline principal de réconciliation."""

    overrides = load_overrides(OVERRIDES_PATH)

    # 1. Index sources par clé primaire
    wd_by_qid: dict[str, dict[str, Any]] = {}
    for r in _iter_jsonl(WD_PATH):
        qid = r.get("source_id")
        if not qid:
            continue
        wd_by_qid[qid] = r

    ch_by_code: dict[str, dict[str, Any]] = {}
    for r in _iter_jsonl(CH_PATH):
        code = r.get("source_id")
        if not code:
            continue
        ch_by_code[code] = r

    gc_by_id: dict[str, dict[str, Any]] = {}
    for r in _iter_jsonl(GC_PATH):
        gid = r.get("source_id")
        if not gid:
            continue
        gc_by_id[gid] = r

    # 2. Bridging
    # 2a. WD ↔ CH via wikidata_id explicite dans CH
    qid_to_ch: dict[str, str] = {}
    ch_to_qid: dict[str, str] = {}
    mappings: list[dict[str, Any]] = []
    for code, ch in ch_by_code.items():
        qid = ch.get("wikidata_id")
        if qid and qid in wd_by_qid:
            qid_to_ch[qid] = code
            ch_to_qid[code] = qid
            mappings.append({
                "wikidata": qid,
                "catholic_hierarchy": code,
                "method": "wikidata_id-explicit",
                "score": 1.0,
            })

    # 2b. GC ↔ WD via (nom normalisé + date proche) ; on indexe WD par nom normalisé
    wd_by_normname: dict[str, list[str]] = defaultdict(list)
    for qid, wd in wd_by_qid.items():
        nm = pick_name(wd.get("names"))
        if not nm:
            continue
        key = normalize_name(nm)
        if key:
            wd_by_normname[key].append(qid)

    gc_to_qid: dict[str, str] = {}
    for gid, gc in gc_by_id.items():
        nm = gc.get("name_full") or gc.get("name")
        if not nm:
            continue
        key = normalize_name(nm)
        candidates = wd_by_normname.get(key, [])
        # match strict d'abord (nom normalisé identique)
        best: tuple[float, str] | None = None
        for qid in candidates:
            wd = wd_by_qid[qid]
            cons_close = years_close(gc.get("consecration_date"), wd.get("consecration_date"))
            birth_close = years_close(gc.get("birth_date"), wd.get("birth_date"))
            if cons_close or birth_close:
                best = (1.0, qid)
                break
        # sinon recherche fuzzy ciblée
        if best is None and key:
            # ne scanner qu'une fenêtre par signature de nom de famille
            surname = key.split()[-1] if " " in key else key
            for qid, wd in wd_by_qid.items():
                wnm = pick_name(wd.get("names"))
                if not wnm:
                    continue
                wkey = normalize_name(wnm)
                if not wkey:
                    continue
                if surname not in wkey:
                    continue
                score = name_similarity(nm, wnm)
                if score < 0.90:
                    continue
                if not (
                    years_close(gc.get("consecration_date"), wd.get("consecration_date"))
                    or years_close(gc.get("birth_date"), wd.get("birth_date"))
                ):
                    continue
                if best is None or score > best[0]:
                    best = (score, qid)
        if best is not None:
            gc_to_qid[gid] = best[1]
            mappings.append({
                "wikidata": best[1],
                "gcatholic": gid,
                "method": "name+date-heuristic",
                "score": round(best[0], 3),
            })

    # 2c. CH ↔ GC déduit via WD si possible, sinon heuristique
    ch_to_gc: dict[str, str] = {}
    gc_to_ch: dict[str, str] = {}
    qid_to_gc: dict[str, str] = {v: k for k, v in gc_to_qid.items()}
    for code, qid in ch_to_qid.items():
        gid = qid_to_gc.get(qid)
        if gid:
            ch_to_gc[code] = gid
            gc_to_ch[gid] = code
            mappings.append({
                "catholic_hierarchy": code,
                "gcatholic": gid,
                "wikidata": qid,
                "method": "via-wikidata",
                "score": 1.0,
            })
    # heuristique pour CH sans QID
    for code, ch in ch_by_code.items():
        if code in ch_to_gc:
            continue
        nm = ch.get("name")
        if not nm:
            continue
        key = normalize_name(nm)
        best: tuple[float, str] | None = None
        for gid, gc in gc_by_id.items():
            if gid in gc_to_ch:
                continue
            gnm = gc.get("name_full") or gc.get("name")
            if not gnm:
                continue
            gkey = normalize_name(gnm)
            if not gkey:
                continue
            # short-circuit : surname doit être commun
            if gkey.split()[-1] != key.split()[-1]:
                continue
            score = name_similarity(nm, gnm)
            if score < 0.90:
                continue
            if not (
                years_close(ch.get("consecration_date"), gc.get("consecration_date"))
                or years_close(ch.get("birth_date"), gc.get("birth_date"))
            ):
                continue
            if best is None or score > best[0]:
                best = (score, gid)
        if best is not None:
            ch_to_gc[code] = best[1]
            gc_to_ch[best[1]] = code
            mappings.append({
                "catholic_hierarchy": code,
                "gcatholic": best[1],
                "method": "name+date-heuristic",
                "score": round(best[0], 3),
            })

    # 3. Itération sur l'union — chaque évêque = un cluster
    bishops: list[Bishop] = []
    seen_qids: set[str] = set()
    seen_codes: set[str] = set()
    seen_gids: set[str] = set()

    # 3a. Tri déterministe : QID Wikidata d'abord, puis CH-only, puis GC-only
    for qid in sorted(wd_by_qid.keys()):
        if qid in seen_qids:
            continue
        wd = wd_by_qid[qid]
        b = Bishop(wikidata_qid=qid, wd=wd)
        b.sources.append("wikidata")
        seen_qids.add(qid)
        code = qid_to_ch.get(qid)
        if code:
            b.ch_code = code
            b.ch = ch_by_code[code]
            b.sources.append("catholic-hierarchy.org")
            seen_codes.add(code)
        gid = qid_to_gc.get(qid)
        if gid:
            b.gc_id = gid
            b.gc = gc_by_id[gid]
            b.sources.append("gcatholic.org")
            seen_gids.add(gid)
        bishops.append(b)

    for code in sorted(ch_by_code.keys()):
        if code in seen_codes:
            continue
        ch = ch_by_code[code]
        b = Bishop(ch_code=code, ch=ch)
        b.sources.append("catholic-hierarchy.org")
        seen_codes.add(code)
        gid = ch_to_gc.get(code)
        if gid and gid not in seen_gids:
            b.gc_id = gid
            b.gc = gc_by_id[gid]
            b.sources.append("gcatholic.org")
            seen_gids.add(gid)
        bishops.append(b)

    for gid in sorted(gc_by_id.keys()):
        if gid in seen_gids:
            continue
        b = Bishop(gc_id=gid, gc=gc_by_id[gid])
        b.sources.append("gcatholic.org")
        seen_gids.add(gid)
        bishops.append(b)

    # 4. Slug stable et déterministe
    slug_counts: dict[str, int] = defaultdict(int)
    pending: list[tuple[Bishop, str, int | None]] = []  # (bishop, base_slug, birth_year)
    for b in bishops:
        name, birth = _best_name_and_birth(b)
        if not name:
            continue  # skip sans nom (anomalie 4)
        base = slugify(name)
        if not base:
            continue
        birth_year = parse_date_year(birth)
        pending.append((b, base, birth_year))
        slug_counts[base] += 1

    # Filtre catholique pour Wikidata sans religion explicite
    filtered: list[tuple[Bishop, str, int | None]] = []
    skipped_non_catholic = 0
    for b, base, byear in pending:
        if b.wd and not b.ch and not b.gc:
            rids = b.wd.get("religion_qids") or []
            if not rids or (rids == ["Q1841"] or (len(rids) == 1 and "Q9592" not in rids)):
                if not looks_catholic_bishop(b.wd):
                    skipped_non_catholic += 1
                    continue
        filtered.append((b, base, byear))

    # Désambiguïsation : ajoute l'année de naissance si collision
    # On compte les bases sur le set filtré pour ne pas désambiguïser inutilement
    filt_counts: dict[str, int] = defaultdict(int)
    for _b, base, _by in filtered:
        filt_counts[base] += 1

    assigned_slugs: set[str] = set()
    final: list[Bishop] = []
    for b, base, byear in filtered:
        slug = base
        if filt_counts[base] > 1:
            if byear is not None:
                slug = f"{base}-{byear}"
        # collisions résiduelles : ajoute un suffixe court par md5(QID/code)
        if slug in assigned_slugs:
            key = b.wikidata_qid or b.ch_code or b.gc_id or base
            suffix = hashlib.md5(key.encode("utf-8")).hexdigest()[:6]
            slug = f"{slug}-{suffix}"
        assigned_slugs.add(slug)
        b.slug = slug
        final.append(b)

    final.sort(key=lambda b: b.slug)

    if only_cross_sourced:
        final = [b for b in final if len(b.sources) >= 2]

    if limit is not None:
        final = final[:limit]

    # 5. Génération des fiches YAML + index
    yamls: list[tuple[str, dict[str, Any]]] = []
    consecrations: list[dict[str, Any]] = []
    clerics_index: list[dict[str, Any]] = []

    # Précompute QID/CH-code → slug pour résoudre les consécrateurs
    qid_to_slug: dict[str, str] = {}
    code_to_slug: dict[str, str] = {}
    for b in final:
        if b.wikidata_qid:
            qid_to_slug[b.wikidata_qid] = b.slug
        if b.ch_code:
            code_to_slug[b.ch_code] = b.slug

    anomalies = {
        "weird_dates_normalized": 0,
        "many_consec_no_principal": 0,
        "consec_date_no_principal": 0,
        "skipped_no_name": 0,
        "skipped_non_catholic": skipped_non_catholic,
    }

    for b in final:
        doc, sacre_edge, index_row, found_anom = _build_bishop_yaml(
            b,
            qid_to_slug=qid_to_slug,
            code_to_slug=code_to_slug,
            overrides=overrides["eveques"].get(b.slug, {}),
        )
        for k, v in found_anom.items():
            anomalies[k] = anomalies.get(k, 0) + v
        yamls.append((b.slug, doc))
        if sacre_edge is not None:
            consecrations.append(sacre_edge)
        clerics_index.append(index_row)

    # 5a-bis. Création from-scratch des évêques exclusivement définis dans
    # `manual_overrides.eveques` (slug absent des sources scrapées).
    existing_slugs: set[str] = {slug for slug, _doc in yamls}
    manual_only_slugs: list[str] = sorted(
        s for s in overrides["eveques"].keys() if s not in existing_slugs
    )
    manual_created = 0
    for slug in manual_only_slugs:
        ov = overrides["eveques"][slug] or {}
        doc, sacre_edge, index_row = _build_manual_bishop(slug, ov)
        yamls.append((slug, doc))
        if sacre_edge is not None:
            consecrations.append(sacre_edge)
        clerics_index.append(index_row)
        manual_created += 1
    anomalies["manual_only_eveques"] = manual_created

    # 5a-ter. Pour chaque arête `manual_overrides.consecrations` dont le
    # consacre n'a pas d'arête dans `consecrations` (i.e., produit absent),
    # ajoute une arête synthétique. Cela couvre aussi bien les évêques créés
    # ci-dessus que les évêques préexistants dont le sacre manquait.
    edges_by_consacre: dict[str, dict[str, Any]] = {e["consacre"]: e for e in consecrations}
    manual_edges_added = 0
    for ov_edge in overrides["consecrations"] or []:
        slug = ov_edge.get("consacre")
        if not slug:
            continue
        if slug in edges_by_consacre:
            # L'arête existe déjà — le rite sera ré-appliqué par
            # tools/clerge_annotate_rite.py en utilisant le manual override.
            continue
        new_edge: dict[str, Any] = {
            "consacre": slug,
            "consecrateur_principal": ov_edge.get("consecrateur_principal"),
            "co_consecrateurs": ov_edge.get("co_consecrateurs") or [],
            "date": ov_edge.get("date"),
            "lieu": ov_edge.get("lieu"),
            "sources": ["manual_overrides"],
        }
        consecrations.append(new_edge)
        edges_by_consacre[slug] = new_edge
        manual_edges_added += 1
    anomalies["manual_edges_added"] = manual_edges_added

    # 5b. Écriture
    if not dry_run:
        EVEQUES_DIR.mkdir(parents=True, exist_ok=True)
        META_DIR.mkdir(parents=True, exist_ok=True)
        for slug, doc in yamls:
            (EVEQUES_DIR / f"{slug}.yaml").write_text(
                yaml.safe_dump(doc, allow_unicode=True, sort_keys=False, width=120),
                encoding="utf-8",
            )
        # consecrations.jsonl : tri sur slug du consacré
        consecrations.sort(key=lambda r: r["consacre"])
        (META_DIR / "consecrations.jsonl").write_text(
            "\n".join(json.dumps(r, ensure_ascii=False) for r in consecrations) + "\n",
            encoding="utf-8",
        )
        clerics_index.sort(key=lambda r: r["slug"])
        (META_DIR / "clerics.jsonl").write_text(
            "\n".join(json.dumps(r, ensure_ascii=False) for r in clerics_index) + "\n",
            encoding="utf-8",
        )
        (META_DIR / "source_mapping.json").write_text(
            json.dumps(
                sorted(mappings, key=lambda m: (m.get("wikidata") or "", m.get("catholic_hierarchy") or "", m.get("gcatholic") or "")),
                ensure_ascii=False,
                indent=2,
            ),
            encoding="utf-8",
        )

    # 6. Stats globales
    par_fraternite: dict[str, int] = defaultdict(int)
    par_siecle: dict[str, int] = defaultdict(int)
    with_photo = 0
    with_consprinc = 0
    with_consdate = 0
    cross_sourced = 0
    for _slug, doc in yamls:
        fr = doc.get("fraternite") or "non-renseigne"
        par_fraternite[fr] += 1
        b = doc.get("naissance")
        y = None
        if isinstance(b, int):
            y = b
        elif isinstance(b, str):
            y = parse_date_year(b)
        if y is not None:
            siecle = (y - 1) // 100 + 1 if y > 0 else (y // 100)
            par_siecle[str(siecle)] += 1
        if doc.get("photo"):
            with_photo += 1
        sacre = doc.get("sacre") or {}
        if sacre.get("consecrateur_principal"):
            with_consprinc += 1
        if sacre.get("date"):
            with_consdate += 1
        if len([s for s in (doc.get("sources") or []) if s.get("source")]) >= 2:
            cross_sourced += 1

    stats = {
        "total_eveques": len(yamls),
        "par_fraternite": dict(sorted(par_fraternite.items(), key=lambda kv: -kv[1])),
        "par_siecle_naissance": dict(sorted(par_siecle.items(), key=lambda kv: int(kv[0]))),
        "avec_photo": with_photo,
        "avec_consecrateur_connu": with_consprinc,
        "avec_date_sacre": with_consdate,
        "cross_source_matches": cross_sourced,
        "anomalies": anomalies,
        "raw_counts": {
            "wikidata": len(wd_by_qid),
            "catholic_hierarchy": len(ch_by_code),
            "gcatholic": len(gc_by_id),
        },
        "bridge_counts": {
            "wd_ch_explicit": len(qid_to_ch),
            "wd_gc_heuristic": len(gc_to_qid),
            "ch_gc": len(ch_to_gc),
        },
    }
    if not dry_run:
        (META_DIR / "stats.json").write_text(
            json.dumps(stats, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )

    return stats


# ---------------------------------------------------------------------------
# Construction d'un YAML d'évêque
# ---------------------------------------------------------------------------

def _best_name_and_birth(b: Bishop) -> tuple[str | None, str | None]:
    """Source de vérité pour le nom et la date de naissance.

    Ordre : CH > WD > GC pour le nom (préfère le libellé canonique
    catholic-hierarchy, plus stable), mais WD pour la version française si dispo.
    """
    name_fr = None
    name_en = None
    if b.wd:
        nm = b.wd.get("names") or {}
        name_fr = nm.get("fr")
        name_en = nm.get("en")
    if name_fr:
        name = name_fr
    elif b.ch and b.ch.get("name"):
        name = b.ch["name"]
    elif name_en:
        name = name_en
    elif b.gc and (b.gc.get("name") or b.gc.get("name_full")):
        name = b.gc.get("name") or b.gc.get("name_full")
    else:
        nm = b.wd.get("names") if b.wd else None
        name = pick_name(nm) if nm else None
    # nettoie le suffixe d'ordre éventuel ", O.P." → conserve
    if name:
        name = re.sub(r"\s+", " ", str(name)).strip()
    birth = None
    if b.ch and b.ch.get("birth_date"):
        birth = b.ch["birth_date"]
    elif b.wd and b.wd.get("birth_date"):
        birth = b.wd["birth_date"]
    elif b.gc and b.gc.get("birth_date"):
        birth = b.gc["birth_date"]
    return name, birth


def _pick(*values: Any) -> Any:
    for v in values:
        if v not in (None, "", [], {}):
            return v
    return None


def _build_manual_bishop(
    slug: str,
    override: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any]]:
    """Construit un YAML d'évêque entièrement à partir du `manual_overrides`.

    Utilisé pour les évêques tradi / sédévacantistes absents de Wikidata,
    catholic-hierarchy et gcatholic. La structure produite reste identique à
    celle de `_build_bishop_yaml` pour ne pas casser les consommateurs en aval
    (site Astro, clerge_annotate_rite).
    """
    sacre_in = override.get("sacre") or {}
    sacre: dict[str, Any] = {
        "date": sacre_in.get("date"),
        "lieu": sacre_in.get("lieu"),
        "consecrateur_principal": sacre_in.get("consecrateur_principal"),
        "co_consecrateurs": sacre_in.get("co_consecrateurs") or None,
        "source_urls": sacre_in.get("source_urls") or None,
    }
    sacre = {k: v for k, v in sacre.items() if v not in (None, [], "")}

    doc: dict[str, Any] = {
        "slug": slug,
        "nom": override.get("nom"),
        "nom_complet": override.get("nom_complet"),
        "naissance": override.get("naissance"),
        "deces": override.get("deces"),
        "naissance_lieu": override.get("naissance_lieu"),
        "deces_lieu": override.get("deces_lieu"),
        "rang": override.get("rang") or "eveque",
        "nationalite": override.get("nationalite"),
        "sacre": sacre,
        "obediences": override.get("obediences") or [{"statut": "rome", "du": None, "au": None}],
        "fonctions": override.get("fonctions"),
        "fraternite": override.get("fraternite"),
        "photo": override.get("photo"),
        "qids": override.get("qids") or {},
        "sources": override.get("sources") or [],
    }
    if "notes" in override:
        doc["notes"] = override["notes"]

    edge: dict[str, Any] | None = None
    if sacre and (sacre.get("date") or sacre.get("consecrateur_principal")):
        edge = {
            "consacre": slug,
            "consecrateur_principal": sacre.get("consecrateur_principal"),
            "co_consecrateurs": sacre.get("co_consecrateurs") or [],
            "date": sacre.get("date"),
            "lieu": sacre.get("lieu"),
            "sources": ["manual_overrides"],
        }

    naiss_y = parse_date_year(override.get("naissance")) if isinstance(override.get("naissance"), str) else override.get("naissance") if isinstance(override.get("naissance"), int) else None
    deces_y = parse_date_year(override.get("deces")) if isinstance(override.get("deces"), str) else override.get("deces") if isinstance(override.get("deces"), int) else None
    index_row = {
        "slug": slug,
        "nom": override.get("nom"),
        "naissance_annee": naiss_y,
        "deces_annee": deces_y,
        "fraternite": override.get("fraternite"),
        "rang": doc["rang"],
        "pays": override.get("nationalite"),
        "photo_disponible": override.get("photo") is not None,
        "wikidata_qid": None,
    }
    return doc, edge, index_row


def _build_bishop_yaml(
    b: Bishop,
    *,
    qid_to_slug: dict[str, str],
    code_to_slug: dict[str, str],
    overrides: dict[str, Any],
) -> tuple[dict[str, Any], dict[str, Any] | None, dict[str, Any], dict[str, int]]:
    """Construit le YAML, l'arête de consécration, et la ligne d'index."""
    anom: dict[str, int] = defaultdict(int)

    name, birth_raw = _best_name_and_birth(b)
    nom_complet = None
    if b.ch and b.ch.get("name") and b.ch["name"] != name:
        nom_complet = b.ch["name"]
    elif b.wd:
        names = b.wd.get("names") or {}
        # prend une version plus longue parmi les labels Wikidata
        candidates = [v for v in names.values() if v and v != name]
        if candidates:
            nom_complet = max(candidates, key=len)

    # Dates (avec normalisation de l'anomalie YYYY-01-01)
    naissance_raw = _pick(
        b.ch.get("birth_date") if b.ch else None,
        b.wd.get("birth_date") if b.wd else None,
        b.gc.get("birth_date") if b.gc else None,
    )
    deces_raw = _pick(
        b.ch.get("death_date") if b.ch else None,
        b.wd.get("death_date") if b.wd else None,
        b.gc.get("death_date") if b.gc else None,
    )
    naissance = normalize_wikidata_date(naissance_raw)
    deces = normalize_wikidata_date(deces_raw)
    # Détecte l'anomalie « deces avant naissance »
    yb = parse_date_year(naissance_raw)
    yd = parse_date_year(deces_raw)
    if yb is not None and yd is not None and yd < yb:
        anom["weird_dates_normalized"] += 1
        # garde quand même les valeurs réduites à l'année
        naissance = str(yb)
        deces = str(yd)

    naissance_lieu = _pick(
        b.ch.get("birth_place") if b.ch else None,
        b.gc.get("birth_place") if b.gc else None,
    )
    deces_lieu = _pick(
        b.ch.get("death_place") if b.ch else None,
    )

    # Rang
    pos_all: list[dict[str, Any]] = []
    if b.wd and b.wd.get("positions"):
        pos_all.extend(b.wd["positions"])
    if b.ch and b.ch.get("positions"):
        pos_all.extend(b.ch["positions"])
    if b.gc and b.gc.get("positions"):
        pos_all.extend(b.gc["positions"])
    rang = detect_rang(pos_all, [b.ch.get("title_prefix") if b.ch else None, b.gc.get("title_prefix") if b.gc else None])

    # Sacre — CH prioritaire (distingue principal/co), sinon WD, sinon GC
    sacre = _build_sacre(
        b,
        qid_to_slug=qid_to_slug,
        code_to_slug=code_to_slug,
        anom=anom,
    )

    # Fonctions
    fonctions = _build_fonctions(b)

    # Fraternité
    fraternite = None
    if b.wd:
        fraternite = fraternite_from_wd(b.wd.get("religious_order_qids") or [])
    if not fraternite and b.gc:
        fraternite = fraternite_from_gc(b.gc.get("religious_institute"))
    if not fraternite and b.ch:
        # suffixes type "O.P." dans le nom
        for suf in (b.ch.get("suffixes") or []):
            fraternite = fraternite_from_gc(suf)
            if fraternite:
                break

    # Photo (Wikidata seule actuellement)
    photo: dict[str, Any] | None = None
    if b.wd and b.wd.get("image_url"):
        photo = {
            "source": "wikimedia-commons",
            "source_url": b.wd["image_url"],
            "licence": "unknown",
            "fichier": None,
        }

    # Nationalité — premier QID Wikidata sans résolution pour l'instant ; GC.country sinon
    nationalite = None
    if b.gc and b.gc.get("country"):
        nationalite = b.gc["country"]
    elif b.wd and b.wd.get("nationality_qids"):
        nationalite = b.wd["nationality_qids"][0]

    # QIDs / IDs cross-source
    qids = {
        "wikidata": b.wikidata_qid,
        "catholic_hierarchy": b.ch_code,
        "gcatholic": b.gc_id,
    }
    # filtre les None
    qids = {k: v for k, v in qids.items() if v}

    # Sources journal
    sources_log: list[dict[str, Any]] = []
    if b.wd:
        sources_log.append({
            "source": "wikidata",
            "fetched_at": b.wd.get("fetched_at"),
            "completeness": _completeness(b.wd, ["names", "birth_date", "death_date", "consecration_date", "consecrator_principal_qid", "image_url", "positions", "religious_order_qids", "nationality_qids"]),
        })
    if b.ch:
        sources_log.append({
            "source": "catholic-hierarchy.org",
            "fetched_at": b.ch.get("fetched_at"),
            "completeness": _completeness(b.ch, ["name", "birth_date", "death_date", "consecration_date", "consecrator_principal_ch_code", "co_consecrator_ch_codes", "positions", "episcopal_lineage_codes"]),
        })
    if b.gc:
        sources_log.append({
            "source": "gcatholic.org",
            "fetched_at": b.gc.get("fetched_at"),
            "completeness": _completeness(b.gc, ["name_full", "birth_date", "death_date", "consecration_date", "motto", "country", "religious_institute", "positions"]),
        })

    # Obédiences : par défaut "rome" sur la période de vie. On évite d'inventer
    # une période précise — on laisse les dates floues.
    obediences = [{"statut": "rome", "du": None, "au": None}]

    # YAML ordonné — l'ordre des clés est déterministe pour des diffs propres
    doc: dict[str, Any] = {
        "slug": b.slug,
        "nom": name,
        "nom_complet": nom_complet,
        "naissance": naissance,
        "deces": deces,
        "naissance_lieu": naissance_lieu,
        "deces_lieu": deces_lieu,
        "rang": rang,
        "nationalite": nationalite,
        "sacre": sacre,
        "obediences": obediences,
        "fonctions": fonctions,
        "fraternite": fraternite,
        "photo": photo,
        "qids": qids,
        "sources": sources_log,
    }

    # Applique les overrides manuels (deep-merge superficiel sur clés top-level)
    if overrides:
        for k, v in overrides.items():
            if isinstance(v, dict) and isinstance(doc.get(k), dict):
                doc[k] = {**doc[k], **v}
            else:
                doc[k] = v

    # Le bloc `sacre` final (post-override) sert de source pour l'arête JSONL :
    # ainsi un override de `consecrateur_principal` (slug réel au lieu d'un
    # placeholder `non-indexe-…`) est bien propagé dans consecrations.jsonl.
    final_sacre = doc.get("sacre") or {}

    # Arête de consécration (1 ligne JSONL si on a un sacre documenté)
    edge: dict[str, Any] | None = None
    if final_sacre and (
        final_sacre.get("date")
        or final_sacre.get("consecrateur_principal")
        or final_sacre.get("co_consecrateurs")
    ):
        sources_edge = [s for s in [
            "catholic-hierarchy.org" if b.ch else None,
            "wikidata" if b.wd else None,
            "gcatholic.org" if b.gc else None,
        ] if s]
        # Si un override a touché le bloc sacre, marque-le explicitement.
        if overrides and "sacre" in overrides:
            sources_edge.append("manual_overrides")
        edge = {
            "consacre": b.slug,
            "consecrateur_principal": final_sacre.get("consecrateur_principal"),
            "co_consecrateurs": final_sacre.get("co_consecrateurs") or [],
            "date": final_sacre.get("date"),
            "lieu": final_sacre.get("lieu"),
            "sources": sources_edge,
        }

    # Index plat
    naiss_y = parse_date_year(naissance if isinstance(naissance, str) else None) or (naissance if isinstance(naissance, int) else None)
    deces_y = parse_date_year(deces if isinstance(deces, str) else None) or (deces if isinstance(deces, int) else None)
    index_row = {
        "slug": b.slug,
        "nom": name,
        "naissance_annee": naiss_y,
        "deces_annee": deces_y,
        "fraternite": fraternite,
        "rang": rang,
        "pays": nationalite,
        "photo_disponible": photo is not None,
        "wikidata_qid": b.wikidata_qid,
    }

    return doc, edge, index_row, dict(anom)


def _completeness(rec: dict[str, Any], fields: list[str]) -> float:
    n = sum(1 for f in fields if rec.get(f))
    return round(n / len(fields), 2)


def _resolve_consecrator(
    qid: str | None,
    ch_code: str | None,
    *,
    qid_to_slug: dict[str, str],
    code_to_slug: dict[str, str],
    label_hint: str | None = None,
) -> str | None:
    """Résout un consécrateur en slug local, ou crée un placeholder."""
    if ch_code and ch_code in code_to_slug:
        return code_to_slug[ch_code]
    if qid and qid in qid_to_slug:
        return qid_to_slug[qid]
    # placeholder déterministe basé sur l'identifiant le plus stable disponible
    if qid:
        return f"non-indexe-{hashlib.md5(qid.encode()).hexdigest()[:8]}"
    if ch_code:
        return f"non-indexe-{hashlib.md5(ch_code.encode()).hexdigest()[:8]}"
    if label_hint:
        return f"non-indexe-{hashlib.md5(label_hint.encode('utf-8')).hexdigest()[:8]}"
    return None


def _build_sacre(
    b: Bishop,
    *,
    qid_to_slug: dict[str, str],
    code_to_slug: dict[str, str],
    anom: dict[str, int],
) -> dict[str, Any]:
    date = _pick(
        b.ch.get("consecration_date") if b.ch else None,
        b.wd.get("consecration_date") if b.wd else None,
        b.gc.get("consecration_date") if b.gc else None,
    )
    lieu = _pick(
        b.ch.get("consecration_place") if b.ch else None,
    )
    source_urls: list[str] = []
    if b.ch and b.ch.get("source_url"):
        source_urls.append(b.ch["source_url"])
    if b.gc and b.gc.get("source_url"):
        source_urls.append(b.gc["source_url"])
    if b.wd and b.wd.get("wikipedia_fr_url"):
        source_urls.append(b.wd["wikipedia_fr_url"])

    principal: str | None = None
    co: list[str] = []

    # 1. CH a la meilleure info (distingue principal/co)
    if b.ch and (b.ch.get("consecrator_principal_ch_code") or b.ch.get("co_consecrator_ch_codes")):
        if b.ch.get("consecrator_principal_ch_code"):
            principal = _resolve_consecrator(
                None,
                b.ch["consecrator_principal_ch_code"],
                qid_to_slug=qid_to_slug,
                code_to_slug=code_to_slug,
                label_hint=b.ch.get("consecrated_by_label"),
            )
        for cc in b.ch.get("co_consecrator_ch_codes") or []:
            slug = _resolve_consecrator(None, cc, qid_to_slug=qid_to_slug, code_to_slug=code_to_slug)
            if slug:
                co.append(slug)
    # 2. WD sinon
    elif b.wd:
        wd_principal = b.wd.get("consecrator_principal_qid")
        wd_all = b.wd.get("consecrator_qids") or []
        wd_co = b.wd.get("co_consecrator_qids") or []
        # Si pas de principal mais ≥5 consécrateurs : anomalie connue, on laisse principal null
        if not wd_principal and len(wd_all) >= 5:
            anom["many_consec_no_principal"] += 1
            for q in wd_all:
                slug = _resolve_consecrator(q, None, qid_to_slug=qid_to_slug, code_to_slug=code_to_slug)
                if slug:
                    co.append(slug)
        else:
            if wd_principal:
                principal = _resolve_consecrator(
                    wd_principal, None, qid_to_slug=qid_to_slug, code_to_slug=code_to_slug
                )
            elif wd_all and not wd_co:
                # un seul consécrateur déclaré sans étiquette « principal »
                if len(wd_all) == 1:
                    principal = _resolve_consecrator(
                        wd_all[0], None, qid_to_slug=qid_to_slug, code_to_slug=code_to_slug
                    )
            for q in wd_co:
                slug = _resolve_consecrator(q, None, qid_to_slug=qid_to_slug, code_to_slug=code_to_slug)
                if slug:
                    co.append(slug)
        if not principal and date:
            anom["consec_date_no_principal"] += 1

    # Construit le bloc seulement si quelque chose est connu (sinon dict vide)
    sacre: dict[str, Any] = {
        "date": normalize_wikidata_date(date) if date else None,
        "lieu": lieu,
        "consecrateur_principal": principal,
        "co_consecrateurs": co or None,
        "source_urls": source_urls or None,
    }
    # supprime les clés vides pour des YAML plus propres
    return {k: v for k, v in sacre.items() if v not in (None, [], "")}


def _build_fonctions(b: Bishop) -> list[dict[str, Any]] | None:
    fonctions: list[dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()

    def add(titre: str | None, siege: str | None, du: str | None, au: str | None, institut: str | None = None):
        if not titre and not siege:
            return
        key = (str(titre or ""), str(siege or ""), str(du or ""))
        if key in seen:
            return
        seen.add(key)
        entry: dict[str, Any] = {"titre": titre}
        if siege:
            entry["siege"] = siege
        if institut:
            entry["institut"] = institut
        entry["du"] = du
        entry["au"] = au
        fonctions.append(entry)

    if b.ch:
        for p in b.ch.get("positions") or []:
            add(p.get("title"), None, p.get("start"), p.get("end"))
    if b.gc:
        for p in b.gc.get("positions") or []:
            siege = p.get("diocese")
            add(p.get("title"), siege, p.get("start"), p.get("end"))
    if b.wd:
        for p in b.wd.get("positions") or []:
            titre = p.get("label_fr") or p.get("label_en")
            add(titre, None, p.get("start"), p.get("end"))

    return fonctions or None


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser(description="Réconciliation cross-sources du corpus clergé")
    ap.add_argument("--dry-run", action="store_true", help="N'écrit aucun fichier, affiche les stats")
    ap.add_argument("--limit", type=int, default=None, help="Limite le nombre d'évêques traités")
    ap.add_argument(
        "--only-cross-sourced",
        action="store_true",
        help="Ne génère un YAML que pour les évêques présents dans ≥2 sources",
    )
    args = ap.parse_args()

    stats = reconcile(
        limit=args.limit,
        only_cross_sourced=args.only_cross_sourced,
        dry_run=args.dry_run,
    )
    print(json.dumps(stats, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
