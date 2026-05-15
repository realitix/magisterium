"""
Annote le rite de chaque consécration épiscopale + calcule le tampon
`ordo_*` propagé via la lignée ascendante.

Phase 5 du pipeline clergé. Idempotent : rejouer la commande produit le
même résultat (mêmes hashes).

Heuristique (premier match gagne, ordre strict) :

1. Manual override (`manual_overrides.yaml`, section `consecrations`)
   → `rite_source=manual`.
2. Date ≤ 1968-06-29 → `rite=ancien` (avant la promulgation du nouveau
   rite par *Pontificalis Romani*).
3. Date ≥ 1968-06-30 + consécrateur principal connu :
   - Si fraternité du consécrateur ∈ {fsspx, fsspx-fondateur, sede-*,
     vieux-cath, orient-*, palmar} → `rite=ancien`.
   - Si fraternité ∈ Ecclesia Dei {fssp, icrsp, ibp, icr} → `rite=ancien`
     avec note "Ecclesia Dei, indult" + ajout à la liste de révision.
   - Sinon (obédience `rome`) → `rite=nouveau`.
4. Date ≥ 1968-06-30 + consécrateur inconnu → `rite=inconnu`.
5. Date inconnue → `rite=inconnu`.

Tampon (sur la chaîne sacre + ancêtres jusqu'à un point d'ancrage
pré-1900 ou un évêque sans consécrateur connu) :

- ≥1 arête `nouveau` → `ordo_dubius`
- sinon ≥1 arête `inconnu` → `ordo_incertus`
- sinon tout `ancien` → `ordo_validus`

Si le sacre de l'évêque lui-même est totalement inconnu (pas de date ET
pas de consécrateur), tampon `ordo_incertus` direct.
"""

from __future__ import annotations

import argparse
import json
import logging
import re
import sys
from collections import OrderedDict
from datetime import date as date_cls
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Set, Tuple

import yaml

ROOT = Path(__file__).resolve().parent.parent
CLERGE = ROOT / "clerge"
EVEQUES_DIR = CLERGE / "eveques"
METADATA_DIR = CLERGE / "_metadata"
CONSECRATIONS_PATH = METADATA_DIR / "consecrations.jsonl"
OVERRIDES_PATH = METADATA_DIR / "manual_overrides.yaml"
LINEAGES_PATH = METADATA_DIR / "lineages.json"
ECCLESIA_DEI_REVIEW_PATH = METADATA_DIR / "ecclesia_dei_review.json"
STATS_PATH = METADATA_DIR / "stats.json"

CUTOFF = date_cls(1968, 6, 29)  # dernier jour de l'ancien rite

# Fraternités tradi / non-romaines → ancien rite après 1968.
TRADI_FRATERNITES = {"fsspx", "fsspx-fondateur", "palmar"}
TRADI_PREFIXES = ("sede-", "vieux-cath", "orient-")

# Fraternités Ecclesia Dei (indult) → ancien rite par défaut + révision.
ECCLESIA_DEI_FRATERNITES = {"fssp", "icrsp", "ibp", "icr"}

PLACEHOLDER_PREFIX = "non-indexe-"
MAX_LINEAGE_DEPTH = 100
ANCHOR_YEAR = 1900  # un évêque né avant 1900 = point d'ancrage

logger = logging.getLogger("clerge_annotate_rite")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def parse_iso_date(value: Any) -> Optional[date_cls]:
    """Tolère 'YYYY-MM-DD', 'YYYY-MM', 'YYYY', None."""
    if value is None:
        return None
    if isinstance(value, date_cls):
        return value
    s = str(value).strip()
    if not s:
        return None
    m = re.match(r"^(\d{4})(?:-(\d{2}))?(?:-(\d{2}))?", s)
    if not m:
        return None
    year = int(m.group(1))
    month = int(m.group(2)) if m.group(2) else 1
    day = int(m.group(3)) if m.group(3) else 1
    try:
        return date_cls(year, month, day)
    except ValueError:
        return None


def year_of(value: Any) -> Optional[int]:
    d = parse_iso_date(value)
    return d.year if d else None


def is_placeholder(slug: Optional[str]) -> bool:
    return bool(slug) and slug.startswith(PLACEHOLDER_PREFIX)


# ---------------------------------------------------------------------------
# Chargement
# ---------------------------------------------------------------------------


def load_overrides() -> Tuple[Dict[Tuple[str, Optional[str]], dict], dict]:
    """
    Retourne (consec_overrides, eveques_overrides).
    consec_overrides : clé (consacre, consecrateur_principal) → dict avec rite/note.
    eveques_overrides : slug → bloc sacre du yaml override.
    """
    if not OVERRIDES_PATH.exists():
        return {}, {}
    data = yaml.safe_load(OVERRIDES_PATH.read_text(encoding="utf-8")) or {}
    consec_list = data.get("consecrations") or []
    consec_map: Dict[Tuple[str, Optional[str]], dict] = {}
    for item in consec_list:
        consacre = item.get("consacre")
        consecrateur = item.get("consecrateur_principal")
        if not consacre:
            continue
        consec_map[(consacre, consecrateur)] = item
        # Aussi indexer sans consecrateur pour un fallback large
        consec_map.setdefault((consacre, None), item)
    eveques = data.get("eveques") or {}
    return consec_map, eveques


def load_consecrations() -> List[OrderedDict]:
    edges: List[OrderedDict] = []
    with CONSECRATIONS_PATH.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            edges.append(json.loads(line, object_pairs_hook=OrderedDict))
    return edges


def yaml_load(path: Path) -> dict:
    with path.open("r", encoding="utf-8") as fh:
        return yaml.safe_load(fh) or {}


def yaml_dump(data: dict, path: Path) -> None:
    text = yaml.safe_dump(
        data,
        default_flow_style=False,
        sort_keys=False,
        allow_unicode=True,
    )
    path.write_text(text, encoding="utf-8")


# ---------------------------------------------------------------------------
# Index des évêques (en mémoire pour rapidité)
# ---------------------------------------------------------------------------


class EvequeIndex:
    """Index minimal en mémoire : slug → champs nécessaires."""

    def __init__(self) -> None:
        self.by_slug: Dict[str, dict] = {}
        self.paths: Dict[str, Path] = {}

    def load_all(self, limit: Optional[int] = None) -> None:
        files = sorted(EVEQUES_DIR.glob("*.yaml"))
        if limit:
            files = files[:limit]
        for path in files:
            slug = path.stem
            data = yaml_load(path)
            sacre = data.get("sacre") or {}
            obediences = data.get("obediences") or []
            last_obedience = obediences[-1] if obediences else None
            self.by_slug[slug] = {
                "slug": slug,
                "naissance_year": year_of(data.get("naissance")),
                "sacre_date": parse_iso_date(sacre.get("date")),
                "sacre_consecrateur": sacre.get("consecrateur_principal"),
                "fraternite": data.get("fraternite"),
                "obedience_statut": (last_obedience or {}).get("statut"),
            }
            self.paths[slug] = path

    def get(self, slug: str) -> Optional[dict]:
        return self.by_slug.get(slug)


# ---------------------------------------------------------------------------
# Détermination du rite d'une arête
# ---------------------------------------------------------------------------


def is_tradi_fraternite(frat: Optional[str]) -> bool:
    if not frat:
        return False
    if frat in TRADI_FRATERNITES:
        return True
    return any(frat.startswith(p) for p in TRADI_PREFIXES)


def is_ecclesia_dei(frat: Optional[str]) -> bool:
    return bool(frat) and frat in ECCLESIA_DEI_FRATERNITES


def determine_rite(
    edge: dict,
    eveques: EvequeIndex,
    overrides: Dict[Tuple[str, Optional[str]], dict],
) -> Tuple[str, str, Optional[str]]:
    """Retourne (rite, rite_source, rite_note?)."""
    consacre = edge.get("consacre")
    consecrateur = edge.get("consecrateur_principal")

    # 1. Manual override
    key = (consacre, consecrateur)
    override = overrides.get(key) or overrides.get((consacre, None))
    if override and override.get("rite"):
        return override["rite"], "manual", override.get("note")

    sacre_date = parse_iso_date(edge.get("date"))

    # 2. Date ≤ cutoff
    if sacre_date is not None and sacre_date <= CUTOFF:
        return "ancien", "inferred", None

    # 5. Date inconnue
    if sacre_date is None:
        return "inconnu", "inferred", None

    # Date ≥ 1968-06-30
    if not consecrateur or is_placeholder(consecrateur):
        return "inconnu", "inferred", None

    cons_info = eveques.get(consecrateur)
    if cons_info is None:
        # Consécrateur cité mais sans fiche YAML
        return "inconnu", "inferred", None

    frat = cons_info.get("fraternite")
    if is_tradi_fraternite(frat):
        return "ancien", "inferred", None
    if is_ecclesia_dei(frat):
        return "ancien", "inferred", "Ecclesia Dei, indult"

    # Par défaut : obédience rome → nouveau rite
    if cons_info.get("obedience_statut") == "rome" or cons_info.get("obedience_statut") is None:
        return "nouveau", "inferred", None

    return "inconnu", "inferred", None


# ---------------------------------------------------------------------------
# Annotation des arêtes
# ---------------------------------------------------------------------------


def annotate_edges(
    edges: List[OrderedDict],
    eveques: EvequeIndex,
    overrides: Dict[Tuple[str, Optional[str]], dict],
) -> Tuple[List[OrderedDict], Dict[str, OrderedDict], List[dict]]:
    """
    Annote chaque arête. Retourne :
    - edges (annotées, ordre préservé)
    - edges_by_consacre (slug → edge ; pour lookup)
    - ecclesia_dei_review (liste)
    """
    review: List[dict] = []
    by_consacre: Dict[str, OrderedDict] = {}

    for edge in edges:
        rite, source, note = determine_rite(edge, eveques, overrides)

        # Réorganiser pour insérer rite/rite_source juste après co_consecrateurs
        # (ordre canonique : consacre, consecrateur_principal, co_consecrateurs,
        #  rite, rite_source, [rite_note], date, lieu, sources)
        new_edge: "OrderedDict[str, Any]" = OrderedDict()
        canonical_order = [
            "consacre",
            "consecrateur_principal",
            "co_consecrateurs",
            "rite",
            "rite_source",
            "rite_note",
            "date",
            "lieu",
            "sources",
        ]
        # Drop any pre-existing rite fields to ensure idempotence
        edge_clean = {k: v for k, v in edge.items() if k not in {"rite", "rite_source", "rite_note"}}
        edge_clean["rite"] = rite
        edge_clean["rite_source"] = source
        if note:
            edge_clean["rite_note"] = note

        for key in canonical_order:
            if key in edge_clean:
                new_edge[key] = edge_clean[key]
        # Préserver tout champ inconnu
        for key, val in edge_clean.items():
            if key not in new_edge:
                new_edge[key] = val

        by_consacre[edge["consacre"]] = new_edge

        if note == "Ecclesia Dei, indult":
            review.append(
                {
                    "consacre": edge.get("consacre"),
                    "consecrateur_principal": edge.get("consecrateur_principal"),
                    "date": edge.get("date"),
                    "fraternite_consecrateur": (eveques.get(edge.get("consecrateur_principal") or "") or {}).get("fraternite"),
                }
            )

        # Replace edge in list
        idx = edges.index(edge) if False else None  # type: ignore
        # We will rebuild list below; placeholder

    # Rebuild edges list preserving order
    annotated: List[OrderedDict] = []
    for edge in edges:
        annotated.append(by_consacre[edge["consacre"]] if edge["consacre"] in by_consacre and by_consacre[edge["consacre"]].get("consecrateur_principal") == edge.get("consecrateur_principal") else edge)

    # Note : un consacre peut avoir plusieurs lignes ? Vérifions plus loin.
    # Par sécurité, on annote dans l'ordre d'origine sans déduplication.
    annotated = []
    for edge in edges:
        rite, source, note = determine_rite(edge, eveques, overrides)
        edge_clean = OrderedDict()
        for k, v in edge.items():
            if k in {"rite", "rite_source", "rite_note"}:
                continue
            edge_clean[k] = v
        # Insérer rite/rite_source/rite_note dans l'ordre canonique
        out: "OrderedDict[str, Any]" = OrderedDict()
        canonical_order = [
            "consacre",
            "consecrateur_principal",
            "co_consecrateurs",
            "rite",
            "rite_source",
            "rite_note",
            "date",
            "lieu",
            "sources",
        ]
        merged = dict(edge_clean)
        merged["rite"] = rite
        merged["rite_source"] = source
        if note:
            merged["rite_note"] = note
        for key in canonical_order:
            if key in merged:
                out[key] = merged[key]
        for key, val in merged.items():
            if key not in out:
                out[key] = val
        annotated.append(out)

    # by_consacre = dernière arête vue par évêque consacré
    by_consacre = {}
    for e in annotated:
        by_consacre[e["consacre"]] = e

    return annotated, by_consacre, review


# ---------------------------------------------------------------------------
# Lignée + tampon
# ---------------------------------------------------------------------------


def compute_lineage(
    slug: str,
    by_consacre: Dict[str, dict],
    eveques: EvequeIndex,
) -> dict:
    """
    Remonte la chaîne consacre → consecrateur_principal.
    Retourne dict avec ancestors, depth, tampon, new_rite_count, unknown_count,
    oldest_anchor.
    """
    ancestors: List[str] = []
    visited: Set[str] = set()
    new_rite_count = 0
    unknown_count = 0
    ancien_count = 0
    sacre_known = False

    current_slug = slug
    cycle_detected = False
    oldest_anchor: Optional[str] = None

    for _ in range(MAX_LINEAGE_DEPTH):
        if current_slug in visited:
            cycle_detected = True
            break
        visited.add(current_slug)

        # Point d'ancrage pré-1900 : on s'arrête là, sans compter en `inconnu`.
        info = eveques.get(current_slug) or {}
        ny = info.get("naissance_year")
        if current_slug != slug and ny is not None and ny < ANCHOR_YEAR:
            oldest_anchor = current_slug
            break

        edge = by_consacre.get(current_slug)
        if not edge:
            # Pas d'arête connue
            if current_slug == slug:
                # sacre du sujet inconnu
                unknown_count += 1
            else:
                # Ancêtre sans arête : si pré-1900 on aurait break plus haut,
                # sinon ancrage muet (pas d'incrément).
                oldest_anchor = oldest_anchor or current_slug
            break

        rite = edge.get("rite")
        consecrateur = edge.get("consecrateur_principal")
        date_v = parse_iso_date(edge.get("date"))

        # Tracking : le sacre du sujet existe-t-il ?
        if current_slug == slug:
            sacre_known = bool(date_v) or (consecrateur and not is_placeholder(consecrateur))

        if rite == "nouveau":
            new_rite_count += 1
        elif rite == "inconnu":
            unknown_count += 1
        elif rite == "ancien":
            ancien_count += 1

        if not consecrateur or is_placeholder(consecrateur):
            oldest_anchor = oldest_anchor or current_slug
            break

        if consecrateur not in eveques.by_slug:
            ancestors.append(consecrateur)
            oldest_anchor = consecrateur
            break

        ancestors.append(consecrateur)
        current_slug = consecrateur
    else:
        oldest_anchor = oldest_anchor or current_slug

    if oldest_anchor is None and ancestors:
        oldest_anchor = ancestors[-1]
    elif oldest_anchor is None:
        oldest_anchor = slug

    # Tampon
    info_self = eveques.get(slug) or {}
    has_own_sacre = bool(info_self.get("sacre_date")) or (
        info_self.get("sacre_consecrateur")
        and not is_placeholder(info_self.get("sacre_consecrateur"))
    )

    if not has_own_sacre and slug not in by_consacre:
        tampon = "ordo_incertus"
    elif new_rite_count > 0:
        tampon = "ordo_dubius"
    elif unknown_count > 0:
        tampon = "ordo_incertus"
    else:
        tampon = "ordo_validus"

    return {
        "ancestors": ancestors,
        "depth": len(ancestors),
        "tampon": tampon,
        "new_rite_count": new_rite_count,
        "unknown_count": unknown_count,
        "oldest_anchor": oldest_anchor,
        "cycle_detected": cycle_detected,
    }


# ---------------------------------------------------------------------------
# Écriture des YAML évêques
# ---------------------------------------------------------------------------


# Ordre canonique d'une fiche évêque (en tête)
CANONICAL_TOP_ORDER = [
    "slug",
    "nom",
    "nom_complet",
    "naissance",
    "deces",
    "naissance_lieu",
    "deces_lieu",
    "rang",
    "nationalite",
    "tampon",
    "sacre",
    "obediences",
    "fonctions",
    "fraternite",
    "photo",
    "qids",
    "sources",
    "notes",
]

SACRE_ORDER = [
    "date",
    "lieu",
    "consecrateur_principal",
    "co_consecrateurs",
    "rite",
    "rite_source",
    "rite_note",
    "source_urls",
]


def reorder_dict(data: dict, key_order: List[str]) -> dict:
    out: "OrderedDict[str, Any]" = OrderedDict()
    for k in key_order:
        if k in data:
            out[k] = data[k]
    for k, v in data.items():
        if k not in out:
            out[k] = v
    return dict(out)  # plain dict for yaml.safe_dump


def write_eveque_yaml(
    slug: str,
    path: Path,
    tampon: str,
    rite: Optional[str],
    rite_source: Optional[str],
    rite_note: Optional[str],
) -> None:
    data = yaml_load(path)
    if not data:
        return

    data["tampon"] = tampon
    sacre = data.get("sacre") or {}
    if rite is not None:
        sacre["rite"] = rite
    if rite_source is not None:
        sacre["rite_source"] = rite_source
    if rite_note is not None:
        sacre["rite_note"] = rite_note
    elif "rite_note" in sacre and not rite_note:
        # Nettoyer une éventuelle note obsolète
        del sacre["rite_note"]
    if sacre:
        sacre = reorder_dict(sacre, SACRE_ORDER)
        data["sacre"] = sacre

    data = reorder_dict(data, CANONICAL_TOP_ORDER)
    yaml_dump(data, path)


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def write_consecrations(edges: List[OrderedDict]) -> None:
    """Écrit consecrations.jsonl de manière déterministe (un objet par ligne)."""
    lines = []
    for e in edges:
        lines.append(json.dumps(e, ensure_ascii=False))
    CONSECRATIONS_PATH.write_text("\n".join(lines) + "\n", encoding="utf-8")


def run(dry_run: bool = False, limit: Optional[int] = None) -> dict:
    logger.info("Chargement des overrides…")
    overrides, _eveques_overrides = load_overrides()

    logger.info("Chargement de l'index des évêques (%s)…", "limité" if limit else "complet")
    eveques = EvequeIndex()
    eveques.load_all(limit=limit)
    logger.info("  %d évêques en mémoire", len(eveques.by_slug))

    logger.info("Chargement de consecrations.jsonl…")
    edges = load_consecrations()
    logger.info("  %d arêtes", len(edges))

    logger.info("Annotation du rite des arêtes…")
    annotated, by_consacre, review = annotate_edges(edges, eveques, overrides)

    # Stats par rite
    par_rite: Dict[str, int] = {"ancien": 0, "nouveau": 0, "mixte": 0, "inconnu": 0}
    for e in annotated:
        par_rite[e["rite"]] = par_rite.get(e["rite"], 0) + 1
    logger.info("  par_rite: %s", par_rite)

    logger.info("Calcul des lignées…")
    lineages: Dict[str, dict] = {}
    par_tampon: Dict[str, int] = {"ordo_validus": 0, "ordo_dubius": 0, "ordo_incertus": 0}

    slugs = list(eveques.by_slug.keys())
    for i, slug in enumerate(slugs):
        if i % 5000 == 0:
            logger.info("  %d/%d", i, len(slugs))
        lin = compute_lineage(slug, by_consacre, eveques)
        lineages[slug] = {
            "ancestors": lin["ancestors"],
            "depth": lin["depth"],
            "tampon": lin["tampon"],
            "new_rite_count": lin["new_rite_count"],
            "unknown_count": lin["unknown_count"],
            "oldest_anchor": lin["oldest_anchor"],
        }
        par_tampon[lin["tampon"]] = par_tampon.get(lin["tampon"], 0) + 1
    logger.info("  par_tampon: %s", par_tampon)

    summary = {
        "par_rite": par_rite,
        "par_tampon": par_tampon,
        "ecclesia_dei_review_count": len(review),
        "lineages_total": len(lineages),
        "lineages_non_trivial": sum(1 for l in lineages.values() if l["depth"] >= 1),
        "anomalies": {
            "cycles_detected": 0,  # peut être enrichi
        },
    }

    if dry_run:
        logger.info("Dry-run — pas d'écriture.")
        return summary

    logger.info("Écriture consecrations.jsonl…")
    write_consecrations(annotated)

    logger.info("Écriture lineages.json…")
    LINEAGES_PATH.write_text(
        json.dumps(lineages, ensure_ascii=False, sort_keys=True, indent=None) + "\n",
        encoding="utf-8",
    )

    logger.info("Écriture ecclesia_dei_review.json (%d entrées)…", len(review))
    ECCLESIA_DEI_REVIEW_PATH.write_text(
        json.dumps(review, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    logger.info("Mise à jour des YAML évêques…")
    # On a besoin du rite par évêque (celui de SA propre consécration)
    for i, slug in enumerate(slugs):
        if i % 5000 == 0:
            logger.info("  %d/%d", i, len(slugs))
        path = eveques.paths.get(slug)
        if not path:
            continue
        own_edge = by_consacre.get(slug)
        rite = own_edge.get("rite") if own_edge else None
        rite_source = own_edge.get("rite_source") if own_edge else None
        rite_note = own_edge.get("rite_note") if own_edge else None
        tampon = lineages[slug]["tampon"]
        # Si pas d'arête (sacre totalement inconnu), inscrire 'inconnu'
        if not own_edge:
            rite = "inconnu"
            rite_source = "inferred"
        write_eveque_yaml(slug, path, tampon, rite, rite_source, rite_note)

    logger.info("Mise à jour stats.json…")
    stats = json.loads(STATS_PATH.read_text(encoding="utf-8")) if STATS_PATH.exists() else {}
    stats["par_rite"] = par_rite
    stats["par_tampon"] = par_tampon
    stats["ecclesia_dei_review_count"] = len(review)
    STATS_PATH.write_text(json.dumps(stats, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    logger.info("Terminé.")
    return summary


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Stats sans écrire")
    parser.add_argument("--limit", type=int, default=None, help="Ne traiter que N évêques")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    summary = run(dry_run=args.dry_run, limit=args.limit)
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
