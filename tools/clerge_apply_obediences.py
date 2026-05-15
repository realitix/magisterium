"""
Corrige les obédiences et la fraternité des évêques traditionalistes /
sédévacantistes / Ecclesia Dei présents dans `clerge/eveques/`.

Phase 6 du pipeline clergé. Objectif : remplacer le défaut
`obediences=[{statut: rome, du: null, au: null}]` par des périodes
chronologiques exactes pour les évêques tradi (FSSPX, sédé Thuc-line,
Cassiciacum, Palmar, vieux-cath., etc.), ce qui débloque ensuite la
phase 5 (annotation du rite) en aval — les consécrations effectuées
par Lefebvre/Williamson/Thuc-après-1981 ne sont plus marquées
"rite=nouveau" par défaut.

Le module fait aussi office d'éditeur ciblé de `manual_overrides.yaml`
pour ajouter des arêtes de consécration dont la date manque dans le
scraping Wikidata (sacres d'Écône 30 juin 1988, etc.) — sinon le moteur
de phase 5 retombe en `rite=inconnu` par défaut.

Idempotent : rejouer le script sur un YAML déjà mis à jour ne change
rien (signatures stables).

Sortie : `clerge/_metadata/obedience_changes.json` — journal des
modifications avec source URL pour chaque évêque corrigé.

Usage:

    uv run python -m tools.clerge_apply_obediences            # applique
    uv run python -m tools.clerge_apply_obediences --dry-run  # diff seul
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import sys
from collections import OrderedDict
from pathlib import Path
from typing import Any, Dict, List, Optional

import yaml

ROOT = Path(__file__).resolve().parent.parent
CLERGE = ROOT / "clerge"
EVEQUES_DIR = CLERGE / "eveques"
METADATA_DIR = CLERGE / "_metadata"
OVERRIDES_PATH = METADATA_DIR / "manual_overrides.yaml"
CHANGES_PATH = METADATA_DIR / "obedience_changes.json"
CAS_FRONTIERE_PATH = METADATA_DIR / "cas_frontiere.json"

logger = logging.getLogger("clerge_apply_obediences")

# ---------------------------------------------------------------------------
# Table curée des évêques tradi/sédé
# ---------------------------------------------------------------------------
#
# Pour chaque slug :
#   obediences   : liste ordonnée {du, au, statut}
#   fraternite   : étiquette finale
#   source       : URL principale qui documente la rupture canonique
#   note         : commentaire bref pour le journal
#
# La présence du YAML dans `clerge/eveques/` est vérifiée avant l'application :
# une entrée inconnue est seulement loggée et ignorée.

OBEDIENCE_OVERRIDES: List[Dict[str, Any]] = [
    # ----- Famille Lefebvre / FSSPX -----
    {
        "slug": "marcel-lefebvre",
        "fraternite": "fsspx-fondateur",
        "obediences": [
            {"du": None, "au": "1988-06-30", "statut": "rome"},
            {"du": "1988-06-30", "au": "1991-03-25", "statut": "fsspx-fondateur"},
        ],
        "source": "https://fr.wikipedia.org/wiki/Marcel_Lefebvre",
        "note": "Sacres d'Écône 30 juin 1988 — déclaration de schisme par Jean-Paul II (motu proprio Ecclesia Dei adflicta, 2 juillet 1988).",
    },
    {
        "slug": "antonio-de-castro-mayer",
        "fraternite": "fsspx-allie",
        "obediences": [
            {"du": None, "au": "1988-06-30", "statut": "rome"},
            {"du": "1988-06-30", "au": "1991-04-26", "statut": "fsspx-allie"},
        ],
        "source": "https://fr.wikipedia.org/wiki/Ant%C3%B4nio_de_Castro_Mayer",
        "note": "Co-consécrateur d'Écône 30 juin 1988 — déclaré schismatique par Ecclesia Dei adflicta.",
    },
    {
        "slug": "richard-williamson",
        "fraternite": "resistance",
        "obediences": [
            {"du": None, "au": "1988-06-30", "statut": "rome"},
            {"du": "1988-06-30", "au": "2012-10-04", "statut": "fsspx"},
            {"du": "2012-10-04", "au": "2025-01-29", "statut": "resistance"},
        ],
        "source": "https://fr.wikipedia.org/wiki/Richard_Williamson",
        "note": "Sacré à Écône le 30 juin 1988. Exclu de la FSSPX le 4 octobre 2012, fonde le mouvement de la Résistance.",
    },
    {
        "slug": "bernard-fellay",
        "fraternite": "fsspx",
        "obediences": [
            {"du": None, "au": "1988-06-30", "statut": "rome"},
            {"du": "1988-06-30", "au": None, "statut": "fsspx"},
        ],
        "source": "https://fr.wikipedia.org/wiki/Bernard_Fellay",
        "note": "Sacré à Écône le 30 juin 1988. Supérieur général FSSPX 1994-2018.",
    },
    # ----- Famille Thuc -----
    {
        "slug": "pierre-martin-ngo-dinh-thuc",
        "fraternite": "sede-thuc-line",
        "obediences": [
            {"du": None, "au": "1981-05-07", "statut": "rome"},
            {"du": "1981-05-07", "au": "1984-12-13", "statut": "sede-thuc-line"},
        ],
        "source": "https://fr.wikipedia.org/wiki/Pierre_Martin_Ng%C3%B4_%C4%90%C3%ACnh_Th%E1%BB%A5c",
        "note": "Sacres de Palmar de Troya (11 janvier 1976) puis Toulon (7 mai 1981, Guérard des Lauriers). Excommunié latae sententiae le 17 septembre 1976 puis ratifié par la CDF en 1983.",
    },
    {
        "slug": "alfredo-mendez-gonzalez",
        # Cas frontière : reste obédience Rome essentiellement, rapprochements
        # tradi en fin de vie (consacre Robert Fidelis McKenna en 1986).
        # On laisse `rome` par défaut et on inscrit le cas frontière.
        "skip": True,
        "frontiere": True,
        "source": "https://fr.wikipedia.org/wiki/Alfredo_M%C3%A9ndez-Gonzalez",
        "note": "Évêque émérite d'Arecibo (Porto Rico) ; co-consécrateur de Robert Fidelis McKenna en 1986 (lignée Thuc-Carmona). Statut canonique exact à clarifier — n'a jamais été déclaré schismatique. Conservé `rome`.",
    },
    # ----- Évêques sacrés par Williamson, descendants -----
    {
        "slug": "jean-michel-faure",
        "fraternite": "resistance",
        "obediences": [
            {"du": None, "au": "2015-03-19", "statut": "rome"},
            {"du": "2015-03-19", "au": None, "statut": "resistance"},
        ],
        "source": "https://fr.wikipedia.org/wiki/Jean-Michel_Faure",
        "note": "Sacré le 19 mars 2015 par Richard Williamson (Résistance, post-FSSPX 2012).",
    },
    {
        "slug": "tomas-de-aquino-ferreira-da-costa",
        "fraternite": "resistance",
        "obediences": [
            {"du": None, "au": "2016-03-19", "statut": "rome"},
            {"du": "2016-03-19", "au": None, "statut": "resistance"},
        ],
        "source": "https://en.wikipedia.org/wiki/Tom%C3%A1s_de_Aquino_Ferreira_da_Costa",
        "note": "Sacré le 19 mars 2016 par Richard Williamson, moine bénédictin de Santa Cruz (Brésil).",
    },
    {
        "slug": "paul-morgan",
        # En réalité Paul Morgan n'a probablement pas été sacré évêque par
        # Williamson — Wikidata confond avec un prêtre FSSPX du même nom.
        # Cas frontière à arbitrer.
        "skip": True,
        "frontiere": True,
        "source": "https://en.wikipedia.org/wiki/Paul_Morgan_(priest)",
        "note": "Fiche Wikidata Q-douteuse : Paul Morgan FSSPX (1963-) est prêtre, non évêque. Donnée Wikidata incorrecte (consécrateur = Williamson, pas de date). À clarifier manuellement.",
    },
    {
        "slug": "licinio-rangel",
        "fraternite": "rome",  # réconcilié avec Rome
        "obediences": [
            {"du": None, "au": "1991-07-28", "statut": "rome"},
            {"du": "1991-07-28", "au": "2002-01-18", "statut": "fsspx-allie"},
            {"du": "2002-01-18", "au": "2002-12-16", "statut": "rome"},
        ],
        "source": "https://fr.wikipedia.org/wiki/Licinio_Rangel",
        "note": "Sacré 1991 par 3 évêques FSSPX (Tissier, Williamson, Galarreta) pour succéder à Castro Mayer à Campos. Réconcilié avec Rome le 18 janvier 2002 (Administration apostolique Saint-Jean-Marie-Vianney).",
    },
    {
        "slug": "carlo-maria-vigano",
        # Cas Wikidata erroné : Viganò a été sacré le 26 avril 1992 par
        # le card. Re — pas par Williamson. Conserve `rome` puis rupture 2024.
        "fraternite": "rome",
        "obediences": [
            {"du": None, "au": "2024-07-04", "statut": "rome"},
            {"du": "2024-07-04", "au": None, "statut": "rupture-canonique"},
        ],
        "source": "https://fr.wikipedia.org/wiki/Carlo_Maria_Vigan%C3%B2",
        "note": "Donnée Wikidata du consécrateur INCORRECTE (Williamson au lieu du card. Re). Conservé `rome` jusqu'à excommunication latae sententiae pour schisme du 5 juillet 2024 (DDF).",
    },
]


# Arêtes manuelles à pousser dans manual_overrides.yaml : sacres dont la date
# manque dans le scraping Wikidata mais qu'on connaît parfaitement.
# Ces overrides permettent à la phase 5 de classer correctement le rite
# (ancien) au lieu d'un défaut `inconnu`.
CONSECRATION_OVERRIDES: List[Dict[str, Any]] = [
    {
        "consacre": "bernard-fellay",
        "consecrateur_principal": "marcel-lefebvre",
        "date": "1988-06-30",
        "rite": "ancien",
        "rite_source": "manual",
        "note": "Sacres d'Écône 30 juin 1988 (ancien rite, pontifical romain de 1962).",
        "source": "https://fr.wikipedia.org/wiki/Bernard_Fellay",
    },
    {
        "consacre": "richard-williamson",
        "consecrateur_principal": "marcel-lefebvre",
        "date": "1988-06-30",
        "rite": "ancien",
        "rite_source": "manual",
        "note": "Sacres d'Écône 30 juin 1988 (ancien rite, pontifical romain de 1962).",
        "source": "https://fr.wikipedia.org/wiki/Richard_Williamson",
    },
    {
        "consacre": "jean-michel-faure",
        "consecrateur_principal": "richard-williamson",
        "date": "2015-03-19",
        "rite": "ancien",
        "rite_source": "manual",
        "note": "Sacré par Williamson au monastère de Santa Cruz (Brésil), Résistance, ancien rite.",
        "source": "https://fr.wikipedia.org/wiki/Jean-Michel_Faure",
    },
    {
        "consacre": "tomas-de-aquino-ferreira-da-costa",
        "consecrateur_principal": "richard-williamson",
        "date": "2016-03-19",
        "rite": "ancien",
        "rite_source": "manual",
        "note": "Sacré par Williamson au monastère de Santa Cruz (Brésil), ancien rite.",
        "source": "https://en.wikipedia.org/wiki/Tom%C3%A1s_de_Aquino_Ferreira_da_Costa",
    },
    {
        # Correction de la donnée Wikidata erronée : Viganò a été sacré par
        # le card. Giovanni Battista Re, pas par Williamson. On ne corrige pas
        # ici le consécrateur (cf. note du YAML) mais on force le rite à
        # `nouveau` puisque le sacre réel est de 1992 par Re (rite Paul VI).
        "consacre": "carlo-maria-vigano",
        "consecrateur_principal": "richard-williamson",
        "date": "1992-04-26",
        "rite": "nouveau",
        "rite_source": "manual",
        "note": "Donnée Wikidata erronée — Viganò sacré en 1992 par le card. Re, pas par Williamson. Rite Paul VI maintenu.",
        "source": "https://fr.wikipedia.org/wiki/Carlo_Maria_Vigan%C3%B2",
    },
]


# Cas frontière documentés (ni tranchés, ni appliqués).
CAS_FRONTIERE: List[Dict[str, Any]] = [
    {
        "slug": "alfredo-mendez-gonzalez",
        "famille": "mendez-thuc",
        "question": "Évêque catholique légitime ayant consacré R. F. McKenna en 1986 (lignée Thuc-Carmona). Jamais déclaré schismatique. Statut canonique de la consécration de McKenna ?",
        "source": "https://fr.wikipedia.org/wiki/Alfredo_M%C3%A9ndez-Gonzalez",
    },
    {
        "slug": "paul-morgan",
        "famille": "fsspx",
        "question": "Wikidata semble confondre un prêtre FSSPX (Paul Morgan, 1963-) avec un sacre épiscopal inexistant. À vérifier manuellement et probablement supprimer du corpus.",
        "source": "https://en.wikipedia.org/wiki/Paul_Morgan_(priest)",
    },
    {
        "slug": "carlo-maria-vigano",
        "famille": "rupture-recente",
        "question": "Rupture canonique 2024 — statut « rupture-canonique » ad hoc. Convient-il de créer une famille palmarienne/sédévacantiste ad hoc pour Viganò ?",
        "source": "https://fr.wikipedia.org/wiki/Carlo_Maria_Vigan%C3%B2",
    },
    {
        "slug": "licinio-rangel",
        "famille": "rangel-campos",
        "question": "Sacré 1991 par 3 évêques FSSPX (sans Lefebvre). Sacre validé par Jean-Paul II en 2002 (Administration apostolique de Campos). Comment classer ce sacre rétrospectivement validé ?",
        "source": "https://fr.wikipedia.org/wiki/Licinio_Rangel",
    },
    {
        "famille": "fsspx-non-corpus",
        "question": "Sacres FSSPX de 1988 absents du corpus Wikidata : Bernard Tissier de Mallerais (Q323378), Alfonso de Galarreta (Q318527). À ré-injecter via un scraper Wikipedia.",
        "source": "https://fr.wikipedia.org/wiki/Sacres_d%27%C3%89c%C3%B4ne",
    },
    {
        "famille": "thuc-line-non-corpus",
        "question": "Évêques de la lignée Thuc post-1981 absents du corpus : Guérard des Lauriers, Moisés Carmona Rivera, Adolfo Zamora, Clemente Domínguez (Palmar). Idem pour Robert Fidelis McKenna, Mark Pivarunas (CMRI), Daniel Dolan présent partiellement, Donald Sanborn. À compléter manuellement ou par scraping Wikipedia ciblé.",
        "source": "https://fr.wikipedia.org/wiki/Pierre_Martin_Ng%C3%B4_%C4%90%C3%ACnh_Th%E1%BB%A5c",
    },
]


# ---------------------------------------------------------------------------
# Helpers YAML
# ---------------------------------------------------------------------------


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


def reorder_dict(data: dict, key_order: List[str]) -> dict:
    out: "OrderedDict[str, Any]" = OrderedDict()
    for k in key_order:
        if k in data:
            out[k] = data[k]
    for k, v in data.items():
        if k not in out:
            out[k] = v
    return dict(out)


def _norm_obediences(obediences: List[dict]) -> List[dict]:
    """Normalise les dates pour comparaison idempotente."""
    out = []
    for ob in obediences:
        out.append(
            {
                "du": ob.get("du"),
                "au": ob.get("au"),
                "statut": ob.get("statut"),
            }
        )
    return out


# ---------------------------------------------------------------------------
# Application
# ---------------------------------------------------------------------------


def apply_to_yaml(entry: Dict[str, Any], dry_run: bool) -> Optional[Dict[str, Any]]:
    """Met à jour un YAML évêque. Retourne dict de log ou None si pas de change."""
    slug = entry["slug"]
    path = EVEQUES_DIR / f"{slug}.yaml"
    if not path.exists():
        logger.warning("YAML absent : %s (entrée ignorée)", slug)
        return {
            "slug": slug,
            "status": "yaml-absent",
            "note": entry.get("note"),
            "source": entry.get("source"),
        }

    if entry.get("skip"):
        return {
            "slug": slug,
            "status": "skipped-frontiere",
            "note": entry.get("note"),
            "source": entry.get("source"),
        }

    data = yaml_load(path)
    before = copy.deepcopy(data)

    new_obediences = entry.get("obediences")
    new_frat = entry.get("fraternite")

    changed = False

    if new_obediences is not None:
        if _norm_obediences(data.get("obediences") or []) != _norm_obediences(new_obediences):
            data["obediences"] = [dict(ob) for ob in new_obediences]
            changed = True

    if new_frat is not None and data.get("fraternite") != new_frat:
        data["fraternite"] = new_frat
        changed = True

    if not changed:
        return None

    data = reorder_dict(data, CANONICAL_TOP_ORDER)

    if not dry_run:
        yaml_dump(data, path)

    return {
        "slug": slug,
        "status": "updated",
        "fraternite": new_frat,
        "obediences": new_obediences,
        "note": entry.get("note"),
        "source": entry.get("source"),
        "before": {
            "obediences": before.get("obediences"),
            "fraternite": before.get("fraternite"),
        },
    }


def apply_consecration_overrides(dry_run: bool) -> Dict[str, Any]:
    """Met à jour manual_overrides.yaml en ajoutant nos arêtes."""
    if OVERRIDES_PATH.exists():
        existing = yaml_load(OVERRIDES_PATH)
    else:
        existing = {}
    existing.setdefault("eveques", {})
    existing.setdefault("consecrations", [])
    existing.setdefault("ordinations", [])

    # Index par clé (consacre, consecrateur_principal) pour idempotence
    by_key: Dict[tuple, dict] = {}
    for item in existing["consecrations"]:
        key = (item.get("consacre"), item.get("consecrateur_principal"))
        by_key[key] = item

    added: List[dict] = []
    updated: List[dict] = []
    for ov in CONSECRATION_OVERRIDES:
        key = (ov["consacre"], ov["consecrateur_principal"])
        new_item = {
            "consacre": ov["consacre"],
            "consecrateur_principal": ov["consecrateur_principal"],
            "date": ov["date"],
            "rite": ov["rite"],
            "rite_source": ov["rite_source"],
            "note": ov["note"],
        }
        if key in by_key:
            if by_key[key] != new_item:
                by_key[key].clear()
                by_key[key].update(new_item)
                updated.append(new_item)
        else:
            existing["consecrations"].append(new_item)
            by_key[key] = new_item
            added.append(new_item)

    if not dry_run and (added or updated):
        # Préserver l'en-tête commenté en réécrivant proprement
        header = (
            "# Annotations manuelles qui supplantent les sources scrapées.\n"
            "# Lu en dernier par tools/clerge_reconcile.py et\n"
            "# tools/clerge_annotate_rite.py.\n"
            "#\n"
            "# Usage typique :\n"
            "# - Forcer le rite d'une consécration Ecclesia Dei avec indult\n"
            "# - Corriger une date contestée entre sources\n"
            "# - Marquer un cas frontière documenté\n"
            "#\n"
            "# Section `consecrations` : pour les sacres dont la date manque\n"
            "# dans le scraping Wikidata (sacres d'Écône, etc.).\n"
            "\n"
        )
        body = yaml.safe_dump(
            existing,
            default_flow_style=False,
            sort_keys=False,
            allow_unicode=True,
        )
        OVERRIDES_PATH.write_text(header + body, encoding="utf-8")

    return {"added": added, "updated": updated}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def run(dry_run: bool = False) -> Dict[str, Any]:
    changes: List[dict] = []
    by_fraternite: Dict[str, int] = {}

    for entry in OBEDIENCE_OVERRIDES:
        log = apply_to_yaml(entry, dry_run=dry_run)
        if log:
            changes.append(log)
            if log["status"] == "updated":
                frat = log.get("fraternite") or "rome"
                by_fraternite[frat] = by_fraternite.get(frat, 0) + 1

    consec_diff = apply_consecration_overrides(dry_run=dry_run)

    summary = {
        "eveques_changes": changes,
        "consec_added": consec_diff["added"],
        "consec_updated": consec_diff["updated"],
        "by_fraternite": by_fraternite,
        "cas_frontiere_count": len(CAS_FRONTIERE),
    }

    if not dry_run:
        CHANGES_PATH.write_text(
            json.dumps(summary, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )
        CAS_FRONTIERE_PATH.write_text(
            json.dumps(CAS_FRONTIERE, ensure_ascii=False, indent=2) + "\n",
            encoding="utf-8",
        )

    return summary


def main(argv: Optional[List[str]] = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--verbose", "-v", action="store_true")
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    summary = run(dry_run=args.dry_run)
    print(json.dumps(
        {
            "by_fraternite": summary["by_fraternite"],
            "cas_frontiere_count": summary["cas_frontiere_count"],
            "consec_added_count": len(summary["consec_added"]),
            "consec_updated_count": len(summary["consec_updated"]),
            "eveques_changed_count": sum(
                1 for c in summary["eveques_changes"] if c["status"] == "updated"
            ),
            "yaml_absent_count": sum(
                1 for c in summary["eveques_changes"] if c["status"] == "yaml-absent"
            ),
        },
        ensure_ascii=False,
        indent=2,
    ))
    return 0


if __name__ == "__main__":
    sys.exit(main())
