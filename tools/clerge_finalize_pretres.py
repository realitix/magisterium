"""Finalisation du corpus des prêtres.

Étapes :

1. Lit toutes les fiches `clerge/pretres/*.yaml`.
2. Résout chaque `ordination.ordinateur` (texte) en slug d'évêque du corpus
   via match exact puis fuzzy match (seuil 0.9) sur nom/nom_complet.
3. Calcule pour chaque prêtre son tampon : tampon du prêtre = tampon de
   l'ordinateur (heuristique simplifiée, le rite presbytéral est noté mais
   ne change pas le verdict).
4. Met à jour chaque YAML prêtre avec `ordinateur_slug` résolu et `tampon`.
5. Génère `clerge/_metadata/ordinations.jsonl`.
6. Ajoute les prêtres à `clerge/_metadata/clerics.jsonl` (idempotent : remplace
   uniquement les entrées avec `rang in {pretre, diacre}`).
7. Stats finales dans `clerge/_metadata/stats.json`.

Idempotent. Doit tourner après `clerge_reconcile.py` et `clerge_annotate_rite.py`.

Usage::

    uv run python -m tools.clerge_finalize_pretres [--dry-run]
"""
from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import OrderedDict
from difflib import SequenceMatcher
from pathlib import Path
from typing import Any

import yaml

ROOT = Path(__file__).resolve().parent.parent
META_DIR = ROOT / "clerge" / "_metadata"
EVEQUES_DIR = ROOT / "clerge" / "eveques"
PRETRES_DIR = ROOT / "clerge" / "pretres"

CLERICS_PATH = META_DIR / "clerics.jsonl"
ORDINATIONS_PATH = META_DIR / "ordinations.jsonl"
LINEAGES_PATH = META_DIR / "lineages.json"
STATS_PATH = META_DIR / "stats.json"
HONORIFICS = re.compile(r"^(mgr|monseigneur|abbé|abbe|pere|père|fr\.|don|don\.|s\.|saint|rev\.|reverend|révérend|cardinal|don)\s+", re.I)


def ascii_fold(s: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFKD", s) if not unicodedata.combining(c))


def normalize_name(s: str) -> str:
    if not s:
        return ""
    s = HONORIFICS.sub("", s.strip())
    s = ascii_fold(s).lower()
    s = re.sub(r"[^a-z0-9\s-]", " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return s


def parse_year(value: Any) -> int | None:
    if not value:
        return None
    s = str(value).strip()
    m = re.search(r"\d{3,4}", s)
    return int(m.group()) if m else None


def year_of(value: Any) -> int | None:
    return parse_year(value)


def load_eveque_index() -> tuple[dict[str, dict], dict[str, list[str]]]:
    """Retourne (slug → fiche, nom_normalisé → [slug, ...])."""
    by_slug: dict[str, dict] = {}
    by_name: dict[str, list[str]] = {}
    for p in sorted(EVEQUES_DIR.glob("*.yaml")):
        try:
            data = yaml.safe_load(p.read_text(encoding="utf-8"))
        except Exception:
            continue
        if not isinstance(data, dict) or "slug" not in data:
            continue
        slug = data["slug"]
        by_slug[slug] = data
        for name_field in ("nom", "nom_complet"):
            name = data.get(name_field)
            if name:
                key = normalize_name(name)
                if key:
                    by_name.setdefault(key, []).append(slug)
    return by_slug, by_name


def fuzzy_resolve(
    text: str, by_name: dict[str, list[str]], by_slug: dict[str, dict], birth_year: int | None
) -> tuple[str | None, float]:
    """Retourne (slug, confidence) ou (None, 0)."""
    key = normalize_name(text)
    if not key:
        return None, 0.0

    # 1. Exact match
    if key in by_name:
        slugs = by_name[key]
        if len(slugs) == 1:
            return slugs[0], 1.0
        # Désambiguïsation par dates si plusieurs
        if birth_year is not None:
            for s in slugs:
                bi = year_of(by_slug[s].get("naissance"))
                if bi is not None and abs(bi - birth_year) <= 5:
                    return s, 0.95
        # Sinon prend le premier (déterministe par tri)
        return sorted(slugs)[0], 0.85

    # 2. Fuzzy match seuil 0.9
    best_slug, best_score = None, 0.0
    for nname, slugs in by_name.items():
        score = SequenceMatcher(None, key, nname).ratio()
        if score > best_score:
            best_score = score
            best_slug = slugs[0]
    if best_score >= 0.9:
        return best_slug, best_score
    return None, best_score


def compute_tampon_for_pretre(
    ordinateur_slug: str | None, lineages: dict, fraternite: str | None
) -> tuple[str, str]:
    """tampon prêtre = tampon de son ordinateur (règle simplifiée).

    Retourne (tampon, source) où source ∈ {ordinateur, fraternite, defaut}.

    - Si ordinateur résolu : tampon de l'ordinateur via lineages.json.
    - Sinon, fallback par fraternité : un prêtre d'une fraternité tradi/sédé
      a forcément été ordonné par un évêque de cette mouvance, dont le tampon
      est ordo_validus dans le corpus.
    - Sinon ordo_incertus.
    """
    if ordinateur_slug:
        line = lineages.get(ordinateur_slug)
        if line and "tampon" in line:
            return line["tampon"], "ordinateur"

    # Fallback par fraternité — défensif et cohérent avec le corpus
    if fraternite:
        f = fraternite.lower()
        if f in {
            "fsspx", "fsspx-fondateur", "fsspx-allie",
            "resistance",
            "fssp", "icrsp", "icr", "ibp",
            "cmri", "cmri-allie", "sgg-school", "sede-allie",
            "sede", "sede-thuc-line", "sede-cassiciacum",
            "palmar",
        }:
            return "ordo_validus", "fraternite"

    return "ordo_incertus", "defaut"


def load_lineages() -> dict:
    if not LINEAGES_PATH.exists():
        return {}
    raw = json.loads(LINEAGES_PATH.read_text(encoding="utf-8"))
    if isinstance(raw, dict):
        return raw
    # Liste -> dict
    return {item["slug"]: item for item in raw if "slug" in item}


def annee_to_int(v: Any) -> int | None:
    return parse_year(v)


def process_pretre(
    pretre_path: Path,
    by_name: dict[str, list[str]],
    by_slug: dict[str, dict],
    lineages: dict,
    write: bool = True,
) -> dict | None:
    """Met à jour le YAML du prêtre et renvoie un dict pour clerics.jsonl + ordinations.jsonl."""
    try:
        data = yaml.safe_load(pretre_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict) or "slug" not in data:
        return None

    slug = data["slug"]
    ordination = data.get("ordination") or {}
    birth_year = year_of(data.get("naissance"))

    # Résolution de l'ordinateur
    raw_ord = ordination.get("ordinateur")
    ord_slug: str | None = None
    ord_confidence: float = 0.0
    if isinstance(raw_ord, str) and raw_ord.strip():
        # Cas 1 : déjà un slug (existe dans by_slug)
        if raw_ord in by_slug:
            ord_slug = raw_ord
            ord_confidence = 1.0
        else:
            ord_slug, ord_confidence = fuzzy_resolve(raw_ord, by_name, by_slug, birth_year)

    # Tampon — règle hiérarchique : ordinateur résolu > fraternité > défaut
    fraternite = data.get("fraternite") if isinstance(data.get("fraternite"), str) else None
    tampon, tampon_source = compute_tampon_for_pretre(ord_slug, lineages, fraternite)

    # Mise à jour idempotente du YAML
    changed = False
    if ord_slug and ordination.get("ordinateur_slug") != ord_slug:
        ordination["ordinateur_slug"] = ord_slug
        changed = True
    if data.get("tampon") != tampon:
        data["tampon"] = tampon
        changed = True
    if data.get("tampon_source") != tampon_source:
        data["tampon_source"] = tampon_source
        changed = True
    if ordination is not data.get("ordination"):
        data["ordination"] = ordination
    if write and changed:
        # Réécrit avec ordre raisonnable
        ordered = OrderedDict()
        for k in ("slug", "nom", "nom_complet", "naissance", "deces", "rang", "tampon"):
            if k in data:
                ordered[k] = data[k]
        for k, v in data.items():
            if k not in ordered:
                ordered[k] = v
        pretre_path.write_text(
            yaml.safe_dump(
                dict(ordered),
                allow_unicode=True,
                sort_keys=False,
                default_flow_style=False,
            ),
            encoding="utf-8",
        )

    # Entrée clerics.jsonl (index plat de recherche)
    bi = year_of(data.get("naissance"))
    de = year_of(data.get("deces"))
    photo = data.get("photo") or {}
    has_photo = bool(photo.get("fichier")) if isinstance(photo, dict) else False
    pays = None
    nat = data.get("nationalite")
    if isinstance(nat, str) and re.fullmatch(r"[A-Z]{2}", nat):
        pays = nat
    cleric_row = {
        "slug": slug,
        "nom": data.get("nom") or slug,
        "naissance_annee": bi,
        "deces_annee": de,
        "fraternite": data.get("fraternite"),
        "rang": data.get("rang", "pretre"),
        "pays": pays,
        "photo_disponible": has_photo,
        "wikidata_qid": (data.get("qids") or {}).get("wikidata"),
    }

    # Entrée ordinations.jsonl si on a un ordinateur résolu
    ordination_row = None
    if ord_slug:
        ordination_row = {
            "ordonne": slug,
            "ordinateur": ord_slug,
            "date": ordination.get("date"),
            "lieu": ordination.get("lieu"),
            "rite": ordination.get("rite"),
            "rite_source": ordination.get("rite_source"),
            "sources": ordination.get("source_urls", []),
            "resolution_confidence": round(ord_confidence, 2),
        }

    return {"cleric": cleric_row, "ordination": ordination_row, "ord_slug": ord_slug}


def merge_into_clerics(rows: list[dict]) -> tuple[int, int]:
    """Ajoute les prêtres à clerics.jsonl, remplaçant les entrées prêtres existantes."""
    existing: list[dict] = []
    if CLERICS_PATH.exists():
        for line in CLERICS_PATH.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except Exception:
                continue
            # On garde tout SAUF les anciennes entrées prêtre/diacre (qu'on régénère)
            if d.get("rang") not in {"pretre", "diacre"}:
                existing.append(d)
    before = len(existing)
    by_slug = {r["slug"]: r for r in rows}
    merged = existing + list(by_slug.values())
    merged.sort(key=lambda r: r["slug"])
    CLERICS_PATH.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in merged) + "\n",
        encoding="utf-8",
    )
    return before, len(merged) - before


def write_ordinations(rows: list[dict]) -> int:
    rows = sorted(rows, key=lambda r: (r["ordonne"], r["ordinateur"]))
    ORDINATIONS_PATH.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n",
        encoding="utf-8",
    )
    return len(rows)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    print(f"[finalize-pretres] lecture des évêques pour résolution des ordinateurs…")
    by_slug, by_name = load_eveque_index()
    print(f"[finalize-pretres] {len(by_slug):,} évêques indexés, {len(by_name):,} noms uniques")

    print("[finalize-pretres] lecture lineages.json…")
    lineages = load_lineages()
    print(f"[finalize-pretres] {len(lineages):,} lignées chargées")

    pretres_files = sorted(PRETRES_DIR.glob("*.yaml"))
    print(f"[finalize-pretres] {len(pretres_files):,} prêtres à traiter")

    cleric_rows: list[dict] = []
    ordination_rows: list[dict] = []
    resolved = 0
    unresolved_pretres: list[str] = []
    tampons: dict[str, int] = {"ordo_validus": 0, "ordo_dubius": 0, "ordo_incertus": 0}

    for p in pretres_files:
        result = process_pretre(p, by_name, by_slug, lineages, write=not args.dry_run)
        if result is None:
            continue
        cleric_rows.append(result["cleric"])
        # Tally du tampon (relu depuis le YAML mis à jour)
        try:
            data = yaml.safe_load(p.read_text(encoding="utf-8"))
            t = data.get("tampon", "ordo_incertus")
            tampons[t] = tampons.get(t, 0) + 1
        except Exception:
            tampons["ordo_incertus"] += 1
        if result["ordination"]:
            ordination_rows.append(result["ordination"])
            resolved += 1
        elif result["ord_slug"] is None:
            unresolved_pretres.append(p.stem)

    print(f"[finalize-pretres] {resolved:,} ordinations résolues, {len(unresolved_pretres):,} non-résolues")
    print(f"[finalize-pretres] tampons prêtres : {tampons}")

    if args.dry_run:
        print("[finalize-pretres] --dry-run : pas d'écriture")
        return 0

    before, added = merge_into_clerics(cleric_rows)
    print(f"[finalize-pretres] clerics.jsonl : {before:,} évêques préservés + {added:,} prêtres")

    n_ord = write_ordinations(ordination_rows)
    print(f"[finalize-pretres] ordinations.jsonl : {n_ord:,} arêtes")

    # Mise à jour stats
    stats = {}
    if STATS_PATH.exists():
        try:
            stats = json.loads(STATS_PATH.read_text(encoding="utf-8"))
        except Exception:
            stats = {}
    stats["pretres_total"] = len(cleric_rows)
    stats["pretres_ordination_resolue"] = resolved
    stats["pretres_par_tampon"] = tampons
    STATS_PATH.write_text(
        json.dumps(stats, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )
    print(f"[finalize-pretres] stats.json mis à jour")

    if unresolved_pretres:
        unr_path = META_DIR / "pretres_ordinateurs_unresolved.json"
        unr_path.write_text(
            json.dumps(unresolved_pretres, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        print(f"[finalize-pretres] non-résolus listés dans {unr_path}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
