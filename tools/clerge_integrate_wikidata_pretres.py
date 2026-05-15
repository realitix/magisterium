"""Intégration des prêtres Wikidata dans le corpus prosopographique.

Lit `clerge/_raw/wikidata_pretres.jsonl` (~34 584 entrées) et produit ou
enrichit `clerge/pretres/{slug}.yaml`, idempotent.

Filtres successifs :

1. **Évêque caché** : positions épiscopales (QIDs réservés) ou label
   épiscopal (évêque/bishop/cardinal/archevêque/archbishop/patriarche/
   patriarch/pope/pape). → log + skip dans
   `clerge/_metadata/wikidata_pretres_eveques_caches.jsonl`.
2. **Cardinal pré-1900 avec ordinateur pape** : si `birth_year < 1900`
   ET tous les `ordinateur_slugs` désignent des papes du corpus, on vide
   `ordinateur_qids` et `ordinateur_slugs` (P1598 désigne la création
   cardinalice, pas l'ordination presbytérale). → log dans
   `clerge/_metadata/wikidata_pretres_cardinal_pape.jsonl`.
3. **Nom manquant** : aucun label fr/en/la → skip + log.

Pour chaque ligne survivante :
- slug stable (ASCII, lowercase, désambiguïsation année si collision)
- créé en mode YAML si nouveau, sinon enrichi (sans écraser les champs
  remplis par d'autres sources).

Idempotent. Re-rejouable sans effet de bord.

Usage::

    uv run python -m tools.clerge_integrate_wikidata_pretres \\
        [--dry-run] [--limit N] [--from-bucket "Qxxx"]
"""

from __future__ import annotations

import argparse
import json
import re
import unicodedata
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

import yaml

ROOT = Path(__file__).resolve().parent.parent
RAW_PATH = ROOT / "clerge" / "_raw" / "wikidata_pretres.jsonl"
EVEQUES_DIR = ROOT / "clerge" / "eveques"
PRETRES_DIR = ROOT / "clerge" / "pretres"
META_DIR = ROOT / "clerge" / "_metadata"

LOG_EVEQUES_CACHES = META_DIR / "wikidata_pretres_eveques_caches.jsonl"
LOG_CARDINAL_PAPE = META_DIR / "wikidata_pretres_cardinal_pape.jsonl"
LOG_SKIPPED = META_DIR / "wikidata_pretres_skipped.jsonl"

# ---------------------------------------------------------------------------
# Filtres
# ---------------------------------------------------------------------------

# QIDs réservés aux fonctions épiscopales (cf. consigne)
EPISCOPAL_QIDS: set[str] = {
    "Q948657",     # évêque titulaire
    "Q611644",     # évêque catholique
    "Q29182",      # archevêque
    "Q49476",      # archevêque (alias)
    "Q50362553",   # évêque catholique (variantes)
    "Q15253909",
    "Q1993358",
    "Q23766552",
}

# Tags pape — détectés aussi pour router vers évêques
POPE_QIDS: set[str] = {"Q19546"}

# Cardinal QIDs — utilisés pour identifier les fiches "cardinal"
CARDINAL_QIDS: set[str] = {"Q45722", "Q2361374"}

EPISCOPAL_LABEL_RE = re.compile(
    r"(évêque|eveque|bishop|cardinal|archevêque|archeveque|archbishop|patriarche|patriarch|pope|pape)",
    re.IGNORECASE,
)


def position_is_episcopal(pos: dict[str, Any]) -> bool:
    qid = pos.get("position_qid")
    if qid and (qid in EPISCOPAL_QIDS or qid in POPE_QIDS):
        return True
    for lkey in ("label_fr", "label_en"):
        lbl = pos.get(lkey)
        if isinstance(lbl, str) and EPISCOPAL_LABEL_RE.search(lbl):
            return True
    return False


def has_episcopal_position(entry: dict[str, Any]) -> bool:
    positions = entry.get("positions") or []
    return any(position_is_episcopal(p) for p in positions if isinstance(p, dict))


# ---------------------------------------------------------------------------
# Slug + normalisation
# ---------------------------------------------------------------------------

_SLUG_KEEP = re.compile(r"[^a-z0-9]+")


def ascii_fold(s: str) -> str:
    s = unicodedata.normalize("NFKD", s)
    return "".join(c for c in s if not unicodedata.combining(c))


def slugify(s: str) -> str:
    s = ascii_fold(s).lower()
    s = _SLUG_KEEP.sub("-", s)
    return s.strip("-")


def pick_name(names: dict[str, str] | None) -> str | None:
    if not names:
        return None
    for lang in ("fr", "en", "la", "it", "es", "de"):
        v = names.get(lang)
        if v:
            return v.strip() or None
    for v in names.values():
        if v:
            return v.strip() or None
    return None


_DATE_RE = re.compile(r"^(-?\d{1,4})(?:-(\d{2}))?(?:-(\d{2}))?")


def parse_year(s: str | None) -> int | None:
    if not s:
        return None
    m = _DATE_RE.match(str(s).strip())
    if not m:
        return None
    try:
        return int(m.group(1))
    except (TypeError, ValueError):
        return None


def normalize_date(value: Any) -> str | None:
    """Retourne 'YYYY-MM-DD' ou 'YYYY' ou None — sans inventer de précision."""
    if not value:
        return None
    s = str(value).strip()
    m = _DATE_RE.match(s)
    if not m:
        return None
    year = m.group(1)
    mo = m.group(2)
    da = m.group(3)
    # Strip leading 0 du siècle pour cohérence YAML
    try:
        y = int(year)
    except ValueError:
        return None
    if mo and da:
        return f"{y:04d}-{mo}-{da}"
    if mo:
        return f"{y:04d}-{mo}"
    return f"{y:04d}"


# ---------------------------------------------------------------------------
# Index des évêques (pour résoudre slugs des ordinateurs et identifier les papes)
# ---------------------------------------------------------------------------


def load_eveque_index() -> tuple[set[str], set[str]]:
    """Retourne (tous_slugs, slugs_papes)."""
    all_slugs: set[str] = set()
    pope_slugs: set[str] = set()
    for p in EVEQUES_DIR.glob("*.yaml"):
        slug = p.stem
        all_slugs.add(slug)
        try:
            text = p.read_text(encoding="utf-8")
        except Exception:
            continue
        # Lecture rapide : pas besoin de parser tout le YAML
        if re.search(r"^rang:\s*pape\b", text, re.MULTILINE):
            pope_slugs.add(slug)
    return all_slugs, pope_slugs


# ---------------------------------------------------------------------------
# Construction de la fiche
# ---------------------------------------------------------------------------


def build_slug(name: str, birth_year: int | None, taken: dict[str, int]) -> str:
    base = slugify(name)
    if not base:
        base = "anonymous"
    candidate = base
    if candidate not in taken:
        taken[candidate] = 1
        return candidate
    # Désambiguïsation par année de naissance
    if birth_year is not None:
        candidate2 = f"{base}-{birth_year}"
        if candidate2 not in taken:
            taken[candidate2] = 1
            return candidate2
    # Sinon suffixe -2, -3, …
    n = taken[candidate] + 1
    while True:
        candidate_n = f"{base}-{n}"
        if candidate_n not in taken:
            taken[base] = n  # bump counter du base
            taken[candidate_n] = 1
            return candidate_n
        n += 1


def collect_existing_slugs() -> set[str]:
    return {p.stem for p in PRETRES_DIR.glob("*.yaml")}


def build_fonctions(positions: Iterable[dict[str, Any]]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for pos in positions:
        if not isinstance(pos, dict):
            continue
        titre = pos.get("label_fr") or pos.get("label_en")
        if not titre:
            continue
        d = {
            "titre": titre,
            "du": normalize_date(pos.get("start")),
            "au": normalize_date(pos.get("end")),
        }
        out.append(d)
    return out


def build_photo(image_url: str | None) -> dict[str, Any] | None:
    if not image_url:
        return None
    return {
        "fichier": None,
        "source": "wikimedia-commons",
        "source_url": image_url,
        "licence": "unknown",
    }


def get_ordinateur_text(entry: dict[str, Any]) -> str | None:
    """Si on a ordinateur_slugs, prend le premier ; sinon None.

    Wikidata ne fournit pas de label, on utilise le slug comme proxy texte
    qui sera ré-résolu (1:1) par clerge_finalize_pretres."""
    slugs = entry.get("ordinateur_slugs") or []
    if isinstance(slugs, list) and slugs:
        return slugs[0]
    return None


def get_ordinateur_slug(entry: dict[str, Any], known_slugs: set[str]) -> str | None:
    slugs = entry.get("ordinateur_slugs") or []
    if isinstance(slugs, list):
        for s in slugs:
            if isinstance(s, str) and s in known_slugs:
                return s
    return None


def build_new_yaml(
    slug: str,
    nom: str,
    entry: dict[str, Any],
    eveques_slugs: set[str],
    cleared_ordinateur: bool,
) -> dict[str, Any]:
    fonctions = build_fonctions(entry.get("positions") or [])
    photo = build_photo(entry.get("image_url"))
    image_url = entry.get("image_url")

    if cleared_ordinateur:
        ord_text = None
        ord_slug = None
    else:
        ord_slug = get_ordinateur_slug(entry, eveques_slugs)
        ord_text = ord_slug  # texte = slug du pape connu (si dispo)
    ordination = {
        "date": normalize_date(entry.get("ordination_date")),
        "ordinateur": ord_text,
        "ordinateur_slug": ord_slug,
        "source_urls": [],
    }
    if entry.get("wikipedia_fr_url"):
        ordination["source_urls"].append(entry["wikipedia_fr_url"])

    doc: dict[str, Any] = OrderedDict()
    doc["slug"] = slug
    doc["nom"] = nom
    # nom_complet : si on a un autre label plus long, on l'utilise
    names = entry.get("names") or {}
    longest = max(
        (v for v in names.values() if isinstance(v, str)), key=lambda s: len(s), default=None
    )
    if longest and longest != nom and len(longest) > len(nom):
        doc["nom_complet"] = longest
    doc["naissance"] = normalize_date(entry.get("birth_date"))
    doc["deces"] = normalize_date(entry.get("death_date"))
    doc["rang"] = "pretre"
    doc["fraternite"] = None
    doc["ordination"] = ordination
    if fonctions:
        doc["fonctions"] = fonctions
    if photo:
        doc["photo"] = photo
    doc["qids"] = {"wikidata": entry.get("source_id")}
    doc["sources"] = [
        {
            "source": "wikidata-pretres",
            "fetched_at": entry.get("fetched_at") or datetime.now(timezone.utc).isoformat(),
            "source_id": entry.get("source_id"),
        }
    ]
    notes = "Importé depuis Wikidata. Ordination presbytérale non documentée"
    if not ord_slug:
        notes += " — l'évêque ordinateur est inconnu."
    else:
        notes += "."
    doc["notes"] = notes
    return doc


def enrich_existing_yaml(
    path: Path, entry: dict[str, Any], eveques_slugs: set[str], cleared_ordinateur: bool
) -> bool:
    """Met à jour la fiche existante en ne touchant pas aux champs déjà remplis.

    Retourne True si modifié.
    """
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(data, dict):
        return False

    changed = False

    # QID Wikidata
    qids = data.get("qids") or {}
    if not isinstance(qids, dict):
        qids = {}
    if not qids.get("wikidata") and entry.get("source_id"):
        qids["wikidata"] = entry["source_id"]
        data["qids"] = qids
        changed = True

    # Photo si manquante
    photo = data.get("photo")
    has_photo_already = (
        isinstance(photo, dict) and (photo.get("fichier") or photo.get("source_url"))
    )
    if not has_photo_already and entry.get("image_url"):
        data["photo"] = build_photo(entry["image_url"])
        changed = True

    # Dates si manquantes (on ne remplace JAMAIS)
    for fld, raw_key in (("naissance", "birth_date"), ("deces", "death_date")):
        if not data.get(fld):
            v = normalize_date(entry.get(raw_key))
            if v:
                data[fld] = v
                changed = True

    # Ordination — n'ajoute que si vide
    ordination = data.get("ordination") or {}
    if not isinstance(ordination, dict):
        ordination = {}
    if not ordination.get("ordinateur") and not cleared_ordinateur:
        ord_slug = get_ordinateur_slug(entry, eveques_slugs)
        if ord_slug:
            ordination["ordinateur"] = ord_slug
            ordination.setdefault("ordinateur_slug", ord_slug)
            data["ordination"] = ordination
            changed = True
    if not ordination.get("date"):
        d = normalize_date(entry.get("ordination_date"))
        if d:
            ordination["date"] = d
            data["ordination"] = ordination
            changed = True

    # Source — append si pas déjà tracé
    sources = data.get("sources") or []
    if isinstance(sources, list):
        already = any(
            isinstance(s, dict) and s.get("source") == "wikidata-pretres"
            and s.get("source_id") == entry.get("source_id")
            for s in sources
        )
        if not already:
            sources.append(
                {
                    "source": "wikidata-pretres",
                    "fetched_at": entry.get("fetched_at")
                    or datetime.now(timezone.utc).isoformat(),
                    "source_id": entry.get("source_id"),
                }
            )
            data["sources"] = sources
            changed = True

    if changed:
        path.write_text(
            yaml.safe_dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False),
            encoding="utf-8",
        )
    return changed


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def iter_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    with path.open(encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            try:
                yield json.loads(line)
            except json.JSONDecodeError:
                continue


def write_log(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        # On vide quand même le fichier pour que le rerun soit propre
        if path.exists():
            path.unlink()
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        "\n".join(json.dumps(r, ensure_ascii=False) for r in rows) + "\n",
        encoding="utf-8",
    )


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument(
        "--from-bucket",
        type=str,
        default=None,
        help="QID de départ — saute les lignes jusqu'à trouver ce source_id.",
    )
    args = parser.parse_args(argv)

    PRETRES_DIR.mkdir(parents=True, exist_ok=True)
    META_DIR.mkdir(parents=True, exist_ok=True)

    print("[integrate-pretres] indexation évêques (pour résolution slugs)…")
    eveques_slugs, pope_slugs = load_eveque_index()
    print(f"[integrate-pretres] {len(eveques_slugs):,} évêques, {len(pope_slugs):,} papes")

    existing_pretres = collect_existing_slugs()
    print(f"[integrate-pretres] {len(existing_pretres):,} fiches prêtres déjà présentes")

    # Compteur pour désambiguïsation : on track les slugs déjà attribués
    taken: dict[str, int] = {s: 1 for s in existing_pretres}

    eveques_caches: list[dict[str, Any]] = []
    cardinal_pape_logs: list[dict[str, Any]] = []
    skipped_logs: list[dict[str, Any]] = []

    n_seen = 0
    n_created = 0
    n_enriched = 0
    n_unchanged = 0
    n_skipped_eveque = 0
    n_skipped_card_pape_cleared = 0
    n_skipped_name = 0

    started = args.from_bucket is None
    for entry in iter_jsonl(RAW_PATH):
        if not started:
            if entry.get("source_id") == args.from_bucket:
                started = True
            else:
                continue

        n_seen += 1
        if args.limit is not None and n_seen > args.limit:
            break

        source_id = entry.get("source_id")

        # Filtre 1 : évêque caché
        if has_episcopal_position(entry):
            n_skipped_eveque += 1
            eveques_caches.append({
                "source_id": source_id,
                "names": entry.get("names"),
                "positions": entry.get("positions"),
            })
            continue

        # Filtre 3 : nom manquant
        nom = pick_name(entry.get("names"))
        if not nom:
            n_skipped_name += 1
            skipped_logs.append({
                "source_id": source_id,
                "reason": "no_name",
                "entry": entry,
            })
            continue

        # Filtre 2 : cardinal pré-1900 avec ordinateur pape → on vide ordinateur
        cleared_ordinateur = False
        birth_year = parse_year(entry.get("birth_date"))
        ord_slugs = entry.get("ordinateur_slugs") or []
        if (
            birth_year is not None
            and birth_year < 1900
            and isinstance(ord_slugs, list)
            and ord_slugs
        ):
            # Sont-ils tous des papes ?
            all_papes = all(
                isinstance(s, str)
                and (s.startswith("papa-") or s in pope_slugs)
                for s in ord_slugs
            )
            if all_papes:
                cleared_ordinateur = True
                n_skipped_card_pape_cleared += 1
                cardinal_pape_logs.append({
                    "source_id": source_id,
                    "name": nom,
                    "birth_year": birth_year,
                    "ordinateur_slugs": ord_slugs,
                })

        # Détermination slug
        # Pour les fiches existantes (par QID), on tente d'abord le match QID
        # via le slug calculé direct ; sinon création.
        candidate_slug = slugify(nom)
        if birth_year is not None:
            candidate_with_year = f"{candidate_slug}-{birth_year}"
        else:
            candidate_with_year = None

        # Cas 1 : un YAML existant a déjà ce QID Wikidata → enrichir
        # (on évite la collision de slug pour un même clerc)
        target_path = None
        for cand in [candidate_slug, candidate_with_year]:
            if not cand:
                continue
            p = PRETRES_DIR / f"{cand}.yaml"
            if p.exists():
                try:
                    data = yaml.safe_load(p.read_text(encoding="utf-8"))
                    if isinstance(data, dict):
                        existing_qid = (data.get("qids") or {}).get("wikidata")
                        if existing_qid == source_id or existing_qid is None:
                            target_path = p
                            break
                except Exception:
                    pass

        if target_path is not None and target_path.exists():
            # Enrichissement
            if args.dry_run:
                n_enriched += 1
            else:
                if enrich_existing_yaml(target_path, entry, eveques_slugs, cleared_ordinateur):
                    n_enriched += 1
                else:
                    n_unchanged += 1
        else:
            # Création — slug nouveau (avec désambiguïsation)
            slug = build_slug(nom, birth_year, taken)
            doc = build_new_yaml(slug, nom, entry, eveques_slugs, cleared_ordinateur)
            if not args.dry_run:
                out_path = PRETRES_DIR / f"{slug}.yaml"
                out_path.write_text(
                    yaml.safe_dump(
                        dict(doc), allow_unicode=True, sort_keys=False, default_flow_style=False
                    ),
                    encoding="utf-8",
                )
            n_created += 1

        if n_seen % 5000 == 0:
            print(
                f"[integrate-pretres] {n_seen:,} lus | {n_created:,} créés | "
                f"{n_enriched:,} enrichis | {n_skipped_eveque:,} évêques cachés"
            )

    print("─" * 60)
    print(f"[integrate-pretres] Lus     : {n_seen:,}")
    print(f"[integrate-pretres] Créés   : {n_created:,}")
    print(f"[integrate-pretres] Enrichis: {n_enriched:,}")
    print(f"[integrate-pretres] Inchangés: {n_unchanged:,}")
    print(f"[integrate-pretres] Skip évêque caché          : {n_skipped_eveque:,}")
    print(f"[integrate-pretres] Cardinal-pape (ordinateur vidé): {n_skipped_card_pape_cleared:,}")
    print(f"[integrate-pretres] Skip nom manquant          : {n_skipped_name:,}")

    if not args.dry_run:
        write_log(LOG_EVEQUES_CACHES, eveques_caches)
        write_log(LOG_CARDINAL_PAPE, cardinal_pape_logs)
        write_log(LOG_SKIPPED, skipped_logs)
        print(f"[integrate-pretres] logs écrits dans {META_DIR}/wikidata_pretres_*.jsonl")

    final = len(list(PRETRES_DIR.glob("*.yaml")))
    print(f"[integrate-pretres] volume final clerge/pretres/ : {final:,} fiches")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
