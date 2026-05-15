"""Lie les documents du corpus magistériel/livres aux clercs prosopographiques.

Stratégie de résolution (priorité décroissante) :

  1. **Path-based** — le chemin contient `papes/{YYYY}-{slug}/...` ou
     `D-fsspx/mgr-lefebvre/...` : le slug du clerc se déduit directement du
     dossier. C'est la voie la plus fiable (conf. ≥ 0.98).

  2. **Auteur exact** — `auteur` du `.meta.yaml`, après normalisation
     (suppression de préfixes « Mgr », « S. », « Saint », « Don »…),
     correspond exactement au `nom` d'un clerc indexé. On désambiguïse via
     la date du document (doit tomber entre naissance et déces du clerc,
     +/- 10 ans de tolérance).

  3. **Fuzzy match** — difflib.SequenceMatcher seuil 0.92 sur les candidats
     compatibles temporellement. Utilisé pour rattraper les variantes
     orthographiques (accents, transcriptions).

Auteurs collectifs (conciles, congrégations, dicastères, conférences,
fraternités, collections type « Divers », auteurs séparés par « / » ou
« + ») : on les inscrit dans le log avec `cleric_slug=null` et
`method=collective` mais ils ne génèrent pas de mapping consommable.

Conservateur : si la confiance est < 0.9, on skip et on logue.

Sorties :
  * `clerge/_metadata/document_authors.jsonl` — 1 ligne par doc résolu.
  * `clerge/_metadata/cleric_documents.json`  — index inverse par clerc.
  * `clerge/_metadata/document_authors_unresolved.jsonl` — log des cas
    non résolus (auteur collectif ou pas de match suffisant).
"""

from __future__ import annotations

import json
import re
import sys
import unicodedata
from collections import defaultdict
from dataclasses import dataclass
from difflib import SequenceMatcher
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
MAGISTERIUM_INDEX = REPO_ROOT / "magisterium" / "_metadata" / "index.jsonl"
LIVRES_INDEX = REPO_ROOT / "livres" / "_metadata" / "index.jsonl"
CLERGE_DIR = REPO_ROOT / "clerge"
CLERICS_JSONL = CLERGE_DIR / "_metadata" / "clerics.jsonl"

OUT_MAPPING = CLERGE_DIR / "_metadata" / "document_authors.jsonl"
OUT_INVERSE = CLERGE_DIR / "_metadata" / "cleric_documents.json"
OUT_UNRESOLVED = CLERGE_DIR / "_metadata" / "document_authors_unresolved.jsonl"

MIN_CONFIDENCE = 0.9
FUZZY_THRESHOLD = 0.92


# ─────────────────────────── Heuristiques auteurs ───────────────────────────


# Préfixes/honorifiques à stripper en tête de chaîne avant comparaison.
HONORIFICS = (
    "mgr ",
    "mgr. ",
    "sa sainteté ",
    "saint ",
    "s. ",
    "ste ",
    "sainte ",
    "don ",
    "pape ",
    "le pape ",
    "card. ",
    "cardinal ",
    "card ",
)

# Indices qu'un auteur est collectif (institution, concile, anthologie).
# On match en lower-case, insensible aux accents.
COLLECTIVE_MARKERS = (
    "concile",
    "congrégation",
    "congregation",
    "conférence",
    "conference",
    "dicastère",
    "dicastere",
    "fraternité saint-pie",
    "fraternite saint-pie",
    "saint-office",
    "sacrée congrégation",
    "sacrée congregation",
    "sacree congregation",
    "sacra congregatio",
    "cardinaux",
    "divers",
    "cirs",
    "collection",
    "comité",
    "comite",
)

# Tokens de séparation qui révèlent un auteur multi-pontifical (« Paul III / Jules III / Pie IV »).
MULTI_AUTHOR_SEPARATORS = (" / ", "+", " & ", " et ")


@dataclass
class Cleric:
    slug: str
    nom: str
    naissance: int | None
    deces: int | None


def strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn"
    )


def normalize(s: str) -> str:
    """Lowercase, accents-strippés, espaces normalisés, honorifiques retirés."""
    out = strip_accents(s).lower().strip()
    out = re.sub(r"\s+", " ", out)
    changed = True
    while changed:
        changed = False
        for h in HONORIFICS:
            if out.startswith(h):
                out = out[len(h):]
                changed = True
    return out.strip()


def is_collective(auteur: str) -> bool:
    n = normalize(auteur)
    if any(sep.strip() in auteur for sep in MULTI_AUTHOR_SEPARATORS if sep.strip()):
        # « X / Y / Z » → collectif
        return True
    for marker in COLLECTIVE_MARKERS:
        if marker in n:
            return True
    return False


# ─────────────────────── Path-based : pope/Lefebvre/etc. ────────────────────


# Dossier `papes/YYYY-slug/` → slug du clerc. Les dossiers `papes/` listés sous
# A-pre-vatican-ii et C-post-vatican-ii utilisent tous la convention
# « YYYY-{regnal-slug} ». On en extrait directement.
PAPE_PATH_RE = re.compile(r"(?:^|/)papes/\d{4}-([a-z0-9-]+?)(?:/|$)")
LEFEBVRE_PATH_RE = re.compile(r"(?:^|/)D-fsspx/mgr-lefebvre(?:/|$)")


def path_based_slug(path: str) -> tuple[str, str] | None:
    """Retourne (cleric_slug, raison) si le chemin l'identifie sans ambiguïté."""
    m = PAPE_PATH_RE.search(path)
    if m is not None:
        return m.group(1), "path_papes"
    if LEFEBVRE_PATH_RE.search(path):
        return "marcel-lefebvre", "path_mgr_lefebvre"
    return None


# ─────────────────────────── Chargement clergé ──────────────────────────────


def load_clerics() -> tuple[dict[str, Cleric], dict[str, list[Cleric]]]:
    """Retourne (by_slug, by_normalized_name).

    `by_normalized_name` map nom_normalisé → liste des clercs portant ce nom
    (homonymes possibles, désambiguïsés par dates).
    """
    by_slug: dict[str, Cleric] = {}
    by_name: dict[str, list[Cleric]] = defaultdict(list)
    with CLERICS_JSONL.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            row = json.loads(line)
            c = Cleric(
                slug=row["slug"],
                nom=row["nom"],
                naissance=row.get("naissance_annee"),
                deces=row.get("deces_annee"),
            )
            by_slug[c.slug] = c
            by_name[normalize(c.nom)].append(c)
    return by_slug, dict(by_name)


# ─────────────────────────── Désambiguïsation ───────────────────────────────


def doc_year(date: str | None) -> int | None:
    if date is None:
        return None
    m = re.match(r"(\d{1,4})", date)
    if m is None:
        return None
    return int(m.group(1))


def temporal_score(cleric: Cleric, year: int | None) -> float:
    """Score 0..1 : compatible si l'année tombe dans [naissance-5, deces+5]."""
    if year is None:
        return 0.5  # neutre : on ne peut pas désambiguïser, on garde
    if cleric.naissance is None and cleric.deces is None:
        return 0.5
    lo = (cleric.naissance or year) - 5
    hi = (cleric.deces or year) + 5
    if lo <= year <= hi:
        return 1.0
    return 0.0


def pick_temporal(
    candidates: list[Cleric], year: int | None
) -> tuple[Cleric, float] | None:
    """Sélectionne le clerc temporellement le plus compatible, ou None."""
    scored = [(c, temporal_score(c, year)) for c in candidates]
    scored.sort(key=lambda t: t[1], reverse=True)
    if not scored:
        return None
    best = scored[0]
    if best[1] == 0.0:
        return None
    # Si plusieurs candidats sont parfaitement compatibles, on est ambigu.
    perfects = [s for s in scored if s[1] == best[1]]
    if len(perfects) > 1 and best[1] == 1.0:
        return None
    return best


# ─────────────────────────── Résolution principale ──────────────────────────


@dataclass
class Resolution:
    cleric_slug: str | None
    confidence: float
    method: str
    note: str | None = None


def resolve_author(
    doc: dict,
    by_slug: dict[str, Cleric],
    by_name: dict[str, list[Cleric]],
) -> Resolution:
    path = doc.get("path") or ""
    auteur = doc.get("auteur") or ""
    year = doc_year(doc.get("date"))

    # 1) Path-based
    pb = path_based_slug(path)
    if pb is not None:
        slug, reason = pb
        if slug in by_slug:
            return Resolution(slug, 0.98, reason)
        # Path indique un clerc qu'on n'a pas dans le corpus prosopographique.
        return Resolution(
            None, 0.0, "unresolved_path",
            note=f"Path pointe vers {slug!r} mais aucun clerc indexé sous ce slug",
        )

    if not auteur:
        return Resolution(None, 0.0, "no_author")

    # 2) Auteur collectif ?
    if is_collective(auteur):
        return Resolution(None, 0.0, "collective", note=auteur)

    norm = normalize(auteur)

    # 3) Match exact sur le nom normalisé
    candidates = by_name.get(norm, [])
    if candidates:
        picked = pick_temporal(candidates, year)
        if picked is not None:
            c, tscore = picked
            return Resolution(c.slug, 0.95 if tscore == 1.0 else 0.90, "exact_name")
        if len(candidates) == 1:
            return Resolution(candidates[0].slug, 0.90, "exact_name_no_dates")
        return Resolution(
            None, 0.0, "ambiguous_exact",
            note=f"{len(candidates)} homonymes pour {auteur!r}",
        )

    # 4) Fuzzy match — limité aux clercs temporellement plausibles si on a une
    # année. Sinon, on parcourt tout (coûteux mais N ≈ 45k acceptable car
    # ce chemin n'est emprunté que pour les ~quelques dizaines d'auteurs
    # qui n'ont pas matché exact).
    best_slug: str | None = None
    best_ratio = 0.0
    for cands in by_name.values():
        for c in cands:
            if year is not None and temporal_score(c, year) == 0.0:
                continue
            ratio = SequenceMatcher(None, norm, normalize(c.nom)).ratio()
            if ratio > best_ratio:
                best_ratio = ratio
                best_slug = c.slug
    if best_slug is not None and best_ratio >= FUZZY_THRESHOLD:
        return Resolution(best_slug, best_ratio, "fuzzy")

    return Resolution(None, best_ratio, "no_match", note=f"meilleur ratio {best_ratio:.2f} pour {auteur!r}")


# ─────────────────────────── Itération corpus ───────────────────────────────


def iter_index(path: Path) -> Iterable[dict]:
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as fh:
        for line in fh:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def main() -> int:
    by_slug, by_name = load_clerics()
    print(f"Indexé {len(by_slug)} clercs ({sum(len(v) for v in by_name.values())} noms normalisés)")

    mappings: list[dict] = []
    unresolved: list[dict] = []
    inverse: dict[str, list[str]] = defaultdict(list)

    total = 0
    resolved = 0
    collectif = 0

    for index_path in (MAGISTERIUM_INDEX, LIVRES_INDEX):
        for doc in iter_index(index_path):
            total += 1
            res = resolve_author(doc, by_slug, by_name)
            if res.cleric_slug is not None and res.confidence >= MIN_CONFIDENCE:
                row = {
                    "document_slug": doc["slug"],
                    "doc_path": doc["path"],
                    "cleric_slug": res.cleric_slug,
                    "confidence": round(res.confidence, 3),
                    "method": res.method,
                }
                mappings.append(row)
                inverse[res.cleric_slug].append(doc["slug"])
                resolved += 1
            else:
                unresolved.append(
                    {
                        "document_slug": doc["slug"],
                        "doc_path": doc["path"],
                        "auteur": doc.get("auteur"),
                        "method": res.method,
                        "confidence": round(res.confidence, 3),
                        "note": res.note,
                    }
                )
                if res.method == "collective":
                    collectif += 1

    # Tri stable pour des sorties reproductibles
    mappings.sort(key=lambda r: r["document_slug"])
    unresolved.sort(key=lambda r: r["document_slug"])
    inverse_sorted = {k: sorted(set(v)) for k, v in sorted(inverse.items())}

    OUT_MAPPING.parent.mkdir(parents=True, exist_ok=True)
    with OUT_MAPPING.open("w", encoding="utf-8") as fh:
        for row in mappings:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    with OUT_UNRESOLVED.open("w", encoding="utf-8") as fh:
        for row in unresolved:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
    OUT_INVERSE.write_text(
        json.dumps(inverse_sorted, ensure_ascii=False, indent=2, sort_keys=True),
        encoding="utf-8",
    )

    print(f"Documents totaux        : {total}")
    print(f"Mappés (confidence ≥ {MIN_CONFIDENCE}): {resolved}")
    print(f"Auteurs collectifs (skip): {collectif}")
    print(f"Non résolus              : {len(unresolved) - collectif}")
    print()
    print("Top 10 clercs avec le plus de documents :")
    top = sorted(inverse_sorted.items(), key=lambda kv: len(kv[1]), reverse=True)[:10]
    for slug, docs in top:
        nom = by_slug[slug].nom if slug in by_slug else slug
        print(f"  {len(docs):4d}  {slug:<25}  {nom}")

    print()
    print(f"Écrit : {OUT_MAPPING.relative_to(REPO_ROOT)}")
    print(f"Écrit : {OUT_INVERSE.relative_to(REPO_ROOT)}")
    print(f"Écrit : {OUT_UNRESOLVED.relative_to(REPO_ROOT)}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
