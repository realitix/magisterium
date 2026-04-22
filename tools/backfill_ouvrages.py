"""Backfill the `ouvrage` block in .meta.yaml for multi-part works.

Motivation
----------
Some works are scraped as N separate documents (one .meta.yaml per part)
because a single .md for the whole work would be too large to render /
translate. Without an aggregation concept, these parts appear as N
independent cards in the site's DocumentsGrid and as N independent hits in
the Pagefind index — the user can't see they belong together.

This script declaratively annotates each part with an `ouvrage` block
(slug, titre, partie_index 1-based, partie_titre, total_parties). The site
then uses this to group cards and search results.

Idempotent. Re-running after adding a new part updates total_parties on
siblings automatically.

Run: ``uv run python -m tools.backfill_ouvrages``
"""
from __future__ import annotations

import sys
from dataclasses import dataclass
from pathlib import Path

import yaml

from scrapers.core.meta import DocMeta, Ouvrage

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "magisterium"


@dataclass(frozen=True)
class ExplicitPart:
    stem: str         # filename stem (no .meta.yaml)
    label: str        # partie_titre


@dataclass(frozen=True)
class ExplicitOuvrage:
    slug: str
    titre: str
    rel_dir: str
    parties: tuple[ExplicitPart, ...]


# ---------------------------------------------------------------------------
# Ouvrages énumérés explicitement : on connaît le nombre exact de parties,
# l'ordre canonique, et un libellé humain pour chacune.
# ---------------------------------------------------------------------------
EXPLICIT: tuple[ExplicitOuvrage, ...] = (
    ExplicitOuvrage(
        slug="1566_catechismus-romanus",
        titre="Catéchisme du concile de Trente",
        rel_dir="A-pre-vatican-ii/catechismes/1566-romain-trente",
        parties=(
            ExplicitPart("1566_catechismus-romanus_00-praefatio", "Préface (Clément XIII)"),
            ExplicitPart("1566_catechismus-romanus_01-pars-prima", "Pars prima — Le Symbole"),
            ExplicitPart("1566_catechismus-romanus_02-pars-secunda", "Pars secunda — Les Sacrements"),
            ExplicitPart("1566_catechismus-romanus_03-pars-tertia", "Pars tertia — Le Décalogue"),
            ExplicitPart("1566_catechismus-romanus_04-pars-quarta", "Pars quarta — L'Oraison dominicale"),
            ExplicitPart("1566_catechismus-romanus_05-praxis-concionatoria", "Praxis concionatoria (homilétique)"),
        ),
    ),
    ExplicitOuvrage(
        slug="1598_dottrina-cristiana-breve",
        titre="Dottrina cristiana breve (saint Robert Bellarmin)",
        rel_dir="A-pre-vatican-ii/catechismes/1598-bellarmin",
        parties=(
            ExplicitPart("1598_dottrina-cristiana-breve_00-premessa", "Premessa"),
            ExplicitPart("1598_dottrina-cristiana-breve_01-prima-classe-fine-cristiano-credo", "Prima classe — Fin du chrétien, Credo"),
            ExplicitPart("1598_dottrina-cristiana-breve_02-seconda-classe-pater-ave", "Seconda classe — Pater, Ave Maria"),
            ExplicitPart("1598_dottrina-cristiana-breve_03-terza-classe-comandamenti-sacramenti-rosario", "Terza classe — Commandements, Sacrements, Rosaire"),
            ExplicitPart("1598_dottrina-cristiana-breve_04-atti-virtu-fede-speranza-carita-contrizione", "Actes des vertus (Foi, Espérance, Charité, Contrition)"),
            ExplicitPart("1598_dottrina-cristiana-breve_05-istruzioni-sacramenti-cresima-penitenza-eucaristia", "Istruzioni — Confirmation, Pénitence, Eucharistie"),
        ),
    ),
    ExplicitOuvrage(
        slug="1885_baltimore-catechism",
        titre="Baltimore Catechism",
        rel_dir="A-pre-vatican-ii/catechismes/1885-baltimore",
        parties=(
            ExplicitPart("1885-04-06_baltimore-catechism-no-1", "No. 1 — débutants"),
            ExplicitPart("1885-04-06_baltimore-catechism-no-2", "No. 2 — préparation à la confirmation"),
            ExplicitPart("1885-04-06_baltimore-catechism-no-3", "No. 3 — première communion"),
            ExplicitPart("1885-04-06_baltimore-catechism-no-4", "No. 4 — pour les enseignants"),
        ),
    ),
    ExplicitOuvrage(
        slug="1908_catechismo-pio-x",
        titre="Catéchisme de saint Pie X",
        rel_dir="A-pre-vatican-ii/catechismes/1908-pie-x",
        parties=(
            ExplicitPart("1908_catechismo-pio-x_00-introduzione", "Introduction"),
            ExplicitPart("1908_catechismo-pio-x_01-lettera-respighi", "Lettre au cardinal Respighi"),
            ExplicitPart("1908_catechismo-pio-x_02-lezione-preliminare", "Leçon préliminaire"),
            ExplicitPart("1908_catechismo-pio-x_03-parte-1-credo", "Partie I — Credo"),
            ExplicitPart("1908_catechismo-pio-x_04-parte-2-orazione", "Partie II — Oraison"),
            ExplicitPart("1908_catechismo-pio-x_05-parte-3-comandamenti", "Partie III — Commandements"),
            ExplicitPart("1908_catechismo-pio-x_06-parte-4-sacramenti", "Partie IV — Sacrements"),
            ExplicitPart("1908_catechismo-pio-x_07-parte-5-virtu", "Partie V — Vertus et vices"),
            ExplicitPart("1908_catechismo-pio-x_08-feste-1-signore", "Fêtes I — du Seigneur"),
            ExplicitPart("1908_catechismo-pio-x_09-feste-2-vergine-santi", "Fêtes II — de la Vierge et des Saints"),
            ExplicitPart("1908_catechismo-pio-x_10-storia-principi", "Histoire — Principes"),
            ExplicitPart("1908_catechismo-pio-x_11-storia-1-antico-testamento", "Histoire I — Ancien Testament"),
            ExplicitPart("1908_catechismo-pio-x_12-storia-2-nuovo-testamento", "Histoire II — Nouveau Testament"),
            ExplicitPart("1908_catechismo-pio-x_13-storia-3-ecclesiastica", "Histoire III — ecclésiastique"),
            ExplicitPart("1908_catechismo-pio-x_14-preghiere-1", "Prières I"),
            ExplicitPart("1908_catechismo-pio-x_15-preghiere-2", "Prières II"),
        ),
    ),
    ExplicitOuvrage(
        slug="1983_codex-iuris-canonici",
        titre="Code de droit canonique (1983)",
        rel_dir="C-post-vatican-ii/droit-canonique/1983-cic",
        parties=(
            ExplicitPart("1983-01-25_sacrae-disciplinae-leges_const", "Sacrae disciplinae leges (constitution apostolique de promulgation)"),
            ExplicitPart("1983-01-25_codex-iuris-canonici-index_code", "Index général"),
            ExplicitPart("1983-01-25_codex-iuris-canonici-liber-01_code", "Livre I — Normes générales"),
            ExplicitPart("1983-01-25_codex-iuris-canonici-liber-02_code", "Livre II — Peuple de Dieu"),
            ExplicitPart("1983-01-25_codex-iuris-canonici-liber-03_code", "Livre III — Fonction d'enseignement de l'Église"),
            ExplicitPart("1983-01-25_codex-iuris-canonici-liber-04_code", "Livre IV — Fonction de sanctification de l'Église"),
            ExplicitPart("1983-01-25_codex-iuris-canonici-liber-05_code", "Livre V — Biens temporels de l'Église"),
            ExplicitPart("1983-01-25_codex-iuris-canonici-liber-06_code", "Livre VI — Sanctions dans l'Église"),
            ExplicitPart("1983-01-25_codex-iuris-canonici-liber-07_code", "Livre VII — Procès"),
        ),
    ),
)


# ---------------------------------------------------------------------------
# CEC latin : 112 parties auto-détectées et auto-labellisées.
# ---------------------------------------------------------------------------
CEC_DIR = "C-post-vatican-ii/catechismes/1997-ccc-editio-typica-latina"
CEC_SLUG = "1997_ccc-editio-typica-latina"
CEC_TITRE = "Catéchisme de l'Église catholique (editio typica latina, 1997)"

# Libellés humains pour les fichiers de tête / annexes. Les 107 fichiers
# restants (corps doctrinal, identifiés par leur code de section vatican.va)
# tombent sur le chemin auto — on affiche "§ p1s1c2a3" etc.
CEC_FRONT_MATTER_LABELS: dict[str, tuple[int, str]] = {
    # (order_key, label) — order_key trié ASC place les pièces de tête en tête
    "aposcons": (0, "Constitution apostolique Fidei Depositum"),
    "lettera-apost": (1, "Lettre apostolique Laetamur Magnopere"),
    "prologue": (2, "Prologue"),
    "abbrev": (3, "Abréviations"),
    "index": (4, "Index général"),
}


def _cec_part_key(stem: str) -> str:
    """Extract the vatican.va section key from a CEC filename stem.

    `1997-08-15_ccc-lt_p1s1c2a3_lt` → `p1s1c2a3`
    """
    # Strip known prefix `1997-08-15_ccc-lt_` and suffix `_lt`.
    after = stem.removeprefix("1997-08-15_ccc-lt_")
    return after.removesuffix("_lt")


def _cec_sort_key(key: str) -> tuple[int, int, int, int, int, int, str]:
    """Numeric-aware sort key for CEC content parts.

    The vatican.va slugs encode section hierarchy. We pull up to 6 numeric
    components; non-parseable slugs fall back to lexicographic tail so
    ordering stays deterministic.
    """
    # Pull consecutive number runs; `p1s2c3a4p5` → (1, 2, 3, 4, 5, 0).
    import re

    nums = [int(m) for m in re.findall(r"\d+", key)]
    padded = (nums + [0, 0, 0, 0, 0, 0])[:6]
    return (*padded, key)  # type: ignore[return-value]


def collect_cec_parts() -> list[tuple[Path, str]]:
    """Return [(meta_path, label)] for the CEC, in canonical order."""
    cec_dir = CORPUS / CEC_DIR
    files = sorted(cec_dir.glob("*.meta.yaml"))
    front: list[tuple[int, Path, str]] = []
    body: list[tuple[tuple, Path, str]] = []
    for meta_path in files:
        key = _cec_part_key(meta_path.name.removesuffix(".meta.yaml"))
        if key in CEC_FRONT_MATTER_LABELS:
            order_key, label = CEC_FRONT_MATTER_LABELS[key]
            front.append((order_key, meta_path, label))
        else:
            body.append((_cec_sort_key(key), meta_path, f"§ {key}"))
    front.sort(key=lambda t: t[0])
    body.sort(key=lambda t: t[0])
    return [(p, l) for _, p, l in front] + [(p, l) for _, p, l in body]


# ---------------------------------------------------------------------------
# Exécution : applique le bloc `ouvrage` à chaque partie, idempotent.
# ---------------------------------------------------------------------------
def apply_ouvrage(meta_path: Path, ouvrage: Ouvrage) -> bool:
    """Read, update `ouvrage`, write back. Return True iff file changed."""
    data = yaml.safe_load(meta_path.read_text(encoding="utf-8"))
    if not isinstance(data, dict):
        raise ValueError(f"{meta_path}: not a dict")

    # Sérialisation Pydantic (same code path que DocMeta.write) pour éviter
    # la divergence de formats. Utilisation de model_dump(exclude_none=True)
    # pour un YAML propre.
    new_block = ouvrage.model_dump(mode="json", exclude_none=True)
    if data.get("ouvrage") == new_block:
        return False

    data["ouvrage"] = new_block

    # Re-valide l'ensemble pour attraper toute dérive de schéma dès maintenant.
    DocMeta.model_validate(data)

    meta_path.write_text(
        yaml.safe_dump(data, allow_unicode=True, sort_keys=False),
        encoding="utf-8",
    )
    return True


def process_explicit(ouv: ExplicitOuvrage) -> tuple[int, int]:
    """Apply the `ouvrage` block to every declared part. Return (changed, total)."""
    total = len(ouv.parties)
    changed = 0
    for idx, part in enumerate(ouv.parties, start=1):
        meta_path = CORPUS / ouv.rel_dir / f"{part.stem}.meta.yaml"
        if not meta_path.exists():
            print(f"  [SKIP] {meta_path.relative_to(CORPUS)} — not found", file=sys.stderr)
            continue
        block = Ouvrage(
            slug=ouv.slug,
            titre=ouv.titre,
            partie_index=idx,
            partie_titre=part.label,
            total_parties=total,
        )
        if apply_ouvrage(meta_path, block):
            changed += 1
    return changed, total


def process_cec() -> tuple[int, int]:
    parts = collect_cec_parts()
    total = len(parts)
    changed = 0
    for idx, (meta_path, label) in enumerate(parts, start=1):
        block = Ouvrage(
            slug=CEC_SLUG,
            titre=CEC_TITRE,
            partie_index=idx,
            partie_titre=label,
            total_parties=total,
        )
        if apply_ouvrage(meta_path, block):
            changed += 1
    return changed, total


def main() -> int:
    grand_changed = 0
    grand_total = 0
    for ouv in EXPLICIT:
        changed, total = process_explicit(ouv)
        grand_changed += changed
        grand_total += total
        print(f"[{ouv.slug}] {changed}/{total} parties mises à jour")

    changed, total = process_cec()
    grand_changed += changed
    grand_total += total
    print(f"[{CEC_SLUG}] {changed}/{total} parties mises à jour")

    print(f"\nTotal : {grand_changed} fichiers modifiés sur {grand_total} parties")
    return 0


if __name__ == "__main__":
    sys.exit(main())
