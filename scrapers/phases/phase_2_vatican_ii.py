"""Phase 2 — 16 documents of the Second Vatican Council (1962–1965).

Source: vatican.va — editio typica latina (official Latin text). All 16
documents (4 constitutions, 9 decrees, 3 declarations) are available in
Latin at the canonical URL pattern:
    https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents/{filename}_lt.html

All were promulgated by Paul VI except none — the council opened under John XXIII
but every single document was formally promulgated by Paul VI (John XXIII died
on 3 June 1963, before Sacrosanctum Concilium, the first document, was approved
on 4 December 1963).
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date

from scrapers.core.pipeline import DocRef, MAGISTERIUM_ROOT, run_pipeline

BASE = "https://www.vatican.va/archive/hist_councils/ii_vatican_council/documents"

# (category_dir, filename_id, incipit, titre_fr, date_iso, type, sujets)
# type ∈ {constitution-conciliaire, decret-conciliaire, declaration-conciliaire}
DOCUMENTS: list[tuple[str, str, str, str, str, str, list[str]]] = [
    # --- Constitutions (4) ---
    (
        "constitutions",
        "vat-ii_const_19631204_sacrosanctum-concilium",
        "Sacrosanctum Concilium",
        "Constitution sur la sainte liturgie",
        "1963-12-04",
        "constitution-conciliaire",
        ["liturgie"],
    ),
    (
        "constitutions",
        "vat-ii_const_19641121_lumen-gentium",
        "Lumen Gentium",
        "Constitution dogmatique sur l'Église",
        "1964-11-21",
        "constitution-conciliaire",
        ["ecclesiologie"],
    ),
    (
        "constitutions",
        "vat-ii_const_19651118_dei-verbum",
        "Dei Verbum",
        "Constitution dogmatique sur la Révélation divine",
        "1965-11-18",
        "constitution-conciliaire",
        ["revelation", "ecriture-sainte"],
    ),
    (
        "constitutions",
        "vat-ii_const_19651207_gaudium-et-spes",
        "Gaudium et Spes",
        "Constitution pastorale sur l'Église dans le monde de ce temps",
        "1965-12-07",
        "constitution-conciliaire",
        ["eglise-et-monde"],
    ),
    # --- Decrees (9) ---
    (
        "decrets",
        "vat-ii_decree_19631204_inter-mirifica",
        "Inter Mirifica",
        "Décret sur les moyens de communication sociale",
        "1963-12-04",
        "decret-conciliaire",
        ["communications-sociales"],
    ),
    (
        "decrets",
        "vat-ii_decree_19641121_orientalium-ecclesiarum",
        "Orientalium Ecclesiarum",
        "Décret sur les Églises orientales catholiques",
        "1964-11-21",
        "decret-conciliaire",
        ["eglises-orientales"],
    ),
    (
        "decrets",
        "vat-ii_decree_19641121_unitatis-redintegratio",
        "Unitatis Redintegratio",
        "Décret sur l'œcuménisme",
        "1964-11-21",
        "decret-conciliaire",
        ["oecumenisme"],
    ),
    (
        "decrets",
        "vat-ii_decree_19651028_christus-dominus",
        "Christus Dominus",
        "Décret sur la charge pastorale des évêques",
        "1965-10-28",
        "decret-conciliaire",
        ["eveques"],
    ),
    (
        "decrets",
        "vat-ii_decree_19651028_optatam-totius",
        "Optatam Totius",
        "Décret sur la formation des prêtres",
        "1965-10-28",
        "decret-conciliaire",
        ["formation-sacerdotale"],
    ),
    (
        "decrets",
        "vat-ii_decree_19651028_perfectae-caritatis",
        "Perfectae Caritatis",
        "Décret sur la rénovation de la vie religieuse",
        "1965-10-28",
        "decret-conciliaire",
        ["vie-religieuse"],
    ),
    (
        "decrets",
        "vat-ii_decree_19651118_apostolicam-actuositatem",
        "Apostolicam Actuositatem",
        "Décret sur l'apostolat des laïcs",
        "1965-11-18",
        "decret-conciliaire",
        ["laics", "apostolat"],
    ),
    (
        "decrets",
        "vat-ii_decree_19651207_ad-gentes",
        "Ad Gentes",
        "Décret sur l'activité missionnaire de l'Église",
        "1965-12-07",
        "decret-conciliaire",
        ["mission"],
    ),
    (
        "decrets",
        "vat-ii_decree_19651207_presbyterorum-ordinis",
        "Presbyterorum Ordinis",
        "Décret sur le ministère et la vie des prêtres",
        "1965-12-07",
        "decret-conciliaire",
        ["pretres", "ministere"],
    ),
    # --- Declarations (3) ---
    (
        "declarations",
        "vat-ii_decl_19651028_gravissimum-educationis",
        "Gravissimum Educationis",
        "Déclaration sur l'éducation chrétienne",
        "1965-10-28",
        "declaration-conciliaire",
        ["education"],
    ),
    (
        "declarations",
        "vat-ii_decl_19651028_nostra-aetate",
        "Nostra Aetate",
        "Déclaration sur les relations de l'Église avec les religions non chrétiennes",
        "1965-10-28",
        "declaration-conciliaire",
        ["religions-non-chretiennes"],
    ),
    (
        "declarations",
        "vat-ii_decl_19651207_dignitatis-humanae",
        "Dignitatis Humanae",
        "Déclaration sur la liberté religieuse",
        "1965-12-07",
        "declaration-conciliaire",
        ["liberte-religieuse"],
    ),
]


def build_refs() -> list[DocRef]:
    refs: list[DocRef] = []
    vat_ii_root = MAGISTERIUM_ROOT / "B-vatican-ii"
    for category, filename, incipit, titre_fr, date_iso, doc_type, sujets in DOCUMENTS:
        # Slug: YYYY-MM-DD_incipit-slug_type-short
        date_part = date_iso.replace("-", "")
        # Extract the incipit-slug from the filename
        # filename is like vat-ii_{const|decree|decl}_YYYYMMDD_incipit-slug
        parts = filename.split("_")
        incipit_slug = "_".join(parts[3:])  # everything after date
        # Short type token for the slug tail
        type_short = {
            "constitution-conciliaire": "const",
            "decret-conciliaire": "decret",
            "declaration-conciliaire": "decl",
        }[doc_type]
        slug = f"{date_iso}_{incipit_slug}_{type_short}"

        refs.append(
            DocRef(
                url=f"{BASE}/{filename}_lt.html",
                target_dir=vat_ii_root / category,
                slug=slug,
                lang="la",
                # The hist_councils pages use <div id="corpo"> for the body,
                # which is NOT covered by the default vatican.va SITE_SELECTORS.
                body_selector="#corpo",
                # The body text is nested inside a layout <table>; pandoc would
                # otherwise collapse it to "[TABLE]". Unwrap the table+font
                # wrappers so the <p> content flows as plain markdown.
                unwrap_tags=["table", "tbody", "tr", "td", "font"],
                meta_hints={
                    "incipit": incipit,
                    "titre_fr": titre_fr,
                    "auteur": "Paul VI",
                    "periode": "vatican-ii",
                    "type": doc_type,
                    "date": date.fromisoformat(date_iso),
                    "autorite_magisterielle": "magistere-extraordinaire-concile-oecumenique",
                    "langue_originale": "la",
                    "langues_disponibles": ["la"],
                    "sujets": sujets,
                },
            )
        )
    return refs


async def main() -> int:
    import os
    refresh = os.environ.get("REFRESH") == "1"
    refs = build_refs()
    print(f"Phase 2 — {len(refs)} documents de Vatican II")
    result = await run_pipeline(refs, phase="phase-2-vatican-ii", refresh=refresh)
    print(
        f"ok={result.n_ok} skipped={result.n_skipped} "
        f"errors={result.n_errors}"
    )
    return 0 if result.n_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
