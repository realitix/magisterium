"""Récupère quelques documents atypiques du magistère post-V2.

Documents visés (URLs non-standard, hors phases régulières) :
  - Jean-Paul II : trois audiences générales sur les fins dernières (1999)
    Ciel (21 juillet), Enfer (28 juillet), Purgatoire (4 août).
  - Benoît XVI : discours à la Curie romaine du 22 décembre 2005
    (« herméneutique de la réforme dans la continuité »).
  - François : Document sur la fraternité humaine d'Abou Dhabi
    (4 février 2019), signé conjointement avec Ahmed al-Tayyeb.

Langue originale retenue : italien dans tous les cas
(version officielle vatican.va pour les audiences/discours/déclarations
post-conciliaires non traduits en latin).
"""
from __future__ import annotations

import asyncio
from datetime import date

from scrapers.core.pipeline import DocRef, MAGISTERIUM_ROOT, run_pipeline


POST_V2 = MAGISTERIUM_ROOT / "C-post-vatican-ii" / "papes"


def _build_refs() -> list[DocRef]:
    refs: list[DocRef] = []

    # --- Jean-Paul II — audiences générales 1999 sur les fins dernières -----
    jp2_dir = POST_V2 / "1978-jean-paul-ii" / "audiences-generales"
    jp2_audiences = [
        ("1999-07-21", "21071999", "Le ciel comme plénitude de l'intimité avec Dieu",
         "ciel", ["fins-dernieres", "ciel"]),
        ("1999-07-28", "28071999", "L'enfer comme refus définitif de Dieu",
         "enfer", ["fins-dernieres", "enfer"]),
        ("1999-08-04", "04081999", "Le purgatoire, purification nécessaire",
         "purgatoire", ["fins-dernieres", "purgatoire"]),
    ]
    for date_iso, urldate, titre_fr, theme, sujets in jp2_audiences:
        slug = f"{date_iso}_audience-generale-{theme}_audience"
        url = (
            "https://www.vatican.va/content/john-paul-ii/it/audiences/1999/"
            f"documents/hf_jp-ii_aud_{urldate}.html"
        )
        refs.append(DocRef(
            url=url,
            target_dir=jp2_dir,
            slug=slug,
            lang="it",
            meta_hints={
                "incipit": f"Audience générale — {theme.capitalize()}",
                "titre_fr": titre_fr,
                "auteur": "Jean-Paul II",
                "periode": "post-vatican-ii",
                "type": "audience-generale",
                "date": date.fromisoformat(date_iso),
                "autorite_magisterielle": "magistere-ordinaire",
                "langue_originale": "it",
                "langues_disponibles": ["it"],
                "sujets": sujets,
            },
        ))

    # --- Benoît XVI — discours à la Curie 22 décembre 2005 ------------------
    b16_dir = POST_V2 / "2005-benoit-xvi" / "discours"
    refs.append(DocRef(
        url=(
            "https://www.vatican.va/content/benedict-xvi/it/speeches/2005/"
            "december/documents/hf_ben_xvi_spe_20051222_roman-curia.html"
        ),
        target_dir=b16_dir,
        slug="2005-12-22_discours-curie-hermeneutique_discours",
        lang="it",
        meta_hints={
            "incipit": "Discours à la Curie romaine",
            "titre_fr": "Sur l'herméneutique de la réforme dans la continuité — vœux à la Curie romaine",
            "auteur": "Benoît XVI",
            "periode": "post-vatican-ii",
            "type": "discours",
            "date": date(2005, 12, 22),
            "autorite_magisterielle": "magistere-ordinaire",
            "langue_originale": "it",
            "langues_disponibles": ["it"],
            "sujets": ["vatican-ii", "hermeneutique-continuite", "reforme"],
        },
    ))

    # --- François — Document sur la fraternité humaine, Abou Dhabi 2019 -----
    fra_dir = POST_V2 / "2013-francois" / "declarations-conjointes"
    refs.append(DocRef(
        url=(
            "https://www.vatican.va/content/francesco/it/travels/2019/outside/"
            "documents/papa-francesco_20190204_documento-fratellanza-umana.html"
        ),
        target_dir=fra_dir,
        slug="2019-02-04_documento-fratellanza-umana_declaration",
        lang="it",
        meta_hints={
            "incipit": "Documento sulla fratellanza umana",
            "titre_fr": "Document sur la fraternité humaine pour la paix mondiale et la coexistence commune (signé avec Ahmed al-Tayyeb, Abou Dhabi)",
            "auteur": "François",
            "periode": "post-vatican-ii",
            "type": "declaration-conjointe",
            "date": date(2019, 2, 4),
            "autorite_magisterielle": "magistere-ordinaire",
            "langue_originale": "it",
            "langues_disponibles": ["it"],
            "sujets": ["dialogue-interreligieux", "islam", "fraternite",
                       "salut-universel", "abu-dhabi"],
        },
    ))

    return refs


async def main() -> None:
    refs = _build_refs()
    print(f"Pipeline : {len(refs)} documents atypiques post-V2.")
    result = await run_pipeline(
        refs, phase="extra-post-v2", refresh=False, concurrency=4,
    )
    print(f"OK={result.n_ok}  SKIPPED={result.n_skipped}  ERRORS={result.n_errors}")


if __name__ == "__main__":
    asyncio.run(main())
