"""Ad-hoc scraping de deux documents liés à la traduction de "pro multis" :

1. Lettre du card. Arinze (CCDDS), 17 octobre 2006, Prot. N. 467/05/L,
   sur la traduction de pro multis dans la formule consécratoire du calice.
   Source archivée : USCCB (Conférence des évêques des États-Unis,
   destinataire de la lettre). La lettre a été envoyée par Rome aux
   présidents des conférences épiscopales en plusieurs versions
   linguistiques ; la version anglaise est celle qui circule officiellement
   dans le monde anglophone.

2. Communiqué officiel de la Conférence des évêques de France (5 nov. 2019)
   annonçant la réception du décret de *confirmatio* romain (1er oct. 2019,
   Préfet : card. Robert Sarah) pour la nouvelle traduction française de la
   3e édition typique du Missel romain — laquelle introduit notamment
   « pour la multitude » à la place de « pour tous » dans la formule
   consécratoire.

NOTE : la mission initiale mentionnait un « décret 2017 » sur la France ;
après vérification des sources (vaticannews, eglise.catholique.fr,
liturgie.catholique.fr), aucun décret 2017 spécifique sur la France n'existe.
2017 est l'année du motu proprio *Magnum principium* (déjà dans le corpus).
Le document de référence pour la décision pratique « pour la multitude » en
France est bien le décret du 1er octobre 2019, dont le communiqué CEF est la
trace officielle archivable.
"""
from __future__ import annotations

import asyncio
from datetime import date

from scrapers.core.pipeline import DocRef, MAGISTERIUM_ROOT, run_pipeline


def build_refs() -> list[DocRef]:
    refs: list[DocRef] = []

    # ── Document 1 : Lettre Arinze sur "pro multis" ───────────────────────
    arinze_dir = (
        MAGISTERIUM_ROOT
        / "C-post-vatican-ii"
        / "curie-romaine"
        / "culte-divin"
    )
    refs.append(
        DocRef(
            url=(
                "https://www.usccb.org/prayer-and-worship/the-mass/"
                "order-of-mass/liturgy-of-the-eucharist/"
                "letter-from-cardinal-arinze-on-the-translation-of-pro-multis"
            ),
            target_dir=arinze_dir,
            slug="2006-10-17_arinze-pro-multis_lettre",
            lang="en",
            body_selector="main, article, div.field--name-body, div.content",
            kind="originale",
            meta_hints={
                "incipit": "In July 2005 this Congregation",
                "titre_fr": (
                    "Lettre du card. Arinze sur la traduction de "
                    "« pro multis »"
                ),
                "titre_original": (
                    "Letter on the Translation of pro multis in the "
                    "Words of Consecration"
                ),
                "auteur": (
                    "Congrégation pour le culte divin et la discipline "
                    "des sacrements"
                ),
                "periode": "post-vatican-ii",
                "type": "lettre",
                "date": date(2006, 10, 17),
                "autorite_magisterielle": "magistere-ordinaire",
                "langue_originale": "en",
                "themes_doctrinaux": [
                    "liturgie",
                    "eucharistie",
                    "consecration",
                    "salut",
                    "traduction-liturgique",
                ],
                "sujets": [
                    "pro multis",
                    "consecration du calice",
                    "traduction du missel",
                    "for many",
                    "Prot. N. 467/05/L",
                ],
                "references_anterieures": [
                    "2001-03-28_liturgiam-authenticam_instr",
                ],
            },
        )
    )

    # ── Document 2 : Communiqué CEF sur la confirmatio (5 novembre 2019) ─
    cef_dir = (
        MAGISTERIUM_ROOT
        / "C-post-vatican-ii"
        / "liturgie"
        / "reformes-liturgiques"
    )
    refs.append(
        DocRef(
            url=(
                "https://eglise.catholique.fr/espace-presse/"
                "communiques-de-presse/"
                "487757-france-obtient-validation-de-nouvelle-traduction-"
                "missel-romain/"
            ),
            target_dir=cef_dir,
            slug="2019-11-05_cef-confirmatio-missel-francais_communique",
            lang="fr",
            body_selector="div.entry-content",
            kind="originale",
            meta_hints={
                "incipit": (
                    "La France obtient la validation de la nouvelle "
                    "traduction du Missel romain"
                ),
                "titre_fr": (
                    "Communiqué CEF — La France obtient la validation "
                    "de la nouvelle traduction du Missel romain "
                    "(décret de confirmatio du 1er octobre 2019)"
                ),
                "titre_original": (
                    "La France obtient la validation de la nouvelle "
                    "traduction du Missel romain"
                ),
                "auteur": "Conférence des évêques de France",
                "periode": "post-vatican-ii",
                "type": "communique",
                "date": date(2019, 11, 5),
                # Le communiqué CEF n'est pas en soi un acte du magistère
                # romain ; il rend public le décret CCDDS du 1er oct. 2019.
                # On garde la valeur magisterium (un communiqué d'une
                # conférence épiscopale est une trace magistérielle locale).
                "autorite_magisterielle": "conference-episcopale",
                "langue_originale": "fr",
                "themes_doctrinaux": [
                    "liturgie",
                    "eucharistie",
                    "consecration",
                    "traduction-liturgique",
                ],
                "sujets": [
                    "missel romain",
                    "troisieme edition typique",
                    "traduction francaise",
                    "pour la multitude",
                    "pro multis",
                    "confirmatio",
                    "Robert Sarah",
                ],
                "references_anterieures": [
                    "2001-03-28_liturgiam-authenticam_instr",
                    "2017-09-03_magnum-principium_mp",
                    "2006-10-17_arinze-pro-multis_lettre",
                ],
            },
        )
    )

    return refs


async def main() -> None:
    refs = build_refs()
    print(f"phase pro-multis : {len(refs)} document(s) à scraper")
    res = await run_pipeline(
        refs, phase="phase-pro-multis", refresh=False, concurrency=2
    )
    print(
        f"  ok={res.n_ok}  skipped={res.n_skipped}  errors={res.n_errors}"
    )


if __name__ == "__main__":
    asyncio.run(main())
