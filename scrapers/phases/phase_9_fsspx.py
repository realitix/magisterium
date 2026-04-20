"""Phase 9 — Documents de la Fraternité Saint-Pie X (FSSPX).

Source principale : laportelatine.org (district de France). Les textes de Mgr
Lefebvre et les déclarations de la Fraternité sont principalement en français
(langue source), parfois avec des traductions latines pour les correspondances
formelles avec les dicastères romains (non ciblées ici).

Organisation :
- magisterium/D-fsspx/mgr-lefebvre/        — écrits de Mgr Marcel Lefebvre
- magisterium/D-fsspx/superieurs-generaux/ — Mgr Fellay, Don Pagliarani, etc.
- magisterium/D-fsspx/documents-fraternite/ — déclarations officielles

Les textes retenus couvrent les grands marqueurs doctrinaux et historiques :
fondation (1974), homélies de rupture (1976), sacres (1988), critique de
Vatican II, relations Rome-FSSPX, textes de succession.
"""
from __future__ import annotations

import asyncio
import sys
from datetime import date

from scrapers.core.pipeline import DocRef, MAGISTERIUM_ROOT, run_pipeline

# ---------------------------------------------------------------------------
# Documents Mgr Lefebvre — bucket "mgr-lefebvre"
# ---------------------------------------------------------------------------
# Schema: (url, slug, date_iso, incipit, titre_fr, doc_type, sujets)
LEFEBVRE_DOCS: list[tuple[str, str, str, str, str, str, list[str]]] = [
    # --- Déclaration fondatrice ---
    (
        "https://laportelatine.org/qui-sommes-nous/declaration-du-21-novembre-1974",
        "1974-11-21_declaration-21-novembre_declaration",
        "1974-11-21",
        "Déclaration du 21 novembre 1974",
        "Nous adhérons de tout cœur à la Rome catholique",
        "declaration",
        ["fondation-fsspx", "rome-eternelle", "vatican-ii", "profession-de-foi"],
    ),
    # --- Sermons historiques ---
    (
        "https://laportelatine.org/formation/mgr-lefebvre/sermons-historiques/lille-29-aout-1976",
        "1976-08-29_sermon-lille_sermon",
        "1976-08-29",
        "Sermon de Lille",
        "Sermon de Mgr Lefebvre à Lille — 29 août 1976",
        "sermon",
        ["messe-traditionnelle", "resistance", "crise-eglise"],
    ),
    (
        "https://laportelatine.org/formation/mgr-lefebvre/sermons-historiques/sermon-des-ordinations-sacerdotales-econe-le-29-juin-1976-mgr-lefebvre",
        "1976-06-29_sermon-ordinations-econe_sermon",
        "1976-06-29",
        "Sermon des ordinations sacerdotales à Écône",
        "Ordinations sacerdotales à Écône — 29 juin 1976",
        "sermon",
        ["ordinations", "econe", "resistance", "tradition"],
    ),
    (
        "https://laportelatine.org/formation/mgr-lefebvre/sermons-historiques/conference-de-mgr-lefebvre-annecy-1987",
        "1987-09-27_conference-annecy_conference",
        "1987-09-27",
        "J'ai vu des prêtres pleurer",
        "Conférence à Annecy — 27 septembre 1987",
        "conference",
        ["crise-eglise", "sacerdoce", "liturgie"],
    ),
    (
        "https://laportelatine.org/formation/mgr-lefebvre/sermons-historiques/sermon-de-mgr-lefebvre-au-bourget-le-11-novembre-1989",
        "1989-11-19_sermon-bourget_sermon",
        "1989-11-19",
        "Sermon du Bourget",
        "Sermon au Bourget — 19 novembre 1989",
        "sermon",
        ["tradition", "resistance", "jubile-sacerdotal"],
    ),
    # --- Sacres de 1988 ---
    (
        "https://laportelatine.org/formation/crise-eglise/rapports-rome-fsspx/lettre-de-mgr-lefebvre-a-jean-paul-ii-du-2-juin-1988",
        "1988-06-02_lettre-jean-paul-ii_lettre",
        "1988-06-02",
        "Lettre à Jean-Paul II du 2 juin 1988",
        "Lettre de Mgr Lefebvre à Jean-Paul II annonçant les sacres",
        "lettre",
        ["sacres-1988", "rapports-rome-fsspx", "rupture-accord"],
    ),
    (
        "https://laportelatine.org/formation/mgr-lefebvre/pourquoi-il-faut-sacrer-des-eveques-explications-aux-journalistes",
        "1988-06-15_pourquoi-sacrer-eveques_conference-presse",
        "1988-06-15",
        "Pourquoi il faut sacrer des évêques",
        "Conférence de presse à Écône — 15 juin 1988",
        "conference-presse",
        ["sacres-1988", "etat-de-necessite", "tradition"],
    ),
    (
        "https://laportelatine.org/formation/crise-eglise/sacres-1988/sacres-1988",
        "1988-06-30_sermon-sacres-episcopaux_sermon",
        "1988-06-30",
        "Sermon des sacres épiscopaux",
        "Sermon des sacres du 30 juin 1988 à Écône",
        "sermon",
        ["sacres-1988", "operation-survie", "tradition", "etat-de-necessite"],
    ),
    (
        "https://laportelatine.org/formation/mgr-lefebvre/textes/sermon-de-mgr-lefebvre-homelie-de-mgr-lefebvre-declaration-et-profession-de-foi-de-mgr-de-castro-mayer-sacre-de-quatre-eveques-30-juin-1988",
        "1988-06-30_homelie-sacres-castro-mayer_sermon",
        "1988-06-30",
        "Homélie et profession de foi aux sacres",
        "Homélie aux sacres — Déclaration de Mgr de Castro Mayer — 30 juin 1988",
        "sermon",
        ["sacres-1988", "castro-mayer", "profession-de-foi"],
    ),
    (
        "https://laportelatine.org/formation/crise-eglise/sacres-1988/la-ou-est-la-tradition-la-est-leglise",
        "1988-11-19_la-ou-est-la-tradition_sermon",
        "1988-11-19",
        "Là où est la Tradition, là est l'Église",
        "Sermon à Notre-Dame des Marches — action de grâces pour les sacres",
        "sermon",
        ["sacres-1988", "tradition", "ecclesiologie"],
    ),
    # --- Dubia / documents doctrinaux ---
    (
        "https://laportelatine.org/formation/crise-eglise/rapports-rome-fsspx/dubia-sur-la-liberte-religieuse-remis-par-mgr-lefebvre-a-la-congregation-pour-la-doctrine-de-la-foi-du-6-novembre-1985",
        "1985-11-06_dubia-liberte-religieuse_dubia",
        "1985-11-06",
        "Dubia sur la liberté religieuse",
        "Dubia remis par Mgr Lefebvre à la CDF — 6 novembre 1985",
        "dubia",
        ["liberte-religieuse", "vatican-ii", "dignitatis-humanae", "cdf"],
    ),
    # --- Textes doctrinaux et pastoraux ---
    (
        "https://laportelatine.org/formation/mgr-lefebvre/textes/la-nouvelle-messe-selon-mgr-lefebvre",
        "1976-00-00_la-nouvelle-messe_article",
        None,
        "La nouvelle messe selon Mgr Lefebvre",
        "La nouvelle messe : critique doctrinale",
        "article",
        ["nouvelle-messe", "novus-ordo", "sacrifice", "liturgie"],
    ),
    (
        "https://laportelatine.org/formation/mgr-lefebvre/textes/nous-faisons-la-sourde-oreille-aux-nouveautes-destructrices-de-leglise",
        "nd_sourde-oreille-nouveautes_article",
        None,
        "Nous faisons la sourde oreille aux nouveautés destructrices",
        "Nous faisons la sourde oreille aux nouveautés destructrices de l'Église",
        "article",
        ["tradition", "resistance", "vatican-ii"],
    ),
    # --- Actes magistériels personnels & télégrammes publics ---
    (
        "https://laportelatine.org/formation/mgr-lefebvre/textes/halte-a-lavortement",
        "1979-11-27_telegramme-avortement_telegramme",
        "1979-11-27",
        "Halte à l'avortement",
        "Télégramme contre la légalisation de l'avortement",
        "telegramme",
        ["avortement", "morale", "loi-naturelle"],
    ),
    # --- Sermons représentatifs (période d'Écône) ---
    (
        "https://laportelatine.org/formation/mgr-lefebvre/textes/sermon-de-mgr-lefebvre-christ-roi-diaconat-sous-diaconat-31-octobre-1976",
        "1976-10-31_sermon-christ-roi_sermon",
        "1976-10-31",
        "Sermon du Christ-Roi — 31 octobre 1976",
        "Christ-Roi — Diaconat et sous-diaconat à Écône",
        "sermon",
        ["christ-roi", "royaute-sociale", "ordinations"],
    ),
    (
        "https://laportelatine.org/formation/mgr-lefebvre/textes/sermon-de-mgr-lefebvre-toussaint-1er-novembre-1976",
        "1976-11-01_sermon-toussaint_sermon",
        "1976-11-01",
        "Nous sommes des pèlerins du Ciel",
        "Sermon de la Toussaint — 1er novembre 1976",
        "sermon",
        ["eschatologie", "sanctete", "toussaint"],
    ),
    (
        "https://laportelatine.org/formation/mgr-lefebvre/textes/sermon-de-mgr-lefebvre-noel-25-decembre-1978",
        "1978-12-25_sermon-noel_sermon",
        "1978-12-25",
        "Vivons-nous vraiment avec Jésus ?",
        "Sermon de Noël — 25 décembre 1978",
        "sermon",
        ["noel", "vie-spirituelle", "incarnation"],
    ),
    (
        "https://laportelatine.org/formation/mgr-lefebvre/textes/sermon-de-mgr-lefebvre-notre-dame-de-compassion-10-avril-1981",
        "1981-04-10_sermon-notre-dame-compassion_sermon",
        "1981-04-10",
        "Sermon de Notre-Dame de Compassion",
        "Notre-Dame de Compassion — 10 avril 1981",
        "sermon",
        ["marie", "compassion", "passion"],
    ),
    (
        "https://laportelatine.org/formation/mgr-lefebvre/textes/sermon-de-mgr-lefebvre-ordinations-sacerdotales-29-juin-1982",
        "1982-06-29_sermon-passion-eglise_sermon",
        "1982-06-29",
        "La Passion de l'Église",
        "Sermon aux ordinations sacerdotales — 29 juin 1982",
        "sermon",
        ["sacerdoce", "passion-eglise", "ordinations"],
    ),
    (
        "https://laportelatine.org/formation/mgr-lefebvre/textes/sermon-de-mgr-lefebvre-toussaint-20eme-anniversaire-de-la-fraternite-1er-novembre-1990",
        "1990-11-01_sermon-20-ans-fsspx_sermon",
        "1990-11-01",
        "20e anniversaire de la FSSPX",
        "Toussaint — 20e anniversaire de la Fraternité — 1er novembre 1990",
        "sermon",
        ["fsspx", "anniversaire", "tradition"],
    ),
    (
        "https://laportelatine.org/formation/mgr-lefebvre/textes/sermon-de-mgr-lefebvre-jubile-sacerdotal-60-ans-19-novembre-1989",
        "1989-11-19_jubile-sacerdotal_sermon",
        "1989-11-19",
        "Jubilé sacerdotal — 60 ans",
        "Sermon du jubilé sacerdotal — 19 novembre 1989",
        "sermon",
        ["sacerdoce", "jubile", "temoignage"],
    ),
    (
        "https://laportelatine.org/formation/mgr-lefebvre/textes/sermon-de-mgr-lefebvre-assomption-15-aout-1990",
        "1990-08-15_sermon-assomption_sermon",
        "1990-08-15",
        "Les regards tournés vers le ciel",
        "Sermon de l'Assomption — 15 août 1990",
        "sermon",
        ["marie", "assomption", "eschatologie"],
    ),
    (
        "https://laportelatine.org/formation/mgr-lefebvre/textes/sermon-de-mgr-lefebvre-premier-dimanche-de-careme-17-fevrier-1991",
        "1991-02-17_sermon-careme_sermon",
        "1991-02-17",
        "Sermon du premier dimanche de Carême",
        "Premier dimanche de Carême — 17 février 1991 (dernier Carême)",
        "sermon",
        ["careme", "penitence", "dernier-sermon"],
    ),
]

# ---------------------------------------------------------------------------
# Documents des supérieurs généraux — bucket "superieurs-generaux"
# ---------------------------------------------------------------------------
SUPERIEURS_DOCS: list[tuple[str, str, str, str, str, str, str, list[str]]] = [
    # (url, slug, date_iso_or_none, incipit, titre_fr, auteur, doc_type, sujets)
    (
        "https://laportelatine.org/formation/crise-eglise/rapports-rome-fsspx/1974-2024-semper-idem",
        "2024-11-22_semper-idem_declaration",
        "2024-11-22",
        "Semper idem — 50e anniversaire de la Déclaration de 1974",
        "Message du supérieur général et de ses assistants — 1974–2024 Semper idem",
        "Don Davide Pagliarani",
        "declaration",
        ["anniversaire-declaration-1974", "semper-idem", "tradition"],
    ),
]

# ---------------------------------------------------------------------------
# Documents de la Fraternité — bucket "documents-fraternite"
# ---------------------------------------------------------------------------
FRATERNITE_DOCS: list[tuple[str, str, str, str, str, str, list[str]]] = [
    # (url, slug, date_iso_or_none, incipit, titre_fr, doc_type, sujets)
    (
        "https://laportelatine.org/critique-du-concile-vatican-ii/la-nouvelle-liturgie",
        "nd_critique-nouvelle-liturgie_etude",
        None,
        "La nouvelle liturgie",
        "Critique de la nouvelle liturgie issue de Vatican II",
        "etude",
        ["nouvelle-messe", "liturgie", "novus-ordo"],
    ),
    (
        "https://laportelatine.org/critique-du-concile-vatican-ii/62-raisons-pour-lesquelles-nous-ne-pouvons-assister-a-la-nouvelle-messe",
        "nd_62-raisons-nouvelle-messe_etude",
        None,
        "62 raisons pour lesquelles nous ne pouvons assister à la nouvelle messe",
        "62 raisons contre la nouvelle messe",
        "etude",
        ["nouvelle-messe", "liturgie", "novus-ordo"],
    ),
    (
        "https://laportelatine.org/critique-du-concile-vatican-ii/le-nouvel-oecumenisme-de-vatican-ii",
        "nd_nouvel-oecumenisme_etude",
        None,
        "Le nouvel œcuménisme de Vatican II",
        "Le nouvel œcuménisme de Vatican II",
        "etude",
        ["oecumenisme", "vatican-ii"],
    ),
    (
        "https://laportelatine.org/critique-du-concile-vatican-ii/une-conception-collegiale-de-leglise-vue-comme-communion",
        "nd_conception-collegiale-eglise_etude",
        None,
        "Une conception collégiale de l'Église comme communion",
        "Ecclésiologie collégiale et communion — critique",
        "etude",
        ["ecclesiologie", "collegialite", "vatican-ii"],
    ),
    (
        "https://laportelatine.org/critique-du-concile-vatican-ii/le-subsistit-in-et-la-nouvelle-conception-de-leglise",
        "nd_subsistit-in_etude",
        None,
        "Le « subsistit in » et la nouvelle conception de l'Église",
        "Analyse du « subsistit in » (Lumen Gentium 8)",
        "etude",
        ["subsistit-in", "ecclesiologie", "lumen-gentium"],
    ),
    (
        "https://laportelatine.org/critique-du-concile-vatican-ii/la-redefinition-de-leglise-comme-sacrement",
        "nd_eglise-comme-sacrement_etude",
        None,
        "La redéfinition de l'Église comme sacrement",
        "La redéfinition de l'Église comme sacrement",
        "etude",
        ["ecclesiologie", "sacramentalite", "vatican-ii"],
    ),
    (
        "https://laportelatine.org/critique-du-concile-vatican-ii/y-a-t-il-un-droit-naturel-a-la-liberte-religieuse",
        "nd_droit-naturel-liberte-religieuse_etude",
        None,
        "Y a-t-il un droit naturel à la liberté religieuse ?",
        "Analyse critique du droit à la liberté religieuse",
        "etude",
        ["liberte-religieuse", "dignitatis-humanae", "droit-naturel"],
    ),
]


def _parse_date(d: str | None) -> date | None:
    if not d:
        return None
    return date.fromisoformat(d)


def build_refs() -> list[DocRef]:
    fsspx_root = MAGISTERIUM_ROOT / "D-fsspx"
    refs: list[DocRef] = []

    # --- Mgr Lefebvre ---
    lefebvre_dir = fsspx_root / "mgr-lefebvre"
    for url, slug, date_iso, incipit, titre_fr, doc_type, sujets in LEFEBVRE_DOCS:
        refs.append(
            DocRef(
                url=url,
                target_dir=lefebvre_dir,
                slug=slug,
                lang="fr",
                meta_hints={
                    "incipit": incipit,
                    "titre_fr": titre_fr,
                    "auteur": "Mgr Marcel Lefebvre",
                    "periode": "fsspx",
                    "type": doc_type,
                    "date": _parse_date(date_iso) if date_iso else None,
                    "autorite_magisterielle": None,
                    "langue_originale": "fr",
                    "langues_disponibles": ["fr"],
                    "sujets": sujets,
                },
            )
        )

    # --- Supérieurs généraux ---
    sg_dir = fsspx_root / "superieurs-generaux"
    for url, slug, date_iso, incipit, titre_fr, auteur, doc_type, sujets in SUPERIEURS_DOCS:
        refs.append(
            DocRef(
                url=url,
                target_dir=sg_dir,
                slug=slug,
                lang="fr",
                meta_hints={
                    "incipit": incipit,
                    "titre_fr": titre_fr,
                    "auteur": auteur,
                    "periode": "fsspx",
                    "type": doc_type,
                    "date": _parse_date(date_iso) if date_iso else None,
                    "autorite_magisterielle": None,
                    "langue_originale": "fr",
                    "langues_disponibles": ["fr"],
                    "sujets": sujets,
                },
            )
        )

    # --- Documents de la Fraternité ---
    fr_dir = fsspx_root / "documents-fraternite"
    for url, slug, date_iso, incipit, titre_fr, doc_type, sujets in FRATERNITE_DOCS:
        refs.append(
            DocRef(
                url=url,
                target_dir=fr_dir,
                slug=slug,
                lang="fr",
                meta_hints={
                    "incipit": incipit,
                    "titre_fr": titre_fr,
                    "auteur": "Fraternité Saint-Pie X",
                    "periode": "fsspx",
                    "type": doc_type,
                    "date": _parse_date(date_iso) if date_iso else None,
                    "autorite_magisterielle": None,
                    "langue_originale": "fr",
                    "langues_disponibles": ["fr"],
                    "sujets": sujets,
                },
            )
        )

    return refs


async def main() -> int:
    import os
    refresh = os.environ.get("REFRESH") == "1"
    refs = build_refs()
    print(f"Phase 9 — {len(refs)} documents FSSPX (Lefebvre + supérieurs + Fraternité)")
    result = await run_pipeline(refs, phase="phase-9-fsspx", refresh=refresh)
    print(
        f"ok={result.n_ok} skipped={result.n_skipped} "
        f"errors={result.n_errors}"
    )
    return 0 if result.n_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
