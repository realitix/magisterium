"""Phase 8 — Droit canonique et liturgie, pré et post Vatican II.

Scope : codes de droit canonique (Corpus Iuris Canonici médiéval, CIC 1917,
CIC 1983, CCEO 1990) et corpus liturgique (Missel de saint Pie V / Missale
Romanum 1962, Pontifical romain 1961-62, Missale Romanum de Paul VI 1969,
réformes liturgiques post-V2 : Sacram Liturgiam, Magnum Principium, Summorum
Pontificum, Traditionis Custodes, Liturgiam Authenticam, etc.).

Sources (langue source uniquement — latin) :
- vatican.va/archive pour CIC 1983 (7 livres + index + bulle Sacrae Disciplinae
  Leges) et CCEO 1990 (3 parties + bulle Sacri Canones).
- vatican.va/content pour les motu proprio et constitutions apostoliques récents
  (Paul VI, Benoît XVI, François) en latin.
- documentacatholicaomnia.eu pour les PDF des codes anciens (CIC 1917 Pio-
  Benedictinus, Decretum Ivonis comme représentant médiéval du Corpus Iuris
  Canonici — le Decretum Gratiani proprement dit n'y est disponible qu'en
  fragments, donc archive.org pour le Decretum critique de Friedberg).
- archive.org pour Missale Romanum 1962 et Pontificale Romanum (PDF).
- fromrome.info pour le texte latin de Quo Primum Tempore (1570).

Documents non trouvés en source latine :
- Inter Oecumenici (1964) : vatican.va ne publie pas cet Instruction en latin.
- Tres abhinc annos (1967) : idem.
- Providentissima Mater Ecclesia (1917, bulle de promulgation du CIC 1917) :
  non publiée sur vatican.va ; le PDF DCO du code 1917 contient son texte en
  préface.
"""
from __future__ import annotations

import asyncio
import os
import sys
from datetime import date

from scrapers.core.pipeline import DocRef, MAGISTERIUM_ROOT, run_pipeline

PHASE = "phase-8-droit-liturgie"


# --- Droit canonique pré-V2 -------------------------------------------------

def build_pre_v2_droit() -> list[DocRef]:
    refs: list[DocRef] = []
    base = MAGISTERIUM_ROOT / "A-pre-vatican-ii" / "droit-canonique"

    # Corpus Iuris Canonici médiéval
    cic_medieval = base / "corpus-iuris-canonici"
    # Ivo de Chartres, Decretum (v. 1095) — ancêtre immédiat du Decretum de
    # Gratien, couramment inclus dans les recueils du Corpus Iuris Canonici
    # médiéval. DCO ne publie qu'un fragment du Decretum Gratiani (manuscrit
    # de Cologne), nous prenons donc l'Ivonis Decretum qui est complet.
    refs.append(DocRef(
        url="https://www.documentacatholicaomnia.eu/03d/1040-1116,_Ivo_Carnotensis,_Decretum,_LT.pdf",
        target_dir=cic_medieval,
        slug="1095_ivonis-carnotensis-decretum_decret",
        lang="la",
        meta_hints={
            "incipit": "Ivonis Carnotensis Decretum",
            "titre_fr": "Décret d'Yves de Chartres (précurseur du Corpus Iuris Canonici)",
            "auteur": "Yves de Chartres",
            "periode": "pre-vatican-ii",
            "type": "code-canonique",
            "date": date(1095, 1, 1),
            "autorite_magisterielle": "magistere-ordinaire-universel",
            "langue_originale": "la",
            "langues_disponibles": ["la"],
            "sujets": ["droit-canonique"],
        },
    ))
    # Gratianus — excerpta manuscrit de Cologne. Le Decretum complet de Gratien
    # dans l'édition critique de Friedberg n'est pas directement téléchargeable
    # en un seul fichier HTML ; nous archivons au minimum cette notice/extrait
    # pour trace.
    refs.append(DocRef(
        url="https://www.documentacatholicaomnia.eu/04z/z_1140-1140__Gratianus_Ioannes__Decretum_Gratiani_%5BExcerpta._Dom_Handschrift_127_Koeln%5D__LT.doc.html",
        target_dir=cic_medieval,
        slug="1140_gratiani-decretum-excerpta_decret",
        lang="la",
        meta_hints={
            "incipit": "Decretum Magistri Gratiani (excerpta, Ms. Köln 127)",
            "titre_fr": "Décret de Gratien (fragments, Corpus Iuris Canonici partie I)",
            "auteur": "Gratien",
            "periode": "pre-vatican-ii",
            "type": "code-canonique",
            "date": date(1140, 1, 1),
            "autorite_magisterielle": "magistere-ordinaire-universel",
            "langue_originale": "la",
            "langues_disponibles": ["la"],
            "sujets": ["droit-canonique"],
        },
    ))

    # CIC 1917 (Codex Pio-Benedictino) — PDF latin complet sur DCO, incluant
    # en préface la bulle Providentissima Mater Ecclesia de Benoît XV.
    cic_1917 = base / "1917-cic-pio-benedictino"
    refs.append(DocRef(
        url="https://www.documentacatholicaomnia.eu/03d/1917-1917,_Absens,_Codex_Iuris_Canonici,_LT.pdf",
        target_dir=cic_1917,
        slug="1917-05-27_codex-iuris-canonici-pio-benedictinus_code",
        lang="la",
        meta_hints={
            "incipit": "Codex Iuris Canonici Pii X iussu digestus, Benedicti XV auctoritate promulgatus",
            "titre_fr": "Code de droit canonique de 1917 (Pio-Benedictinus)",
            "auteur": "Pie X+Benoît XV",
            "periode": "pre-vatican-ii",
            "type": "code-canonique",
            "date": date(1917, 5, 27),
            "autorite_magisterielle": "magistere-ordinaire-universel",
            "langue_originale": "la",
            "langues_disponibles": ["la"],
            "sujets": ["droit-canonique"],
        },
    ))
    # Motu proprio Cum Iuris Canonici (15 sept. 1917) — institue la commission
    # d'interprétation authentique du CIC.
    refs.append(DocRef(
        url="https://www.vatican.va/content/benedict-xv/la/motu_proprio/documents/hf_ben-xv_motu-proprio_19170915_cum-iuris-canonici.html",
        target_dir=cic_1917,
        slug="1917-09-15_cum-iuris-canonici_mp",
        lang="la",
        meta_hints={
            "incipit": "Cum Iuris Canonici",
            "titre_fr": "Motu proprio instituant la commission d'interprétation authentique du CIC",
            "auteur": "Benoît XV",
            "periode": "pre-vatican-ii",
            "type": "motu-proprio",
            "date": date(1917, 9, 15),
            "autorite_magisterielle": "magistere-ordinaire-universel",
            "langue_originale": "la",
            "langues_disponibles": ["la"],
            "sujets": ["droit-canonique"],
        },
    ))

    return refs


# --- Droit canonique post-V2 ------------------------------------------------

def build_post_v2_droit() -> list[DocRef]:
    refs: list[DocRef] = []
    base = MAGISTERIUM_ROOT / "C-post-vatican-ii" / "droit-canonique"

    # CIC 1983 : bulle de promulgation + index + 7 livres
    cic_1983 = base / "1983-cic"
    # Bulle Sacrae Disciplinae Leges (25 janv. 1983)
    refs.append(DocRef(
        url="https://www.vatican.va/content/john-paul-ii/la/apost_constitutions/documents/hf_jp-ii_apc_25011983_sacrae-disciplinae-leges.html",
        target_dir=cic_1983,
        slug="1983-01-25_sacrae-disciplinae-leges_const",
        lang="la",
        meta_hints={
            "incipit": "Sacrae Disciplinae Leges",
            "titre_fr": "Constitution apostolique de promulgation du Code de droit canonique de 1983",
            "auteur": "Jean-Paul II",
            "periode": "post-vatican-ii",
            "type": "constitution-apostolique",
            "date": date(1983, 1, 25),
            "autorite_magisterielle": "magistere-ordinaire-universel",
            "langue_originale": "la",
            "langues_disponibles": ["la"],
            "sujets": ["droit-canonique"],
        },
    ))
    # Index
    refs.append(DocRef(
        url="https://www.vatican.va/archive/cod-iuris-canonici/cic_index_la.html",
        target_dir=cic_1983,
        slug="1983-01-25_codex-iuris-canonici-index_code",
        lang="la",
        body_selector="#corpo",
        unwrap_tags=["table", "tbody", "tr", "td", "font"],
        meta_hints={
            "incipit": "Codex Iuris Canonici — Index",
            "titre_fr": "Code de droit canonique de 1983 — Index général",
            "auteur": "Jean-Paul II",
            "periode": "post-vatican-ii",
            "type": "code-canonique",
            "date": date(1983, 1, 25),
            "autorite_magisterielle": "magistere-ordinaire-universel",
            "langue_originale": "la",
            "langues_disponibles": ["la"],
            "sujets": ["droit-canonique"],
        },
    ))
    # 7 livres — layout vatican.va/archive : contenu dans <div id="corpo">
    # enveloppé d'une table de mise en page qu'il faut désenvelopper (même
    # pattern que les pages hist_councils/ en phase 2).
    libri = [
        ("I",   "De normis generalibus",                 "Normes générales"),
        ("II",  "De populo Dei",                         "Peuple de Dieu"),
        ("III", "De Ecclesiae munere docendi",           "Fonction d'enseignement"),
        ("IV",  "De Ecclesiae munere sanctificandi",     "Fonction de sanctification (sacrements)"),
        ("V",   "De bonis Ecclesiae temporalibus",       "Biens temporels"),
        ("VI",  "De sanctionibus in Ecclesia",           "Sanctions pénales"),
        ("VII", "De processibus",                        "Procès"),
    ]
    roman_to_int = {"I": 1, "II": 2, "III": 3, "IV": 4, "V": 5, "VI": 6, "VII": 7}
    for roman, titre_lat, titre_fr_short in libri:
        n = roman_to_int[roman]
        refs.append(DocRef(
            url=f"https://www.vatican.va/archive/cod-iuris-canonici/latin/documents/cic_liber{roman}_la.html",
            target_dir=cic_1983,
            slug=f"1983-01-25_codex-iuris-canonici-liber-{n:02d}_code",
            lang="la",
            body_selector="#corpo",
            unwrap_tags=["table", "tbody", "tr", "td", "font"],
            meta_hints={
                "incipit": f"Codex Iuris Canonici — Liber {roman} ({titre_lat})",
                "titre_fr": f"CIC 1983 — Livre {roman} : {titre_fr_short}",
                "auteur": "Jean-Paul II",
                "periode": "post-vatican-ii",
                "type": "code-canonique",
                "date": date(1983, 1, 25),
                "autorite_magisterielle": "magistere-ordinaire-universel",
                "langue_originale": "la",
                "langues_disponibles": ["la"],
                "sujets": ["droit-canonique"],
            },
        ))

    # CCEO 1990
    cceo = base / "1990-cceo"
    # Bulle Sacri Canones (18 oct. 1990)
    refs.append(DocRef(
        url="https://www.vatican.va/content/john-paul-ii/la/apost_constitutions/documents/hf_jp-ii_apc_19901018_sacri-canones.html",
        target_dir=cceo,
        slug="1990-10-18_sacri-canones_const",
        lang="la",
        meta_hints={
            "incipit": "Sacri Canones",
            "titre_fr": "Constitution apostolique de promulgation du Code des Églises orientales",
            "auteur": "Jean-Paul II",
            "periode": "post-vatican-ii",
            "type": "constitution-apostolique",
            "date": date(1990, 10, 18),
            "autorite_magisterielle": "magistere-ordinaire-universel",
            "langue_originale": "la",
            "langues_disponibles": ["la"],
            "sujets": ["droit-canonique"],
        },
    ))
    # 3 parties du CCEO
    for part in (1, 2, 3):
        refs.append(DocRef(
            url=f"https://www.vatican.va/content/john-paul-ii/la/apost_constitutions/documents/hf_jp-ii_apc_19901018_codex-can-eccl-orient-{part}.html",
            target_dir=cceo,
            slug=f"1990-10-18_codex-canonum-ecclesiarum-orientalium-{part}_code",
            lang="la",
            meta_hints={
                "incipit": f"Codex Canonum Ecclesiarum Orientalium — Pars {part}",
                "titre_fr": f"Code des Églises orientales (CCEO) — Partie {part}/3",
                "auteur": "Jean-Paul II",
                "periode": "post-vatican-ii",
                "type": "code-canonique",
                "date": date(1990, 10, 18),
                "autorite_magisterielle": "magistere-ordinaire-universel",
                "langue_originale": "la",
                "langues_disponibles": ["la"],
                "sujets": ["droit-canonique"],
            },
        ))

    return refs


# --- Liturgie pré-V2 --------------------------------------------------------

def build_pre_v2_liturgie() -> list[DocRef]:
    refs: list[DocRef] = []
    base = MAGISTERIUM_ROOT / "A-pre-vatican-ii" / "liturgie"

    # Missel de saint Pie V — Bulle Quo Primum Tempore + Missale Romanum 1962
    # (editio typica de la lignée tridentine).
    missel_pie_v = base / "1570-missel-pie-v"
    refs.append(DocRef(
        url="https://www.fromrome.info/2023/02/10/quo-primum-st-pius-vs-1570-bull-on-the-roman-missal-latin-and-english-text/",
        target_dir=missel_pie_v,
        slug="1570-07-14_quo-primum-tempore_const",
        lang="la",
        # Page comporte texte latin + traduction anglaise ; la traduction
        # anglaise est mêlée, on prend toute la zone article et on filtrera en
        # post-traitement si besoin.
        body_selector="article, main, div.entry-content",
        meta_hints={
            "incipit": "Quo Primum Tempore",
            "titre_fr": "Bulle Quo Primum Tempore, promulgation du Missel romain",
            "auteur": "Pie V",
            "periode": "pre-vatican-ii",
            "type": "constitution-apostolique",
            "date": date(1570, 7, 14),
            "autorite_magisterielle": "magistere-ordinaire-universel",
            "langue_originale": "la",
            "langues_disponibles": ["la"],
            "sujets": ["liturgie"],
        },
    ))
    # Missale Romanum 1962 — PDF (editio typica tridentine dernière édition
    # avant Vatican II, représente fidèlement le missel de Pie V dans sa
    # transmission jusqu'en 1962). 340 Mo.
    refs.append(DocRef(
        url="https://archive.org/download/missale-romanum-1962/Missale%20Romanum%201962.pdf",
        target_dir=missel_pie_v,
        slug="1962-06-23_missale-romanum-editio-typica_missel",
        lang="la",
        meta_hints={
            "incipit": "Missale Romanum (editio typica 1962)",
            "titre_fr": "Missel romain — édition typique 1962 (dernière édition tridentine)",
            "auteur": "Pie V",
            "periode": "pre-vatican-ii",
            "type": "missel",
            "date": date(1962, 6, 23),
            "autorite_magisterielle": "magistere-ordinaire-universel",
            "langue_originale": "la",
            "langues_disponibles": ["la"],
            "sujets": ["liturgie"],
        },
    ))

    # Pontifical romain (édition typique 1961-62, promulguée sous Pie XII puis
    # Jean XXIII). PDF sur archive.org.
    pontifical = base / "1955-pontifical-romain"
    refs.append(DocRef(
        url="https://archive.org/download/PontificaleRomanum/PontificaleRomanum.pdf",
        target_dir=pontifical,
        slug="1961-04-13_pontificale-romanum_pontifical",
        lang="la",
        meta_hints={
            "incipit": "Pontificale Romanum",
            "titre_fr": "Pontifical romain (édition typique 1961-1962)",
            "auteur": "Jean XXIII",
            "periode": "pre-vatican-ii",
            "type": "pontifical",
            "date": date(1961, 4, 13),
            "autorite_magisterielle": "magistere-ordinaire-universel",
            "langue_originale": "la",
            "langues_disponibles": ["la"],
            "sujets": ["liturgie"],
        },
    ))

    return refs


# --- Liturgie post-V2 -------------------------------------------------------

def build_post_v2_liturgie() -> list[DocRef]:
    refs: list[DocRef] = []
    base = MAGISTERIUM_ROOT / "C-post-vatican-ii" / "liturgie"

    # Missel de Paul VI (1969) — Constitution Missale Romanum. Le texte même
    # du missel n'est pas publié en ligne comme document unifié par la Santa
    # Sede ; on archive la constitution promulgatoire.
    missel_pvi = base / "1969-missel-paul-vi"
    refs.append(DocRef(
        url="https://www.vatican.va/content/paul-vi/la/apost_constitutions/documents/hf_p-vi_apc_19690403_missale-romanum.html",
        target_dir=missel_pvi,
        slug="1969-04-03_missale-romanum_const",
        lang="la",
        meta_hints={
            "incipit": "Missale Romanum",
            "titre_fr": "Constitution apostolique de promulgation du Missel romain rénové",
            "auteur": "Paul VI",
            "periode": "post-vatican-ii",
            "type": "constitution-apostolique",
            "date": date(1969, 4, 3),
            "autorite_magisterielle": "magistere-ordinaire-universel",
            "langue_originale": "la",
            "langues_disponibles": ["la"],
            "sujets": ["liturgie"],
        },
    ))

    # Réformes liturgiques post-V2
    reformes = base / "reformes-liturgiques"

    # (url, slug, incipit, titre_fr, auteur, date_iso, type_meta, type_short)
    reforme_docs: list[tuple[str, str, str, str, str, str, str, str]] = [
        (
            "https://www.vatican.va/content/paul-vi/la/motu_proprio/documents/hf_p-vi_motu-proprio_19640125_sacram-liturgiam.html",
            "sacram-liturgiam", "Sacram Liturgiam",
            "Motu proprio mettant en vigueur certaines dispositions de Sacrosanctum Concilium",
            "Paul VI", "1964-01-25", "motu-proprio", "mp",
        ),
        (
            "https://www.vatican.va/roman_curia/congregations/ccdds/documents/rc_con_ccdds_doc_20010507_liturgiam-authenticam_lt.html",
            "liturgiam-authenticam", "Liturgiam Authenticam",
            "Instruction sur l'usage des langues vernaculaires dans la liturgie romaine",
            "Jean-Paul II", "2001-03-28", "instruction-liturgique", "instr",
        ),
        (
            "https://www.vatican.va/content/benedict-xvi/la/motu_proprio/documents/hf_ben-xvi_motu-proprio_20070707_summorum-pontificum.html",
            "summorum-pontificum", "Summorum Pontificum",
            "Motu proprio sur la liturgie romaine antérieure à la réforme de 1970",
            "Benoît XVI", "2007-07-07", "motu-proprio", "mp",
        ),
        (
            "https://www.vatican.va/content/francesco/la/motu_proprio/documents/papa-francesco-motu-proprio_20170903_magnum-principium.html",
            "magnum-principium", "Magnum Principium",
            "Motu proprio modifiant le can. 838 (compétences sur les traductions liturgiques)",
            "François", "2017-09-03", "motu-proprio", "mp",
        ),
        (
            "https://www.vatican.va/content/francesco/la/motu_proprio/documents/20210716-motu-proprio-traditionis-custodes.html",
            "traditionis-custodes", "Traditionis Custodes",
            "Motu proprio sur l'usage de la liturgie romaine antérieure à la réforme de 1970",
            "François", "2021-07-16", "motu-proprio", "mp",
        ),
    ]

    for url, short_slug, incipit, titre_fr, auteur, date_iso, type_meta, type_short in reforme_docs:
        slug = f"{date_iso}_{short_slug}_{type_short}"
        refs.append(DocRef(
            url=url,
            target_dir=reformes,
            slug=slug,
            lang="la",
            meta_hints={
                "incipit": incipit,
                "titre_fr": titre_fr,
                "auteur": auteur,
                "periode": "post-vatican-ii",
                "type": type_meta,
                "date": date.fromisoformat(date_iso),
                "autorite_magisterielle": "magistere-ordinaire-universel",
                "langue_originale": "la",
                "langues_disponibles": ["la"],
                "sujets": ["liturgie"],
            },
        ))

    return refs


# --- main -------------------------------------------------------------------

def build_refs() -> list[DocRef]:
    return (
        build_pre_v2_droit()
        + build_post_v2_droit()
        + build_pre_v2_liturgie()
        + build_post_v2_liturgie()
    )


async def main() -> int:
    refresh = os.environ.get("REFRESH") == "1"
    refs = build_refs()
    print(f"Phase 8 — {len(refs)} documents (droit canonique + liturgie)")
    buckets: dict[str, int] = {}
    for r in refs:
        buckets[r.target_dir.name] = buckets.get(r.target_dir.name, 0) + 1
    for b, n in sorted(buckets.items()):
        print(f"  bucket {b}: {n} docs")

    result = await run_pipeline(refs, phase=PHASE, refresh=refresh, concurrency=8)
    print(
        f"Phase 8 done: ok={result.n_ok} skipped={result.n_skipped} "
        f"errors={result.n_errors}"
    )
    return 0 if result.n_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
