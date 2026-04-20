"""Phase 5 — Solemn magisterial documents of popes before Pie IX (pre-1846).

Scope: hand-curated doctrinally-important documents from Gregory XVI (1831-1846)
back to Leo X (*Exsurge Domine*, 1520). Language: essentially all documents are
served in English translation by papalencyclicals.net; Vatican.va coverage for
this period is negligible and documentacatholicaomnia.eu mostly offers Italian
downloadable files (.doc) rather than HTML. We therefore use papalencyclicals
(lang=en) as the primary source, noting `langue_originale="la"` in metadata.

Naming:
    slug        = YYYY-MM-DD_{incipit-slug}_{type-short}
    target_dir  = magisterium/A-pre-vatican-ii/papes/{YYYY}-{nom-pape}/{type-plural}
"""
from __future__ import annotations

import asyncio
import os
import sys
from dataclasses import dataclass, field
from datetime import date
from typing import Optional

from scrapers.core.pipeline import DocRef, MAGISTERIUM_ROOT, run_pipeline

PHASE = "phase-5-papes-pre-1846"
PAPAL_BASE = "https://www.papalencyclicals.net"


# --- Data model --------------------------------------------------------------

TYPE_SHORT = {
    "encyclique": "enc",
    "bulle": "bulle",
    "constitution-apostolique": "const",
    "breve": "breve",
    "motu-proprio": "mp",
    "lettre-apostolique": "apl",
}

TYPE_FOLDER = {
    "encyclique": "encycliques",
    "bulle": "bulles",
    "constitution-apostolique": "constitutions-apostoliques",
    "breve": "breves",
    "motu-proprio": "motu-proprio",
    "lettre-apostolique": "lettres-apostoliques",
}


@dataclass(frozen=True)
class Pope:
    folder: str             # target dir name under papes/
    auteur: str             # meta.auteur value


@dataclass(frozen=True)
class Doc:
    pope: Pope
    date_iso: str           # YYYY-MM-DD
    incipit: str            # incipit (latin)
    titre_fr: Optional[str] # short french summary of subject
    slug_incipit: str       # slug fragment (kebab-case of incipit)
    url: str                # absolute URL
    type_meta: str          # one of TYPE_SHORT keys
    lang: str = "en"        # language served on the page
    autorite: Optional[str] = None  # override if needed
    sujets: tuple[str, ...] = ()


# --- Pope registry -----------------------------------------------------------

P_GREG16  = Pope("1831-gregoire-xvi",  "Grégoire XVI")
P_PIUS8   = Pope("1829-pie-viii",      "Pie VIII")
P_LEO12   = Pope("1823-leon-xii",      "Léon XII")
P_PIUS7   = Pope("1800-pie-vii",       "Pie VII")
P_PIUS6   = Pope("1775-pie-vi",        "Pie VI")
P_CLEM14  = Pope("1769-clement-xiv",   "Clément XIV")
P_CLEM13  = Pope("1758-clement-xiii",  "Clément XIII")
P_BEN14   = Pope("1740-benoit-xiv",    "Benoît XIV")
P_CLEM12  = Pope("1730-clement-xii",   "Clément XII")
P_CLEM11  = Pope("1700-clement-xi",    "Clément XI")
P_INN11   = Pope("1676-innocent-xi",   "Innocent XI")
P_ALEX7   = Pope("1655-alexandre-vii", "Alexandre VII")
P_CLEM8   = Pope("1592-clement-viii",  "Clément VIII")
P_SIX5    = Pope("1585-sixte-v",       "Sixte V")
P_PIUS5   = Pope("1566-pie-v",         "Pie V")
P_PAUL3   = Pope("1534-paul-iii",      "Paul III")
P_LEO10   = Pope("1513-leon-x",        "Léon X")


# --- Document list (hand-curated, doctrinally important) --------------------

DOCUMENTS: list[Doc] = [
    # ============ Grégoire XVI (1831-1846) ============
    Doc(P_GREG16, "1832-08-15", "Mirari Vos",
        "Sur le libéralisme et l'indifférentisme religieux",
        "mirari-vos",
        f"{PAPAL_BASE}/greg16/g16mirar.htm",
        "encyclique",
        sujets=("liberalisme", "indifferentisme", "libertes-modernes")),
    Doc(P_GREG16, "1832-05-27", "Summo Iugiter Studio",
        "Sur les mariages mixtes", "summo-iugiter-studio",
        f"{PAPAL_BASE}/greg16/g16summo.htm",
        "encyclique", sujets=("mariage-mixte",)),
    Doc(P_GREG16, "1832-06-09", "Cum Primum",
        "Sur l'obéissance civile", "cum-primum",
        f"{PAPAL_BASE}/greg16/g16cumpr.htm",
        "encyclique", sujets=("obeissance-civile",)),
    Doc(P_GREG16, "1833-10-04", "Quo Graviora",
        "Sur la Constitution pragmatique", "quo-graviora",
        f"{PAPAL_BASE}/greg16/g16quogr.htm",
        "encyclique"),
    Doc(P_GREG16, "1834-06-25", "Singulari Nos",
        "Sur les erreurs de Lamennais", "singulari-nos",
        f"{PAPAL_BASE}/greg16/g16singu.htm",
        "encyclique", sujets=("lamennais", "liberalisme-catholique")),
    Doc(P_GREG16, "1835-05-17", "Commissum Divinitus",
        "Sur l'Église et l'État", "commissum-divinitus",
        f"{PAPAL_BASE}/greg16/g16commi.htm",
        "encyclique", sujets=("eglise-et-etat",)),
    Doc(P_GREG16, "1839-12-03", "In Supremo Apostolatus",
        "Condamnation de la traite des esclaves", "in-supremo-apostolatus",
        f"{PAPAL_BASE}/greg16/g16sup.htm",
        "lettre-apostolique", sujets=("esclavage",)),
    Doc(P_GREG16, "1840-09-18", "Probe Nostis",
        "Sur la propagation de la foi", "probe-nostis",
        f"{PAPAL_BASE}/greg16/g16probe.htm",
        "encyclique", sujets=("missions",)),
    Doc(P_GREG16, "1841-04-30", "Quas Vestro",
        "Sur les mariages mixtes", "quas-vestro",
        f"{PAPAL_BASE}/greg16/g16quasv.htm",
        "encyclique", sujets=("mariage-mixte",)),
    Doc(P_GREG16, "1844-05-08", "Inter Praecipuas",
        "Sur les sociétés bibliques", "inter-praecipuas",
        f"{PAPAL_BASE}/greg16/g16inter.htm",
        "encyclique", sujets=("societes-bibliques", "ecriture-sainte")),

    # ============ Pie VIII (1829-1830) ============
    Doc(P_PIUS8, "1829-05-24", "Traditi Humilitati",
        "Sur son programme de pontificat", "traditi-humilitati",
        f"{PAPAL_BASE}/pius08/p8tradit.htm",
        "encyclique", sujets=("programme-pontifical", "indifferentisme")),

    # ============ Léon XII (1823-1829) ============
    Doc(P_LEO12, "1824-05-05", "Ubi Primum",
        "Sur l'accession au pontificat", "ubi-primum",
        f"{PAPAL_BASE}/leo12/l12ubipr.htm",
        "encyclique", sujets=("programme-pontifical", "indifferentisme")),
    Doc(P_LEO12, "1824-05-24", "Quod Hoc Ineunte",
        "Proclamation du Jubilé universel", "quod-hoc-ineunte",
        f"{PAPAL_BASE}/leo12/l12quodh.htm",
        "encyclique", sujets=("jubile",)),
    Doc(P_LEO12, "1825-12-25", "Charitate Christi",
        "Extension du Jubilé à toute l'Église", "charitate-christi",
        f"{PAPAL_BASE}/leo12/l12chari.htm",
        "encyclique", sujets=("jubile",)),
    Doc(P_LEO12, "1826-03-13", "Quo Graviora",
        "Sur les sociétés secrètes", "quo-graviora",
        f"{PAPAL_BASE}/leo12/l12quogr.htm",
        "constitution-apostolique",
        sujets=("societes-secretes", "franc-maconnerie")),

    # ============ Pie VII (1800-1823) ============
    Doc(P_PIUS7, "1800-05-15", "Diu Satis",
        "Sur le retour aux principes de l'Évangile", "diu-satis",
        f"{PAPAL_BASE}/pius07/p7diusat.htm",
        "encyclique", sujets=("programme-pontifical", "revolution-francaise")),

    # ============ Pie VI (1775-1799) ============
    Doc(P_PIUS6, "1775-12-25", "Inscrutabile",
        "Sur les problèmes du pontificat", "inscrutabile",
        f"{PAPAL_BASE}/pius06/p6inscru.htm",
        "encyclique", sujets=("philosophes", "lumieres")),
    Doc(P_PIUS6, "1791-04-13", "Charitas",
        "Sur la Constitution civile du clergé", "charitas",
        f"{PAPAL_BASE}/pius06/p6charit.htm",
        "encyclique",
        sujets=("constitution-civile-clerge", "revolution-francaise")),
    # NOTE: Auctorem Fidei (1794) n'est pas disponible sur papalencyclicals
    # (URL renvoie 404). Impossible à inclure sans source alternative fiable.

    # ============ Clément XIV (1769-1774) ============
    Doc(P_CLEM14, "1769-09-12", "Decet Quam Maxime",
        "Sur les abus en matière de taxes et bénéfices", "decet-quam-maxime",
        f"{PAPAL_BASE}/clem14/c14decet.htm",
        "encyclique", sujets=("benefices", "clerge")),
    Doc(P_CLEM14, "1769-12-12", "Cum Summi",
        "Proclamation du Jubilé universel", "cum-summi",
        f"{PAPAL_BASE}/clem14/c14cumsu.htm",
        "encyclique", sujets=("jubile",)),
    Doc(P_CLEM14, "1769-12-12", "Inscrutabili Divinae Sapientiae",
        "Proclamation du Jubilé universel",
        "inscrutabili-divinae-sapientiae",
        f"{PAPAL_BASE}/clem14/c14inscr.htm",
        "encyclique", sujets=("jubile",)),
    Doc(P_CLEM14, "1774-04-30", "Salutis Nostrae",
        "Proclamation du Jubilé universel", "salutis-nostrae",
        f"{PAPAL_BASE}/clem14/c14salut.htm",
        "encyclique", sujets=("jubile",)),

    # ============ Clément XIII (1758-1769) ============
    Doc(P_CLEM13, "1758-09-13", "A Quo Die",
        "Sur l'unité de l'Église", "a-quo-die",
        f"{PAPAL_BASE}/clem13/c13aquod.htm",
        "encyclique", sujets=("unite-eglise",)),
    Doc(P_CLEM13, "1759-09-17", "Cum Primum",
        "Sur l'accession au pontificat", "cum-primum",
        f"{PAPAL_BASE}/clem13/c13cumpr.htm",
        "encyclique", sujets=("programme-pontifical",)),
    Doc(P_CLEM13, "1759-12-20", "Appetente Sacro",
        "Sur le jeûne", "appetente-sacro",
        f"{PAPAL_BASE}/clem13/c13appet.htm",
        "encyclique", sujets=("jeune", "penitence")),
    Doc(P_CLEM13, "1761-06-14", "In Dominico Agro",
        "Sur la doctrine chrétienne", "in-dominico-agro",
        f"{PAPAL_BASE}/clem13/c13indom.htm",
        "encyclique", sujets=("catechese", "doctrine")),
    Doc(P_CLEM13, "1766-11-25", "Christianae Reipublicae",
        "Sur le danger des écrits anti-chrétiens",
        "christianae-reipublicae",
        f"{PAPAL_BASE}/clem13/c13chris.htm",
        "encyclique", sujets=("lumieres", "philosophes", "livres-dangereux")),
    Doc(P_CLEM13, "1768-01-06", "Summa Quae",
        "Sur les troubles de l'Église", "summa-quae",
        f"{PAPAL_BASE}/clem13/c13summa.htm",
        "encyclique", sujets=("gallicanisme",)),

    # ============ Benoît XIV (1740-1758) ============
    Doc(P_BEN14, "1740-12-03", "Ubi Primum",
        "Sur les devoirs des évêques", "ubi-primum",
        f"{PAPAL_BASE}/ben14/b14ubipr.htm",
        "encyclique", sujets=("episcopat",)),
    Doc(P_BEN14, "1741-06-30", "Quanta Cura",
        "Sur la cupidité", "quanta-cura",
        f"{PAPAL_BASE}/ben14/b14quant.htm",
        "encyclique", sujets=("cupidite",)),
    Doc(P_BEN14, "1743-05-18", "Nimiam Licentiam",
        "Sur la validité des mariages en Pologne", "nimiam-licentiam",
        f"{PAPAL_BASE}/ben14/b14nimia.htm",
        "encyclique", sujets=("mariage",)),
    Doc(P_BEN14, "1745-11-01", "Vix Pervenit",
        "Sur l'usure et autres profits malhonnêtes", "vix-pervenit",
        f"{PAPAL_BASE}/ben14/b14vixpe.htm",
        "encyclique", sujets=("usure", "morale-economique")),
    Doc(P_BEN14, "1748-06-29", "Magnae Nobis",
        "Sur les empêchements de mariage", "magnae-nobis",
        f"{PAPAL_BASE}/ben14/b14magna.htm",
        "encyclique", sujets=("mariage",)),
    Doc(P_BEN14, "1749-02-19", "Annus Qui Hunc",
        "Sur la musique sacrée et la peinture",
        "annus-qui-hunc",
        f"{PAPAL_BASE}/ben14/annus-qui-hunc.htm",
        "encyclique", sujets=("musique-sacree", "liturgie", "art-sacre")),
    Doc(P_BEN14, "1749-06-26", "Apostolica Constitutio",
        "Proclamation du Jubilé de 1750",
        "apostolica-constitutio",
        f"{PAPAL_BASE}/ben14/b14apost.htm",
        "constitution-apostolique", sujets=("jubile",)),
    Doc(P_BEN14, "1749-05-05", "Peregrinantes",
        "Proclamation du Jubilé de 1750", "peregrinantes",
        f"{PAPAL_BASE}/ben14/b14pereg.htm",
        "encyclique", sujets=("jubile",)),
    Doc(P_BEN14, "1751-06-14", "A Quo Primum",
        "Sur les Juifs et les Chrétiens demeurant dans les mêmes lieux",
        "a-quo-primum",
        f"{PAPAL_BASE}/ben14/b14aquo.htm",
        "encyclique", sujets=("juifs",)),
    Doc(P_BEN14, "1754-06-26", "Cum Religiosi",
        "Sur la catéchèse", "cum-religiosi",
        f"{PAPAL_BASE}/ben14/b14cumre.htm",
        "encyclique", sujets=("catechese",)),
    Doc(P_BEN14, "1755-07-26", "Allatae Sunt",
        "Sur l'observance des rites orientaux", "allatae-sunt",
        f"{PAPAL_BASE}/ben14/b14allat.htm",
        "encyclique", sujets=("eglises-orientales", "rites")),
    Doc(P_BEN14, "1756-03-01", "Ex Quo",
        "Sur les Euchologes", "ex-quo",
        f"{PAPAL_BASE}/ben14/b14exquo.htm",
        "encyclique", sujets=("liturgie", "eglises-orientales")),
    Doc(P_BEN14, "1756-10-16", "Ex Omnibus",
        "Sur les appels de l'Église gallicane",
        "ex-omnibus",
        f"{PAPAL_BASE}/ben14/b14exomn.htm",
        "encyclique", sujets=("gallicanisme", "jansenisme")),

    # ============ Clément XII (1730-1740) ============
    Doc(P_CLEM12, "1738-04-28", "In Eminenti Apostolatus",
        "Condamnation de la franc-maçonnerie",
        "in-eminenti-apostolatus",
        f"{PAPAL_BASE}/clem12/c12inemengl.htm",
        "bulle",
        autorite="magistere-ordinaire-universel",
        sujets=("franc-maconnerie", "societes-secretes")),

    # ============ Clément XI (1700-1721) ============
    Doc(P_CLEM11, "1713-09-08", "Unigenitus Dei Filius",
        "Condamnation des 101 propositions de Quesnel (jansénisme)",
        "unigenitus-dei-filius",
        f"{PAPAL_BASE}/clem11/c11unige.htm",
        "constitution-apostolique",
        autorite="magistere-extraordinaire-constitution-dogmatique",
        sujets=("jansenisme", "quesnel", "grace")),

    # ============ Innocent XI (1676-1689) ============
    Doc(P_INN11, "1679-11-20", "Sollicitudo Pastoralis",
        "Sur les Ordres religieux", "sollicitudo-pastoralis",
        f"{PAPAL_BASE}/innoc11/soll-l.htm",
        "constitution-apostolique", sujets=("ordres-religieux",)),
    Doc(P_INN11, "1687-11-20", "Coelestis Pastor",
        "Condamnation des erreurs de Miguel de Molinos (quiétisme)",
        "coelestis-pastor",
        f"{PAPAL_BASE}/innoc11/i11coel.htm",
        "bulle",
        autorite="magistere-ordinaire-universel",
        sujets=("quietisme", "molinos", "mystique")),

    # ============ Alexandre VII (1655-1667) ============
    Doc(P_ALEX7, "1659-09-09", "Super Cathedram Principis Apostolorum",
        "Établissement de la mission catholique au Vietnam",
        "super-cathedram",
        f"{PAPAL_BASE}/alex07/alex07super.htm",
        "bulle", sujets=("missions", "vietnam")),

    # ============ Clément VIII (1592-1605) ============
    Doc(P_CLEM8, "1595-10-07", "Ex Supernae Dispositionis Arbitrio",
        "Sur l'Union des Ruthènes à Rome (Union de Brest)",
        "ex-supernae-dispositionis",
        f"{PAPAL_BASE}/clem08/clem8exsuper.htm",
        "bulle",
        lang="la",
        sujets=("union-brest", "eglises-orientales")),

    # ============ Sixte V (1585-1590) ============
    Doc(P_SIX5, "1590-03-03", "Triumphantis Hierusalem",
        "Proclamation de saint Bonaventure docteur de l'Église",
        "triumphantis-hierusalem",
        f"{PAPAL_BASE}/sixtus05/triumph.htm",
        "bulle", sujets=("docteurs-eglise", "bonaventure")),

    # ============ Pie V (1566-1572) ============
    Doc(P_PIUS5, "1569-09-17", "Consueverunt Romani",
        "Sur le Rosaire", "consueverunt-romani",
        f"{PAPAL_BASE}/pius05/p5consue.htm",
        "bulle", sujets=("rosaire",)),
    Doc(P_PIUS5, "1570-03-05", "Regnans in Excelsis",
        "Excommunication d'Élisabeth Ire d'Angleterre",
        "regnans-in-excelsis",
        f"{PAPAL_BASE}/pius05/p5regnans.htm",
        "bulle",
        autorite="magistere-ordinaire-universel",
        sujets=("angleterre", "elizabeth-i", "excommunication")),
    Doc(P_PIUS5, "1570-07-14", "Quo Primum Tempore",
        "Promulgation du Missel romain tridentin",
        "quo-primum-tempore",
        f"{PAPAL_BASE}/pius05/p5quopri.htm",
        "bulle",
        autorite="magistere-extraordinaire-constitution-dogmatique",
        sujets=("liturgie", "missel-romain", "trente")),
    Doc(P_PIUS5, "1571-07-25", "Exponi Nobis Nuper",
        "Sur les privilèges missionnaires", "exponi-nobis-nuper",
        f"{PAPAL_BASE}/pius05/p5exponi.htm",
        "bulle", sujets=("missions",)),

    # ============ Paul III (1534-1549) ============
    Doc(P_PAUL3, "1537-05-29", "Sublimis Deus",
        "Sur l'humanité et l'évangélisation des Indiens",
        "sublimis-deus",
        f"{PAPAL_BASE}/paul03/p3subli.htm",
        "bulle",
        autorite="magistere-ordinaire-universel",
        sujets=("esclavage", "indiens-amerique", "evangelisation")),
    Doc(P_PAUL3, "1538-10-29", "In Apostolatus Culmine",
        "Sur la Compagnie de Jésus", "in-apostolatus-culmine",
        f"{PAPAL_BASE}/paul03/p3inapost.htm",
        "bulle",
        lang="la",
        sujets=("jesuites", "ordres-religieux")),

    # ============ Léon X (1513-1521) ============
    Doc(P_LEO10, "1520-06-15", "Exsurge Domine",
        "Condamnation des erreurs de Martin Luther",
        "exsurge-domine",
        f"{PAPAL_BASE}/leo10/l10exdom.htm",
        "bulle",
        autorite="magistere-extraordinaire-constitution-dogmatique",
        sujets=("luther", "reforme-protestante")),
    Doc(P_LEO10, "1521-01-03", "Decet Romanum Pontificem",
        "Bulle d'excommunication de Martin Luther",
        "decet-romanum-pontificem",
        f"{PAPAL_BASE}/leo10/l10decet.htm",
        "bulle",
        autorite="magistere-ordinaire-universel",
        sujets=("luther", "excommunication", "reforme-protestante")),
]


# --- DocRef construction ----------------------------------------------------

def build_refs() -> list[DocRef]:
    refs: list[DocRef] = []
    for d in DOCUMENTS:
        type_short = TYPE_SHORT[d.type_meta]
        type_folder = TYPE_FOLDER[d.type_meta]
        target_dir = (
            MAGISTERIUM_ROOT / "A-pre-vatican-ii" / "papes"
            / d.pope.folder / type_folder
        )
        slug = f"{d.date_iso}_{d.slug_incipit}_{type_short}"

        refs.append(DocRef(
            url=d.url,
            target_dir=target_dir,
            slug=slug,
            lang=d.lang,
            meta_hints={
                "incipit": d.incipit,
                "titre_fr": d.titre_fr,
                "auteur": d.pope.auteur,
                "periode": "pre-vatican-ii",
                "type": d.type_meta,
                "date": date.fromisoformat(d.date_iso),
                "autorite_magisterielle": (
                    d.autorite or "magistere-ordinaire-universel"
                ),
                "langue_originale": "la",
                "langues_disponibles": sorted({d.lang, "la"}),
                "sujets": list(d.sujets),
            },
        ))
    return refs


# --- main -------------------------------------------------------------------

async def main() -> int:
    refresh = os.environ.get("REFRESH") == "1"
    refs = build_refs()
    print(f"Phase 5 — {len(refs)} documents papaux pré-1846")

    # Summary by pope for operator visibility
    from collections import Counter
    per_pope = Counter(d.pope.folder for d in DOCUMENTS)
    for folder, n in sorted(per_pope.items()):
        print(f"  {folder}: {n}")

    result = await run_pipeline(refs, phase=PHASE, refresh=refresh, concurrency=8)
    print(
        f"Phase 5 done: ok={result.n_ok} skipped={result.n_skipped} "
        f"errors={result.n_errors}"
    )
    return 0 if result.n_errors == 0 else 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
