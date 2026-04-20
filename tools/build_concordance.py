"""Build magisterium/_metadata/concordance.jsonl, one line per theme.

Classification is heuristic: regex against incipit/titre_fr + known slug
substrings + sujets/themes_doctrinaux/path keywords. A document may match
several themes, or none.

FSSPX documents are funneled into ``post_v2`` (they are a post-V2 reaction).

Run: ``uv run python -m tools.build_concordance``.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "magisterium"
INDEX = CORPUS / "_metadata" / "index.jsonl"
OUT = CORPUS / "_metadata" / "concordance.jsonl"


# ---------------------------------------------------------------------------
# Theme definitions.
#
# Each theme has:
#   keywords      : regex (case-insensitive) applied to titre_fr/incipit/path
#                   (also probed against sujets/themes_doctrinaux strings).
#   known_slugs   : substrings; a match on the slug triggers inclusion.
# ---------------------------------------------------------------------------
THEMES: dict[str, dict[str, list[str]]] = {
    "liberte-religieuse": {
        "keywords": [
            r"libert[ée]\s+religieuse",
            r"libert[ée]\s+de\s+conscience",
            r"libert[ée]s?\s+modernes",
            r"tol[ée]rance\s+religieuse",
            r"religious\s+liberty",
            r"religious\s+freedom",
        ],
        "known_slugs": [
            "mirari-vos",
            "quanta-cura",
            "syllabus",
            "libertas",
            "immortale-dei",
            "dignitatis-humanae",
            "dignitatis_humanae",
            "singulari-nos",
        ],
    },
    "oecumenisme-relations-autres-religions": {
        "keywords": [
            r"oecum[eé]nis",
            r"œcum[eé]nis",
            r"unit[ée]\s+des\s+chr[eé]tiens",
            r"dialogue\s+inter",
            r"autres\s+religions",
            r"non[- ]chr[eé]tiens",
            r"juda[ïi]sme",
            r"islam",
            r"ecumen",
        ],
        "known_slugs": [
            "mortalium-animos",
            "humani-generis",
            "unitatis-redintegratio",
            "nostra-aetate",
            "dominus-iesus",
            "ut-unum-sint",
            "orientalium-ecclesiarum",
            "orientalium_ecclesiarum",
            "ecclesiam-suam",
        ],
    },
    "liturgie-messe": {
        "keywords": [
            r"liturgi",
            r"missel",
            r"sainte\s+messe",
            r"rite\s+romain",
            r"messe\s+tridentine",
            r"forme\s+extraordinaire",
            r"sacrosanctum",
            r"eucharisti",
        ],
        "known_slugs": [
            "quo-primum",
            "quo_primum",
            "mediator-dei",
            "sacrosanctum-concilium",
            "sacrosanctum_concilium",
            "missale-romanum",
            "missale_romanum",
            "traditionis-custodes",
            "traditionis_custodes",
            "summorum-pontificum",
            "summorum_pontificum",
            "sacram-liturgiam",
            "ecclesia-de-eucharistia",
            "redemptionis-sacramentum",
            "memoriale-domini",
            "musicam-sacram",
            "inter-oecumenici",
        ],
    },
    "ecclesiologie-salut-hors-eglise": {
        "keywords": [
            r"eccl[eé]siologi",
            r"corps\s+mystique",
            r"hors\s+de\s+l[’']?[eé]glise",
            r"extra\s+ecclesiam",
            r"subsist(it|e)\s+in",
            r"unique\s+[eé]glise\s+du\s+christ",
        ],
        "known_slugs": [
            "lumen-gentium",
            "lumen_gentium",
            "mystici-corporis",
            "satis-cognitum",
            "dominus-iesus",
            "unam-sanctam",
            "cantate-domino",
        ],
    },
    "rapport-eglise-etat": {
        "keywords": [
            r"[eé]glise\s+et\s+[eé]tat",
            r"pouvoir\s+civil",
            r"soci[eé]t[eé]\s+civile",
            r"royaut[eé]\s+(sociale|du\s+christ)",
            r"autorit[eé]\s+politique",
            r"s[eé]paration\s+de\s+l[’']?[eé]glise",
        ],
        "known_slugs": [
            "immortale-dei",
            "diuturnum-illud",
            "diuturnum_illud",
            "quas-primas",
            "gaudium-et-spes",
            "gaudium_et_spes",
            "sapientiae-christianae",
            "cum-multa",
            "au-milieu-des-sollicitudes",
            "vehementer-nos",
        ],
    },
    "magistere-infaillibilite": {
        "keywords": [
            r"infaillibilit[eé]",
            r"magist[eè]re",
            r"assentiment",
            r"professio\s+fidei",
            r"ex\s+cathedra",
            r"pastor\s+aeternus",
        ],
        "known_slugs": [
            "pastor-aeternus",
            "pastor_aeternus",
            "humani-generis",
            "ad-tuendam-fidem",
            "donum-veritatis",
            "mysterium-ecclesiae",
            "professio-fidei",
            "tuas-libenter",
        ],
    },
    "morale-sexuelle-mariage": {
        "keywords": [
            r"mariage",
            r"matrimoni",
            r"contraception",
            r"r[eé]gulation\s+des\s+naissances",
            r"avortement",
            r"famille\s+chr[eé]tienne",
            r"morale\s+conjugale",
            r"chastet[eé]",
            r"divorc",
        ],
        "known_slugs": [
            "casti-connubii",
            "casti_connubii",
            "humanae-vitae",
            "humanae_vitae",
            "familiaris-consortio",
            "persona-humana",
            "amoris-laetitia",
            "veritatis-splendor",
            "evangelium-vitae",
            "arcanum-divinae",
            "donum-vitae",
            "deus-caritas-est",
        ],
    },
    "doctrine-sociale": {
        "keywords": [
            r"doctrine\s+sociale",
            r"question\s+sociale",
            r"question\s+ouvri[eè]re",
            r"travail(leurs)?",
            r"justice\s+sociale",
            r"[eé]cologie\s+int[eé]grale",
            r"d[eé]veloppement\s+(humain|int[eé]gral|des\s+peuples)",
        ],
        "known_slugs": [
            "rerum-novarum",
            "rerum_novarum",
            "quadragesimo-anno",
            "quadragesimo_anno",
            "populorum-progressio",
            "laborem-exercens",
            "centesimus-annus",
            "caritas-in-veritate",
            "laudato-si",
            "laudato_si",
            "fratelli-tutti",
            "mater-et-magistra",
            "pacem-in-terris",
            "sollicitudo-rei-socialis",
            "octogesima-adveniens",
        ],
    },
    "modernisme": {
        "keywords": [
            r"modernism",
            r"moderniste",
            r"nouvelle\s+th[eé]ologie",
            r"relativisme\s+doctrinal",
        ],
        "known_slugs": [
            "pascendi",
            "lamentabili",
            "sacrorum-antistitum",
            "syllabus",
            "humani-generis",
            "notre-charge-apostolique",
            "qui-pluribus",
        ],
    },
    "collegialite-primaute": {
        "keywords": [
            r"coll[eé]gialit[eé]",
            r"primaut[eé]\s+(pontificale|romaine|de\s+pierre)",
            r"synode\s+des\s+[eé]v[eê]ques",
            r"conf[eé]rences?\s+[eé]piscopales?",
        ],
        "known_slugs": [
            "pastor-aeternus",
            "pastor_aeternus",
            "lumen-gentium",
            "christus-dominus",
            "apostolos-suos",
            "episcopalis-communio",
            "praedicate-evangelium",
        ],
    },
    "sacerdoce-celibat": {
        "keywords": [
            r"sacerdoce",
            r"pr[eê]tre",
            r"c[eé]libat\s+(eccl[eé]siastique|sacerdotal)",
            r"formation\s+des\s+pr[eê]tres",
            r"presbyt[eé]ral",
        ],
        "known_slugs": [
            "ad-catholici-sacerdotii",
            "sacerdotalis-caelibatus",
            "presbyterorum-ordinis",
            "presbyterorum_ordinis",
            "pastores-dabo-vobis",
            "optatam-totius",
            "querida-amazonia",
        ],
    },
}


def _compile(patterns: list[str]) -> list[re.Pattern]:
    return [re.compile(p, re.IGNORECASE) for p in patterns]


_COMPILED = {name: _compile(defn["keywords"]) for name, defn in THEMES.items()}


def classify(meta: dict) -> list[str]:
    """Return the list of themes matched by this index entry."""
    slug = (meta.get("slug") or "").lower()
    path = (meta.get("path") or "").lower()
    incipit = (meta.get("incipit") or "").lower()
    titre = (meta.get("titre_fr") or "").lower()
    auteur = (meta.get("auteur") or "").lower()
    sujets = " ".join(str(s).lower() for s in (meta.get("sujets") or []))
    themes_doc = " ".join(str(s).lower() for s in (meta.get("themes_doctrinaux") or []))

    haystack_text = " \u241f ".join([incipit, titre, sujets, themes_doc, path, auteur])

    out: list[str] = []
    for theme, defn in THEMES.items():
        hit = False
        for needle in defn["known_slugs"]:
            n = needle.lower()
            if n in slug or n in path:
                hit = True
                break
        if not hit:
            for rx in _COMPILED[theme]:
                if rx.search(haystack_text):
                    hit = True
                    break
        if hit:
            out.append(theme)
    return out


def _bucket(meta: dict) -> str:
    p = meta.get("periode")
    if p == "pre-vatican-ii":
        return "pre_v2"
    if p == "vatican-ii":
        return "v2"
    # post-vatican-ii + fsspx both land in post_v2 per spec
    return "post_v2"


def build_concordance() -> int:
    if not INDEX.exists():
        print(f"[build_concordance] index missing: {INDEX}", file=sys.stderr)
        print("  run `just build-index` first", file=sys.stderr)
        return 1

    buckets: dict[str, dict[str, list[str]]] = {
        theme: {"pre_v2": [], "v2": [], "post_v2": []} for theme in THEMES
    }

    with INDEX.open(encoding="utf-8") as fh:
        for raw in fh:
            raw = raw.strip()
            if not raw:
                continue
            entry = json.loads(raw)
            themes = classify(entry)
            bucket = _bucket(entry)
            slug = entry.get("slug")
            if not slug:
                continue
            for theme in themes:
                buckets[theme][bucket].append(slug)

    # deterministic ordering within each bucket
    for theme, by_bucket in buckets.items():
        for bucket in by_bucket:
            by_bucket[bucket] = sorted(set(by_bucket[bucket]))

    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", encoding="utf-8") as fh:
        for theme in THEMES:
            rec = {"theme": theme, **buckets[theme]}
            fh.write(json.dumps(rec, ensure_ascii=False) + "\n")

    total_matches = sum(
        len(b["pre_v2"]) + len(b["v2"]) + len(b["post_v2"]) for b in buckets.values()
    )
    print(
        f"[build_concordance] wrote {len(THEMES)} themes "
        f"({total_matches} theme-document pairings) to {OUT.relative_to(ROOT)}"
    )
    return 0


def main() -> int:
    return build_concordance()


if __name__ == "__main__":
    sys.exit(main())
