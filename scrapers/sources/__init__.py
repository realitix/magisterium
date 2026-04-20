"""Witness URLs for connectivity testing (Phase 0.5).

Each entry: source label -> (witness_url, description).
Used by scrapers.phases.phase_0_5_connectivity.
"""
from __future__ import annotations

WITNESSES: dict[str, tuple[str, str]] = {
    "vatican.va": (
        "https://www.vatican.va/content/pius-xii/la/encyclicals/documents/"
        "hf_p-xii_enc_12081950_humani-generis.html",
        "Humani Generis (1950) — latin",
    ),
    "documentacatholicaomnia.eu": (
        "https://www.documentacatholicaomnia.eu/"
        "a_1051_Sanctorum_Paparum_Decretalia_Ac_Argumenta_Quae_Pertinent.html",
        "Index chronologique papal — latin",
    ),
    "papalencyclicals.net": (
        "https://www.papalencyclicals.net/pius10/p10pasce.htm",
        "Pascendi Dominici Gregis (1907) — anglais",
    ),
    "laportelatine.org": (
        "https://laportelatine.org/critique-du-concile-vatican-ii",
        "Index critique Vatican II — français (FSSPX)",
    ),
    "salve-regina.com": (
        "https://www.salve-regina.com/index.php?title=Accueil",
        "Page d'accueil MediaWiki — français",
    ),
    "clerus.va": (
        "https://www.clerus.va/it.html",
        "Dicastère pour le clergé — italien",
    ),
    "ewtn.com": (
        "https://www.ewtn.com/catholicism/library/declaration-on-religious-freedom"
        "-dignitatis-humanae-2068",
        "Dignitatis Humanae — anglais",
    ),
    "vatican2voice.org": (
        "http://vatican2voice.org/4basics/sixteen.htm",
        "Index des 16 documents Vatican II — anglais",
    ),
}
