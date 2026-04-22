"""Discover official translations of a scraped document.

For sites like vatican.va, each document is published in multiple languages
under parallel URLs that only differ by a 2-letter suffix before `.html` —
e.g. ``..._lt.html``, ``..._fr.html``, ``..._en.html``, ``..._it.html``,
``..._de.html``, ``..._es.html``, ``..._pt.html``.

This module parses the raw HTML of the scraped originale and returns a list
of ``(lang_code, url)`` pairs pointing to sibling translations on the same
host. The caller is responsible for filtering out languages it already
holds and enqueuing new DocRefs with ``kind="officielle"``.
"""
from __future__ import annotations

import re
from urllib.parse import urljoin, urlparse

from selectolax.parser import HTMLParser

# Mapping des suffixes de nom de fichier vatican.va vers les codes ISO 639-1
# qu'on utilise dans le corpus. `_lt` (latinus) → `la`.
VATICAN_SUFFIX_TO_LANG: dict[str, str] = {
    "lt": "la",
    "la": "la",
    "fr": "fr",
    "en": "en",
    "it": "it",
    "de": "de",
    "es": "es",
    "sp": "es",  # vieilles pages utilisent _sp
    "pt": "pt",
    "pl": "pl",
    "hu": "hu",
    "sl": "sl",
    "zh": "zh",
    "ar": "ar",
    "ru": "ru",
    "uk": "uk",
    "cs": "cs",
    "nl": "nl",
    "sk": "sk",
    "hr": "hr",
    "ro": "ro",
}

# Langues qu'on accepte effectivement dans le corpus — les autres (kiswahili,
# marathi, etc.) sont ignorées pour limiter le bruit.
ACCEPTED_LANGS: frozenset[str] = frozenset({
    "la", "fr", "en", "it", "de", "es", "pt", "pl",
    "hu", "sl", "zh", "ar", "ru", "uk", "cs", "nl", "sk", "hr", "ro",
})

_VATICAN_LANG_PATTERN = re.compile(
    r"^(?P<base>.+?)_(?P<suffix>[a-z]{2,3})\.html?$", re.IGNORECASE
)


def _parse_vatican_filename(filename: str) -> tuple[str, str] | None:
    """Retourne (base_stem, lang_suffix_minuscule) ou None si le nom de
    fichier ne suit pas le motif vatican.va."""
    m = _VATICAN_LANG_PATTERN.match(filename)
    if m is None:
        return None
    return (m.group("base"), m.group("suffix").lower())


def discover_vatican_va(
    html: bytes | str,
    source_url: str,
) -> list[tuple[str, str]]:
    """Parse le HTML d'une page vatican.va et renvoie les URLs de traduction.

    Retourne ``[(lang_code, url), …]`` triée par code langue, sans inclure
    la langue de la source. La liste est vide si l'URL ne suit pas le motif
    attendu ou si aucun lien de traduction n'est trouvé.
    """
    parsed = urlparse(source_url)
    if parsed.netloc != "www.vatican.va":
        return []
    source_file = parsed.path.rsplit("/", 1)[-1]
    info = _parse_vatican_filename(source_file)
    if info is None:
        return []
    source_stem, source_suffix = info
    base_dir = parsed.path.rsplit("/", 1)[0]

    if isinstance(html, bytes):
        # Legacy vatican.va pages sont parfois en windows-1252 ; on ne se
        # soucie pas ici de l'encodage du texte utile (on cherche juste des URLs).
        text = html.decode("utf-8", errors="replace")
    else:
        text = html

    tree = HTMLParser(text)
    out: dict[str, str] = {}
    for a in tree.css("a[href]"):
        href = (a.attributes.get("href") or "").strip()
        if not href or href.startswith(("mailto:", "javascript:", "#")):
            continue
        abs_url = urljoin(source_url, href)
        abs_parsed = urlparse(abs_url)
        if abs_parsed.netloc != parsed.netloc:
            continue
        filename = abs_parsed.path.rsplit("/", 1)[-1]
        info2 = _parse_vatican_filename(filename)
        if info2 is None:
            continue
        stem, suffix = info2
        # Doit avoir le même stem et être dans le même dossier que la source
        abs_dir = abs_parsed.path.rsplit("/", 1)[0]
        if stem != source_stem or abs_dir != base_dir:
            continue
        if suffix == source_suffix:
            continue
        lang = VATICAN_SUFFIX_TO_LANG.get(suffix)
        if lang is None or lang not in ACCEPTED_LANGS:
            continue
        # Normaliser l'URL : garder le scheme/host/path sans query/fragment
        clean = f"{abs_parsed.scheme}://{abs_parsed.netloc}{abs_parsed.path}"
        out.setdefault(lang, clean)

    return sorted(out.items())


def discover(html: bytes | str, source_url: str) -> list[tuple[str, str]]:
    """Dispatcher par domaine. Retourne liste (lang, url) des traductions
    officielles détectables depuis le HTML de la source. Vide par défaut
    pour les domaines sans support."""
    host = urlparse(source_url).netloc
    if host == "www.vatican.va":
        return discover_vatican_va(html, source_url)
    # papalencyclicals.net, laportelatine.org, etc. : pas de traductions
    # officielles multi-langues, on laisse le skill IA s'en occuper.
    return []


__all__ = ["discover", "discover_vatican_va", "ACCEPTED_LANGS"]
