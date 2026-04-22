"""Clean scraped HTML before pandoc conversion.

Strips content that has no value in the magisterium corpus :
  - chrome images (vatican.va logos, spacer GIFs, social share icons)
  - dead anchors (`javascript:`, empty `mailto:`, empty `#`, social sharers)
  - `<a>` elements whose body becomes empty after the above

Runs on a selectolax HTMLParser node (usually the `<body>` or a site-specific
root selected by `_extract_body`). Mutates the tree in place.

The site-level post-processor (`site/src/data/cleanHtml.ts`) still handles
two things that require the full corpus context : rewriting vatican.va URLs
to internal slugs when the cited document is present, and marking the
remaining absolute links as external. Everything else happens here at scrape
time so the markdown stored in ``magisterium/`` is already clean.
"""
from __future__ import annotations

import re
from typing import Iterable

from selectolax.parser import Node

# --- Patterns ---------------------------------------------------------------

# `<img src="...">` patterns that indicate site chrome, not real content.
_DEAD_IMG_SRC = re.compile(
    r"""
    (?:
      /img/vuoto\.gif
      | /img/(?:back|up|top|print)\.(?:jpg|png|gif)
      | /img/pkeys\.jpg
      | /img/logo-vatican\.png
      | /img/riga_int\.jpg
      | /img/psearch_fill\.jpg
      | /share/(?:facebook|twitter|gplus|mail)\.png
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

# `<a href="...">` patterns that indicate dead / chrome links.
_DEAD_HREF = re.compile(
    r"""
    ^(?:
      javascript:
      | mailto:\s*$
      | \#\s*$
      | (?:https?:)?//(?:www\.)?facebook\.com/sharer
      | (?:https?:)?//(?:www\.)?twitter\.com/(?:home|share|intent)
      | (?:https?:)?//(?:www\.)?plus\.google\.com/share
      | (?:https?:)?//(?:www\.)?linkedin\.com/.*sharing
      | (?:https?:)?//(?:www\.)?api\.whatsapp\.com
      | (?:https?:)?//t\.me/share
    )
    """,
    re.IGNORECASE | re.VERBOSE,
)

_WHITESPACE_ONLY = re.compile(r"^(?:\s|&nbsp;|\xa0)*$")

# Code langue ISO sur 2 lettres (y compris variantes vatican.va : _lt, _ge, _sp…)
_LANG_CODE = re.compile(r"^[A-Z]{2,3}$")
# Un href vatican.va qui se termine par `_<lang>.html` — la barre de choix
# de langue.
_VATICAN_LANG_HREF = re.compile(
    r"vatican\.va/.+_[a-z]{2,3}\.html?$",
    re.IGNORECASE,
)


# --- Helpers ----------------------------------------------------------------


def _strip_chrome_images(root: Node) -> int:
    """Remove `<img>` elements whose src matches the chrome patterns.

    Returns the number of images removed.
    """
    removed = 0
    for img in list(root.css("img")):
        src = img.attributes.get("src", "") or ""
        if _DEAD_IMG_SRC.search(src):
            img.decompose()
            removed += 1
    return removed


def _strip_dead_links(root: Node) -> int:
    """Unwrap `<a>` tags whose href is dead (javascript, empty mailto,
    share widgets, etc.). The inner text is preserved.

    Returns the number of anchors unwrapped.
    """
    removed = 0
    for a in list(root.css("a")):
        href = a.attributes.get("href", "") or ""
        if _DEAD_HREF.match(href):
            # Replace the <a> with its inner HTML, preserving the text content.
            a.unwrap()
            removed += 1
    return removed


def _strip_nav_chrome_images(root: Node) -> int:
    """Remove `<a>` tags that wrap a back/top/print/logo chrome image, even
    when their href contains a parenthesis (e.g. `javascript:history.go(-1)`).

    Called as a pre-pass before the generic image/anchor strippers — which
    is safer because those don't gracefully handle chrome buttons whose
    href uses `(...)` syntax.
    """
    removed = 0
    for a in list(root.css("a")):
        img = a.css_first("img")
        if img is None:
            continue
        src = img.attributes.get("src", "") or ""
        if _DEAD_IMG_SRC.search(src):
            a.decompose()
            removed += 1
    return removed


def _strip_empty_anchors(root: Node) -> int:
    """Remove `<a>` elements whose visible content is empty after prior
    strippings. Iterates until stable because removing one may empty its
    block-level parent.

    Returns the total number of empty anchors removed.
    """
    total = 0
    while True:
        removed_this_pass = 0
        for a in list(root.css("a")):
            # Remove nested images from the count (they were already stripped
            # if they matched chrome, but other images may remain and count
            # as content).
            text = (a.text() or "").strip()
            has_img = a.css_first("img") is not None
            has_svg = a.css_first("svg") is not None
            if text == "" and not has_img and not has_svg:
                a.decompose()
                removed_this_pass += 1
        total += removed_this_pass
        if removed_this_pass == 0:
            break
    return total


def _strip_vatican_lang_nav(root: Node) -> int:
    """Remove the vatican.va language chooser strip when it appears in-body.

    Pattern in HTML (DDF / curie récentes) :
        [<a href="..._ge.html">DE</a> - <a href="..._en.html">EN</a>
         - <a href="..._sp.html">ES</a> - <a href="..._fr.html">FR</a>
         - <a href="..._it.html">IT</a> - <a href="..._po.html">PT</a>]

    On détecte un parent <p> / <div> dont le contenu utile se limite à une
    liste de tels liens (2+ adjacents, libellés en codes 2-3 lettres et
    href pointant vers une variante `..._xx.html` sur vatican.va). On
    supprime le parent entier pour ne pas laisser de séparateurs orphelins.
    Returns the number of navs stripped.
    """
    removed = 0
    # On regarde p / div / span de niveau supérieur.
    for container in list(root.css("p, div, span")):
        anchors = container.css("a")
        if len(anchors) < 2:
            continue
        # Tous les <a> doivent être des liens de nav de langue
        all_lang_anchors = True
        for a in anchors:
            text = (a.text() or "").strip()
            href = (a.attributes.get("href") or "").strip()
            if not _LANG_CODE.match(text):
                all_lang_anchors = False
                break
            if not _VATICAN_LANG_HREF.search(href):
                all_lang_anchors = False
                break
        if not all_lang_anchors:
            continue
        # Vérifier que le reste du texte (hors liens) ne contient rien de
        # substantiel — uniquement des séparateurs et crochets.
        full_text = (container.text() or "").strip()
        link_text = " ".join((a.text() or "").strip() for a in anchors)
        leftover = full_text
        for piece in link_text.split():
            leftover = leftover.replace(piece, " ", 1)
        leftover = re.sub(r"[\s\-–—·|\[\]()]+", "", leftover)
        if leftover:
            continue  # contient autre chose que la nav → on n'y touche pas
        container.decompose()
        removed += 1
    return removed


def _collapse_empty_blocks(root: Node) -> int:
    """Remove now-empty `<li>`, `<p>`, `<div>` elements left by previous
    strips. Non-semantic cleanup — better output for pandoc.
    """
    removed = 0
    for tag in ("li", "p", "div"):
        for el in list(root.css(tag)):
            # Skip if it contains any element child that isn't empty.
            html = el.html or ""
            # Remove tags and entities to decide emptiness.
            inner = re.sub(r"<[^>]+/?>", "", html)
            inner = inner.replace("&nbsp;", "").replace("\xa0", "")
            if _WHITESPACE_ONLY.match(inner):
                el.decompose()
                removed += 1
    return removed


# --- Public API -------------------------------------------------------------


class CleanStats:
    __slots__ = ("images", "dead_links", "empty_anchors", "empty_blocks")

    def __init__(self) -> None:
        self.images = 0
        self.dead_links = 0
        self.empty_anchors = 0
        self.empty_blocks = 0

    def __repr__(self) -> str:  # pragma: no cover
        return (
            f"CleanStats(images={self.images}, dead_links={self.dead_links}, "
            f"empty_anchors={self.empty_anchors}, empty_blocks={self.empty_blocks})"
        )


def clean_scraped_html(root: Node) -> CleanStats:
    """Apply all chrome / dead-link strips to a selectolax tree in place.

    Returns a `CleanStats` with per-pass counts (mostly for debugging).
    """
    stats = CleanStats()
    # Pre-pass : remove entire anchors wrapping a chrome image (safer than
    # relying on the href strip when the URL contains parentheses).
    stats.dead_links += _strip_nav_chrome_images(root)
    # Barre de choix de langue vatican.va → rendue inutile par le switcher UI.
    stats.dead_links += _strip_vatican_lang_nav(root)
    stats.images = _strip_chrome_images(root)
    stats.dead_links += _strip_dead_links(root)
    # Two passes : removing anchors can make blocks empty, and removing blocks
    # can orphan further anchors (rare but possible). Loop until stable.
    while True:
        a = _strip_empty_anchors(root)
        b = _collapse_empty_blocks(root)
        stats.empty_anchors += a
        stats.empty_blocks += b
        if a == 0 and b == 0:
            break
    return stats


__all__: Iterable[str] = ("clean_scraped_html", "CleanStats")
