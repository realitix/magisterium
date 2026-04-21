"""Apply the HTML chrome strips to every already-scraped .md in magisterium/.

Originally the cleanup (strip chrome images, dead anchors, empty wrappers)
was done only on the site side at build time. We've since moved the logic
into the scraper (`scrapers/core/clean_html.py`) so that freshly-scraped
markdown is already clean, but the existing ~1300 documents were scraped
before the fix. Refetching all of them would hammer vatican.va for no gain.

Instead, this tool re-applies the same strips on the *markdown* directly,
using string-level transformations that mirror what the Python cleaner
does on the HTML source before pandoc.

Idempotent. Runs in place. Use `--dry-run` to preview the diff.

Usage:
    uv run python -m tools.clean_corpus [--dry-run] [--limit N]
"""
from __future__ import annotations

import argparse
import re
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
CORPUS = REPO_ROOT / "magisterium"


# --- Markdown-level patterns -------------------------------------------------
#
# We operate on the already-converted Markdown. Pandoc emits these constructs
# for the HTML patterns we want to strip :
#
#   ![alt](src)                       — an image ;      matched by IMG_MD
#   [![alt](src)](link)                — image wrapped in a link
#   [text](javascript:…)               — dead anchors (kept text)
#   [text](mailto:)                    — empty mailto
#   [text](#)                          — empty fragment
#   [text](https://www.facebook.com/sharer…)
#   [text](https://twitter.com/home…)
#   …and so on.

_IMG_CHROME = re.compile(
    r"""
    !\[[^\]]*\]\(                              # ![alt](
    [^)]*?                                     # whatever leads into the match
    (?:
        /img/(?:vuoto|back|up|top|print|pkeys|riga_int|psearch_fill|logo-vatican)
        \.(?:gif|png|jpg)
      | /share/(?:facebook|twitter|gplus|mail)\.png
    )
    [^)]*                                      # query / suffix
    \)
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Navigation chrome bar : line containing images of back/top/print buttons,
# typically with `javascript:history.go(-1)` as the href (which breaks the
# naive `[^)]*` regexes because of the internal parenthesis in `go(-1)`).
# Strip the entire line.
_NAV_CHROME_LINE = re.compile(
    r"""
    ^[ \t]*                                    # leading whitespace
    .*                                         # any prefix on the line
    (?:
        /img/(?:back|up|top|print)\.(?:png|jpg|gif)
      | javascript:history\.go
    )
    .*$                                        # rest of the line
    """,
    re.IGNORECASE | re.VERBOSE | re.MULTILINE,
)

# Image wrapped in a link — `[![alt](src)](url)`. Replace with nothing.
_IMG_LINK_CHROME = re.compile(
    r"""
    \[
        \s*
        """ + _IMG_CHROME.pattern + r"""
        \s*
    \]
    \(
        [^)]*
    \)
    """,
    re.IGNORECASE | re.VERBOSE,
)

_DEAD_LINK = re.compile(
    r"""
    \[
        ([^\]]*?)                              # captured text (group 1)
    \]
    \(
        (?:
            javascript:[^)]*
          | mailto:[^)]*                       # empty or not, drop the link
          | \#\s*                              # empty fragment
          | https?://(?:www\.)?facebook\.com/sharer[^)]*
          | https?://(?:www\.)?twitter\.com/(?:home|share|intent)[^)]*
          | https?://(?:www\.)?plus\.google\.com/share[^)]*
          | https?://(?:www\.)?linkedin\.com/[^)]*sharing[^)]*
          | https?://(?:www\.)?api\.whatsapp\.com[^)]*
          | https?://t\.me/share[^)]*
        )
    \)
    """,
    re.IGNORECASE | re.VERBOSE,
)

# Remaining empty linked image shells — `[](url)` with nothing between brackets
_EMPTY_LINK = re.compile(r"\[\s*\]\([^)]*\)")

# Orphan punctuation lines : a line that is only brackets, parens, or pipes,
# left behind by previous strips (notably when the URL contained nested
# parentheses like `javascript:history.go(-1)` and the outer regex stopped
# at the inner `)`).
_ORPHAN_PUNCT_LINE = re.compile(
    r"""
    ^[ \t\xa0]*              # leading whitespace
    [)(\[\]|*\-]{1,8}         # 1..8 bracket/pipe/asterisk/dash characters
    [ \t\xa0]*$              # trailing whitespace
    """,
    re.MULTILINE | re.VERBOSE,
)


def clean_markdown(text: str) -> tuple[str, dict[str, int]]:
    """Apply all strips to a markdown string.

    Returns the cleaned text and a stats dict.
    """
    stats = {
        "nav_chrome_line": 0,
        "img_link_chrome": 0,
        "img_chrome": 0,
        "dead_link": 0,
        "empty_link": 0,
        "orphan_punct": 0,
    }

    # 0. Nav chrome lines (back/top/print button bars, javascript:history.go).
    # These must go first because their javascript: URLs contain parentheses
    # that break the naive `[^)]*` regexes below.
    def _drop_nav(_m: re.Match[str]) -> str:
        stats["nav_chrome_line"] += 1
        return ""

    text = _NAV_CHROME_LINE.sub(_drop_nav, text)

    # 1. Images wrapped in a link : drop the whole construct.
    def _drop_img_link(_m: re.Match[str]) -> str:
        stats["img_link_chrome"] += 1
        return ""

    text = _IMG_LINK_CHROME.sub(_drop_img_link, text)

    # 2. Bare chrome images not wrapped in a link.
    def _drop_img(_m: re.Match[str]) -> str:
        stats["img_chrome"] += 1
        return ""

    text = _IMG_CHROME.sub(_drop_img, text)

    # 3. Dead anchor links : keep the text, drop the link markup.
    def _unlink(m: re.Match[str]) -> str:
        stats["dead_link"] += 1
        inner = m.group(1).strip()
        return inner

    text = _DEAD_LINK.sub(_unlink, text)

    # 4. Empty links `[](url)` left by image strips.
    def _drop_empty(_m: re.Match[str]) -> str:
        stats["empty_link"] += 1
        return ""

    text = _EMPTY_LINK.sub(_drop_empty, text)

    # 5. Orphan punctuation lines left by the above passes (e.g. `)` alone
    # on a line because an earlier URL contained an unbalanced parenthesis).
    def _drop_orphan(_m: re.Match[str]) -> str:
        stats["orphan_punct"] += 1
        return ""

    text = _ORPHAN_PUNCT_LINE.sub(_drop_orphan, text)

    # 6. Collapse runs of blank lines introduced by the deletions.
    text = re.sub(r"\n{3,}", "\n\n", text)

    return text, stats


def main() -> int:
    ap = argparse.ArgumentParser(description="Clean chrome/dead-links from corpus markdown.")
    ap.add_argument("--dry-run", action="store_true", help="Show what would change, don't write.")
    ap.add_argument("--limit", type=int, default=0, help="Process only the first N files.")
    args = ap.parse_args()

    mds = sorted(p for p in CORPUS.rglob("*.md") if not p.name.endswith(".MISSING.md"))
    if args.limit:
        mds = mds[: args.limit]

    totals = {
        "nav_chrome_line": 0,
        "img_link_chrome": 0,
        "img_chrome": 0,
        "dead_link": 0,
        "empty_link": 0,
        "orphan_punct": 0,
    }
    changed_files = 0

    for md in mds:
        original = md.read_text(encoding="utf-8")
        cleaned, stats = clean_markdown(original)
        touched = any(v > 0 for v in stats.values())
        if touched:
            changed_files += 1
            for k, v in stats.items():
                totals[k] += v
            if args.dry_run:
                rel = md.relative_to(REPO_ROOT)
                print(f"[would clean] {rel}  " + "  ".join(f"{k}={v}" for k, v in stats.items() if v))
            else:
                md.write_text(cleaned, encoding="utf-8")

    print()
    print(f"Files scanned  : {len(mds)}")
    print(f"Files touched  : {changed_files}")
    for k, v in totals.items():
        print(f"  {k:20s} : {v}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
