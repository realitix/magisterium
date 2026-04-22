r"""One-shot cleanup : retire la nav vatican.va en tête des fichiers .md scrapés.

Les pages vatican.va DDF / curie récentes exposent en haut une barre de
langues en texte brut, ramenée telle quelle dans les markdown scrapés :

    \[[DE](...) - [EN](...) - [ES](...) - [FR](...) - [IT](...) - [PT](...)\]

On n'en a pas besoin dans le site Magisterium puisque le switcher de langue
est géré par l'UI. Ce tool la détecte (pattern : 2+ liens `[XX](url)` séparés
par ` - `, éventuellement entre crochets échappés) dans les 10 premières
lignes non vides de chaque .md et la retire.

Idempotent : re-passer dessus ne fait rien de plus.

Run :
    uv run python -m tools.strip_lang_nav [--dry-run]
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
CORPUS = ROOT / "magisterium"

# Un lien de nav : [XX](http...) avec XX = 2-3 lettres
_LINK_RE = re.compile(r"\[[A-Z]{2,3}\]\(https?://[^)]+\)")
# Une ligne de nav : 1+ liens séparés par ` - `, optionnellement entre `\[...\]`.
# Accepte {1,} pour couvrir les pages qui publient un seul lien redondant
# vers elles-mêmes (ex. `\[[IT](...)\]` sur une page italienne).
_NAV_LINE_RE = re.compile(
    # Une ligne composée uniquement de liens-langue `[XX](url)` :
    #   - préfixe possible : `>` (blockquote), `\[` (bracket échappé), `[`
    #   - label : jusqu'à 10 caractères (couvre `ZH_TW`, `FR -` mal parsé,
    #     et autres bruits ; on reste contraint en longueur pour limiter
    #     les faux positifs sur de vrais liens de contenu)
    #   - URL : n'importe quoi sauf parenthèse (relatif, absolu, pdf…)
    #   - séparateurs possibles entre liens : ` - `, ` – `, ` · `, ` | `, `, `
    #   - suffixe possible : `\]`, `]`
    r"^\s*>?\s*\\?\[?\s*"
    r"(?:\[[^\]\n]{1,10}\]\([^)]+\)\s*(?:[-–—·|,]\s*)?){1,}"
    r"\s*\\?\]?\s*$"
)
# Ligne de « bruit » laissée par le scraping (résidus d'éléments non-texte
# que pandoc a sérialisés partiellement — parenthèses isolées, barres de
# séparation, lignes horizontales). On ne les supprime qu'en zone d'en-tête,
# jusqu'à la première ligne de texte réel.
_NOISE_LINE_RE = re.compile(
    r"^[\s\-–—_*=()\[\]\\.·|]*$"
)
# Phrases de chrome récurrentes sur vatican.va, ajoutées automatiquement
# par le site source autour du contenu utile. On les repère à 100 %
# littéralement en début de fichier pour éviter tout faux positif.
_CHROME_PREFIXES: tuple[str, ...] = (
    "The Holy See - Vatican web site",
    "The Holy See",
    "Vatican web site",
    "Libreria Editrice Vaticana",
    "© Copyright",
)


def _clean(text: str) -> tuple[str, bool]:
    """Retourne (nouveau_texte, has_changed).

    Deux passes :
      1. Dans les 10 premières lignes, on retire toutes les nav vatican.va.
      2. On racle ensuite tout le « bruit » résiduel en tête (parenthèses
         isolées, lignes vides, séparateurs horizontaux) jusqu'à la première
         ligne de texte substantif.
    """
    lines = text.splitlines(keepends=True)
    scan_until = min(10, len(lines))
    changed = False
    out: list[str] = []
    for i, line in enumerate(lines):
        if i < scan_until and _NAV_LINE_RE.match(line):
            changed = True
            continue
        # Chrome vatican.va récurrent : "The Holy See - Vatican web site", etc.
        if i < scan_until:
            stripped_line = line.strip()
            if any(stripped_line.startswith(p) for p in _CHROME_PREFIXES) and len(stripped_line) < 80:
                changed = True
                continue
        out.append(line)

    # Racler le bruit d'en-tête : lignes vides / séparateurs / ponctuation
    # orpheline, **mais seulement dans les 10 premières lignes restantes**.
    limit = min(10, len(out))
    idx = 0
    while idx < limit:
        stripped = out[idx].strip()
        if stripped == "" or _NOISE_LINE_RE.match(stripped):
            idx += 1
        else:
            break
    if idx > 0:
        changed = True
        out = out[idx:]
    return ("".join(out), changed)


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    md_files = sorted(CORPUS.rglob("*.md"))
    touched = 0
    for md in md_files:
        # Skip sentinels and non-lang files
        if md.name.endswith(".MISSING.md"):
            continue
        try:
            text = md.read_text(encoding="utf-8")
        except Exception:  # noqa: BLE001
            continue
        new, changed = _clean(text)
        if changed:
            touched += 1
            if not args.dry_run:
                md.write_text(new, encoding="utf-8")

    prefix = "[DRY-RUN] " if args.dry_run else ""
    print(f"{prefix}strip_lang_nav : {touched} fichiers modifiés / {len(md_files)} scannés")
    return 0


if __name__ == "__main__":
    sys.exit(main())
