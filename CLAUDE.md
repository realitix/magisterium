# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Big picture

Two-part monorepo :

1. **Python corpus archiver** — scrapers + tools qui construisent un corpus local de documents magistériels catholiques (conciles, papes, curie, catéchismes, droit, liturgie, FSSPX) dans `magisterium/`. Chaque document stocke d'abord sa **langue originale** (source de vérité doctrinale) puis, si le site source le permet, les **traductions officielles** (vatican.va publie souvent 6-8 langues). Les langues manquantes peuvent être complétées par IA via le skill `translate-corpus` (voir ci-dessous).
2. **Astro static site** dans `site/` — publie le corpus (pages document multi-langues, pages thème, fiches Q/R doctrinales) avec recherche full-text Pagefind côté client. Déployé sur Netlify.

Le site **lit le corpus au build** depuis `../magisterium/` (pas de duplication). Toute modification du corpus Python oblige un rebuild du site.

## Commandes courantes (via `just`)

```bash
# Setup (une fois)
just setup                 # uv sync + vérifie pandoc

# Scraping (phases indépendantes, reprenables)
just phase-1               # conciles œcuméniques
just phase-2               # Vatican II
just phase-3 / 4 / 5       # papes pré-V2 / post-V2 / pré-1846
just phase-6               # curie romaine (CDF/DDF, dicastères)
just phase-7               # catéchismes
just phase-8               # droit canonique, liturgie
just phase-9               # FSSPX
just resume                # reprend un scraping interrompu
just refresh SOURCE        # re-scrape un doc précis

# Outils corpus
just build-index             # régénère magisterium/_metadata/index.jsonl
just build-concordance       # régénère magisterium/_metadata/concordance.jsonl (11 thèmes)
just complete-translations   # pour chaque doc vatican.va, récupère les traductions officielles (multi-lang)
just migrate-traductions     # migration schema → bloc `traductions` (one-shot)
just validate                # vérifie tous les .meta.yaml + .md
just stats                   # statistiques corpus
uv run python -m tools.repair_broken          # rescrape les .md cassés (contenu [TABLE], etc.)
uv run python -m tools.repair_broken --dry-run

# Site web (cd site/ équivalent)
just dev-site              # astro dev sur localhost:4321
just build-site            # build production + index Pagefind → site/dist/
just preview-site          # sert site/dist/ en local
just install-site          # npm install dans site/
```

Pas de test suite. `just validate` fait office de vérification structurelle (YAML OK, Pydantic OK, sha256 non vide, .md non vide ou sibling .MISSING.md présent).

## Architecture du scraper

**Pipeline idempotent** (`scrapers/core/pipeline.py`) : `DocRef` (URL + slug + langue + dossier cible) → `fetcher.py` (httpx HTTP/2, rate limit par domaine) → `markdown.py` (pandoc + selectolax, via des sélecteurs CSS par domaine dans `SITE_SELECTORS`) → `.md` + `.meta.yaml` sidecar.

- **Idempotence** : skip si le `.meta.yaml` existe, sauf `refresh=True`.
- **Modèle** : `DocMeta` (Pydantic) dans `scrapers/core/meta.py` — schéma strict des sidecars.
- **Parallélisme** : domaines exécutés en parallèle, rate limit par domaine (`rate_limit.py`).
- **Sélecteurs** : `SITE_SELECTORS` map chaque site à un sélecteur CSS pour extraire le corps. Vatican.va utilise des `<table>` de mise en page ; le parser a une heuristique qui descend dans les table layouts sinon rend `[TABLE]`. Quand cette heuristique rate, `tools.repair_broken` permet de refetch en masse.

Phases (`scrapers/phases/phase_N_*.py`) : chaque phase construit une liste de `DocRef` pour un corpus donné et les passe au pipeline.

## Structure du corpus

```
magisterium/
├── A-pre-vatican-ii/        # conciles, papes, curie, catéchismes, droit, liturgie
├── B-vatican-ii/            # constitutions, déclarations, décrets
├── C-post-vatican-ii/       # papes post-V2, curie récente, catéchismes (CEC)
├── D-fsspx/                 # Mgr Lefebvre, supérieurs généraux, FSSPX
└── _metadata/
    ├── index.jsonl          # 1 ligne JSON par document (1304 au total)
    ├── concordance.jsonl    # 11 thèmes → slugs pré-V2 / V2 / post-V2 / FSSPX
    ├── fetch-strategy.json
    └── errors.log
```

Règle clé : **la langue originale reste la source de vérité doctrinale**. Elle est toujours présente et marquée `kind: originale` dans `traductions` du `.meta.yaml`. Les traductions officielles scrapées (ex. vatican.va) sont marquées `kind: officielle`, les traductions IA produites par le skill `translate-corpus` sont marquées `kind: ia`. Un document peut donc avoir jusqu'à ~10 fichiers `.lang.md` frères (un par langue). Toute analyse doctrinale sérieuse (fiches Q/R, citations) DOIT partir du `.<langue_originale>.md` — les autres sont de l'aide à la lecture.

Structure d'une entrée `traductions` :

```yaml
traductions:
  la:
    kind: originale           # source de vérité
    sha256: <hash>
    source_url: https://www.vatican.va/...
  fr:
    kind: officielle          # publiée par le Saint-Siège
    sha256: <hash>
    source_url: https://www.vatican.va/..._fr.html
  es:
    kind: ia                  # générée par translate-corpus
    sha256: <hash>
    model: claude-opus-4-7
    translated_from: la
    source_sha256: <hash de la source au moment de la traduction>
    translated_at: 2026-05-01T12:00:00Z
```

## Architecture du site

**Stack** : Astro 5 (TS strict) + Pagefind (search côté client) + Marked (render markdown) + Netlify (hosting).

**Data layer** (`site/src/data/`) : `loadDocuments.ts` et `loadThemes.ts` lisent `../magisterium/_metadata/*.jsonl` au build, en cache in-memory. `loadMarkdown.ts` résout le `.lang.md` correspondant à un slug.

**Content collection** `questions` (`site/src/content/config.ts`) : schéma Zod pour les fiches Q/R, incluant le champ obligatoire `voices` (les 4 courants catholiques).

**Layouts principaux** (`site/src/layouts/`) :
- `Base.astro` — charge styles + Header/Footer + skip-link
- `Document.astro` — affiche un document avec metadata sidebar
- `Theme.astro` — vue comparative pré-V2 / V2 / post-V2 via `ThemeCompare`
- `Question.astro` — fiche Q/R avec ToC sticky + scroll-spy, FourVoices en pied, DocumentsGrid enrichie, ReadingProgress, BackToTop

**Pages dynamiques** :
- `pages/documents/[slug]/[lang].astro` — route canonique, une par couple (document, langue) ; indexée Pagefind avec filtre `lang`.
- `pages/documents/[slug]/index.astro` — route legacy `/documents/{slug}/` qui redirige vers la langue originale (compatibilité des anciens liens ; ni indexée ni dupliquée dans les résultats de recherche).
- `pages/themes/[slug].astro` (11).
- `pages/questions/[slug].astro`.

`getStaticPaths()` itère sur les loaders. Pour les documents, chaque entrée du bloc `traductions` d'un doc génère une route distincte. L'UI expose un switcher de langue avec des badges de provenance (originale / trad. officielle / trad. IA), et un bandeau non-dismissible apparaît en tête d'article pour `kind: ia` qui renvoie vers la version originale.

**Pagefind** : index généré au postbuild (`astro build && pagefind --site dist`). Seuls les éléments avec `data-pagefind-body` (les `<main>` des layouts Document/Question/Theme) sont indexés. L'UI charge `pagefind-ui.js` à runtime, tokenise, télécharge les shards pertinents. Pas de backend.

**Déploiement Netlify** : `site/netlify.toml` avec `ignore = "/bin/false"` qui **force un rebuild à chaque push**, car le site dépend du corpus hors du `base = "site"` directory — sinon Netlify skippe quand seul `magisterium/` change.

## Règles éditoriales (importantes)

**Fiches Q/R** (`site/src/content/questions/*.md`) :

- **Posture apologétique traditionnelle assumée** : lecture traditionnelle privilégiée, tensions post-V2 nommées comme telles, lettre des textes respectée.
- **Toute citation en langue étrangère DOIT être suivie d'une traduction française** (latin, italien, anglais, allemand…). Sans exception.
- **Bloc `voices` obligatoire** dans le frontmatter — les quatre courants catholiques contemporains dans l'ordre canonique : `conciliaire` → `ecclesia_dei` → `fsspx` → `sedevacantiste`. Chaque voix a `tagline`, `body` (60-90 mots), `punchline?` optionnelle. Rendu automatique par `FourVoices.astro` en pied de fiche.
- **Sections canoniques** du markdown : Contexte → Enseignement traditionnel → Enseignement V2 et post-V2 → Contradictions et tensions → Réponse dogmatique → Comment répondre pastoralement → Références.

Pour créer une nouvelle fiche Q/R, invoquer le skill **`create-fiche-qr`** (`.claude/skills/create-fiche-qr/SKILL.md`) qui documente le processus complet : délégation parallèle à des agents opus (un par gros texte du corpus : Trente, Pie X, CEC latin…), consignes strictes de citation, synthèse orchestrée.

Voir aussi `site/CONTRIBUTING-QR.md` pour le détail du flow et les règles de rendu.

## Dépendances externes

- **uv** pour Python (pyproject.toml, uv.lock)
- **just** pour les commandes (optionnel, tout est faisable via `uv run …`)
- **pandoc** pour la conversion HTML → markdown du scraper
- **Node 22** pour le site (voir `site/.nvmrc`)

Cible Python 3.12. Ruff configuré (line-length 100).
