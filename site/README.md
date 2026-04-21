# Site Magisterium

Site statique Astro qui publie le corpus magistériel catholique et des fiches Questions/Réponses doctrinales.

## Stack

- **Astro 5** (TypeScript strict)
- **Pagefind 1.5** pour la recherche plein-texte côté client
- **Marked** pour le rendu Markdown
- Typographie : EB Garamond + Cormorant Garamond (fallback Georgia)

## Structure

- `src/content/questions/` — fiches Q/R (frontmatter + markdown)
- `src/pages/` — routes Astro
  - `index.astro` — page d'accueil
  - `a-propos.astro` — méthodologie et posture éditoriale
  - `questions/[slug].astro` — fiche Q/R individuelle
  - `themes/[slug].astro` — vue comparative pré-V2 / V2 / post-V2
  - `documents/[slug].astro` — page document (1304 routes)
  - `recherche.astro` — recherche Pagefind
- `src/layouts/` — `Base`, `Document`, `Theme`, `Question`
- `src/components/` — `Header`, `Footer`, `Citation`, `ThemeCompare`, `SearchBar`
- `src/data/` — data loaders (lisent `../magisterium/_metadata/index.jsonl` et `concordance.jsonl` + `*.meta.yaml` au build)

Le site lit le corpus directement depuis `../magisterium/` au build — pas de duplication.

## Développement local

Depuis la racine du monorepo (`/home/realitix/git/catholique/`) :

```bash
just install-site   # npm install dans site/
just dev-site       # astro dev sur http://localhost:4321
just build-site     # build production dans site/dist/ + Pagefind
just preview-site   # sert dist/ en local
```

Ou directement dans `site/` :

```bash
npm install
npm run dev
npm run build
npm run preview
```

## Déploiement Netlify

Le fichier `netlify.toml` configure déjà :
- `base = "site"` (Netlify opère dans ce sous-dossier du monorepo)
- `command = "npm install && npm run build"`
- `publish = "dist"`
- Node 22
- Headers de cache pour `/fonts/` et `/pagefind/`

### Option 1 — Lier le repo Git à Netlify
1. Créer un site sur `app.netlify.com`.
2. Connecter le repo ; Netlify détecte `netlify.toml`.
3. Build automatique à chaque push.

### Option 2 — Déploiement manuel (CLI)

```bash
cd site
npm install
npm run build
npx netlify deploy --prod --dir=dist
```

### Domaine personnalisé

Ajouter un alias `CNAME` ou `A` vers `<site>.netlify.app` dans la zone DNS, puis configurer le domaine dans Netlify > Domain management.

## Ajouter une nouvelle fiche Q/R

Voir [`CONTRIBUTING-QR.md`](./CONTRIBUTING-QR.md) pour le processus complet (délégation aux agents opus, consignes de traduction, revue).

Résumé : créer un fichier `src/content/questions/<slug>.md` avec frontmatter YAML complet (`title`, `question`, `date`, `tags`, `related_documents`, `related_themes`, `posture`). Les citations en langue étrangère doivent **systématiquement** être suivies d'une traduction française.

## Taille du site buildé

- ≈ 1349 pages HTML (1 accueil + 11 thèmes + 1304 documents + 1 Q/R + index + recherche + a-propos + 404 + pagination)
- ≈ 79 Mo total (dont index Pagefind)
- Build : ≈ 10 s

## Notes

- Certaines pages conciliaires ont `langue_originale: la` dans leur meta.yaml mais seule la version anglaise est présente dans le corpus — le data loader fait un fallback automatique.
- Pagefind indexe uniquement les pages avec `data-pagefind-body` (layouts Document, Question, Theme) — les pages d'index ne sont pas indexées, ce qui évite le bruit.
