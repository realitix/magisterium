---
name: create-fiche-qr
description: Crée une fiche Question/Réponse doctrinale pour le site Magisterium, en langue source + traduction française, avec posture apologétique traditionnelle assumée. Orchestre des agents opus en parallèle (un par gros texte du corpus), produit le markdown final avec frontmatter complet (incluant les quatre voix catholiques obligatoires), et vérifie que la fiche rend correctement. À utiliser quand l'utilisateur formule une question doctrinale (salut, sacrements, liturgie, morale, ecclésiologie…) et qu'il attend une fiche de corpus catholique — ou qu'il le dit explicitement (« crée une fiche Q/R », « fais une fiche sur X »).
---

# Création d'une fiche Question/Réponse Magisterium

Tu es en charge de produire une fiche Q/R complète pour le site Magisterium. Cette fiche doit respecter rigoureusement le processus éditorial documenté dans `/home/realitix/git/catholique/site/CONTRIBUTING-QR.md`. Lis-le d'abord intégralement.

## Principes non négociables

1. **Langue source uniquement** pour les sources du corpus (latin, italien, anglais selon le document original). Aucune traduction scrapée n'est utilisée comme source primaire.
2. **Toutes les citations en langue étrangère DOIVENT être accompagnées d'une traduction française juste après** — sans exception, même pour une seule phrase.
3. **Posture apologétique traditionnelle assumée** : la lecture traditionnelle du magistère prime ; les tensions post-Vatican II sont nommées comme telles, tout en respectant la lettre des textes.
4. **Mode orchestrateur strict** : toutes les analyses lourdes sont déléguées à des agents opus lancés en parallèle via `run_in_background: true`. L'orchestrateur (toi) se contente de cadrer, vérifier et synthétiser.

## Flow d'exécution

### Étape 1 — Cadrer la question

- Formuler la question en une phrase interrogative claire en français.
- **Expliciter les exclusions** : « hors hypothèse de X », « en excluant Y ». C'est capital pour éviter que les agents répondent à côté.
- Identifier un **cas pastoral concret** (un musulman vivant en France, un ami bouddhiste, un grand-père qui n'a pas été baptisé, etc.) qui servira de test final.
- Lister les **notions techniques mobilisées** (ignorance invincible, baptême de désir, extra Ecclesiam, nécessité de la foi, etc.).

### Étape 2 — Identifier les corpus pertinents

- Lire `/home/realitix/git/catholique/magisterium/_metadata/concordance.jsonl` (11 thèmes) et identifier celui/ceux qui recoupent la question.
- Faire un `Grep` ciblé sur les mots-clés attendus (latin, italien, français) dans `magisterium/`.
- Lister explicitement les **gros textes** à traiter par agent dédié. Patterns récurrents :
  - Catéchisme du Concile de Trente (6 fichiers latins, ~148 000 mots)
  - Catéchisme de saint Pie X (16 fichiers italiens, ~68 000 mots)
  - Catéchisme de Bellarmin (6 fichiers italiens)
  - CEC editio typica latina 1997 (112 fichiers, ~224 000 mots)
  - Encycliques volumineuses (Mystici Corporis, Redemptoris Missio, Dominus Iesus)
  - Actes conciliaires majeurs (Vatican I Dei Filius, Lumen Gentium, Gaudium et Spes)

### Étape 3 — Lancer les agents opus en parallèle

Lancer TOUS les agents pertinents en une seule tournée via `run_in_background: true` (un `Agent` call par agent, tous dans le même message autant que possible). Exemples d'agents récurrents :

- `agent-trente` — analyse du Catéchisme du Concile de Trente
- `agent-pie-x` — analyse du Catéchisme de saint Pie X
- `agent-bellarmin` — analyse du Catéchisme de Bellarmin
- `agent-cec-latin` — analyse du CEC editio typica latina
- `agent-tradition` — synthèse pré-V2 (conciles, papes, curie avant 1962)
- `agent-v2-postv2` — synthèse V2 et post-V2 (LG, AG, GS, NA, CDF récent, papes post-V2)

### Étape 4 — Prompt type de chaque agent

Chaque prompt d'agent doit imposer :

1. **Citations en langue source + traduction française systématique**. La traduction suit immédiatement, dans un bloc `**Traduction :** « ... »` ou via `<Citation>` si MDX.
2. **Référence précise** : slug du document dans `index.jsonl` + numéro de paragraphe/ligne/question.
3. **Qualification dogmatique** : distinguer dogme défini / doctrine commune / opinion théologique / enseignement pastoral.
4. **Conclusion explicite** en fin de rapport : réponse directe à la question.
5. **Limite** 1000-1500 mots par rapport.
6. **Posture traditionnelle** respectant la lettre des textes.

Exemple de prompt squelette :

> Tu es un théologien catholique. Analyse le [NOM DU TEXTE] pour répondre à la question : [QUESTION, avec exclusions explicites].
>
> Fichiers à lire : [LISTE EXPLICITE DE CHEMINS].
>
> Consignes : citer en langue source avec traduction française systématique, référencer slug + paragraphe, distinguer dogme/doctrine commune/opinion, conclure explicitement. 1000-1500 mots.

### Étape 5 — Synthèse par l'orchestrateur

Une fois les N rapports reçus, produire la synthèse finale avec cette **structure rigide** (sept sections) :

1. **Contexte** — cadrage, enjeu pastoral.
2. **Enseignement traditionnel (pré-Vatican II)** — synthèse corpus pré-1962.
3. **Enseignement de Vatican II et post-V2** — synthèse textes conciliaires et postérieurs.
4. **Contradictions et tensions** — points de friction explicités.
5. **Réponse dogmatique** — position traditionnelle, avec qualification. Tableau de synthèse (dogme / doctrine commune / condamné) si pertinent.
6. **Comment répondre pastoralement** — paragraphe destiné à l'ami catholique concret de l'étape 1.
7. **Références** — liste des documents cités avec leurs slugs et dates.

### Étape 6 — Rédiger le markdown final

Fichier : `/home/realitix/git/catholique/site/src/content/questions/<slug>.md`.

Frontmatter **obligatoire** :

```yaml
---
title: "Titre lisible (peut dépasser les 70 caractères pour les questions longues)"
question: "La question complète, avec toutes les nuances et exclusions."
slug: "kebab-case-court"
date: "YYYY-MM-DD"
summary: "Résumé 1-2 phrases avec la réponse directe."
tags: ["mot-clé-1", "mot-clé-2"]
related_documents:
  - "slug1"     # doivent exister dans magisterium/_metadata/index.jsonl
  - "slug2"
related_themes:
  - "theme1"    # doivent exister dans magisterium/_metadata/concordance.jsonl
posture: "traditionnelle"
voices:
  conciliaire:
    tagline: "Phrase condensée entre guillemets."
    body: "60-90 mots, doctrinalement juste, montrant la prédication dominante qui euphémise la lettre des textes officiels."
    punchline: "Optionnelle, italique discret. Constat ironique sur les effets pastoraux."
  ecclesia_dei:
    tagline: "..."
    body: "60-90 mots. Les tradis ralliés (FSSP, ICRSP, IBP) équilibristes entre Florence et LG 16, courtois avec l'évêque."
    punchline: "Allusion à leur retenue diplomatique."
  fsspx:
    tagline: "..."
    body: "60-90 mots. Fraternité Saint-Pie X : tradition intégrale, canoniquement irrégulière, fermeté sur la lettre."
    punchline: "Fermeté sur la formule classique."
  sedevacantiste:
    tagline: "..."
    body: "60-90 mots. Résolution des contradictions par la vacance du Siège. Position explicitement assumée par ce site."
    punchline: "Auto-ironique sur l'isolement de la position."
---
```

**Points de vigilance pour le frontmatter** :

- `related_documents` : chaque slug DOIT exister dans `/home/realitix/git/catholique/magisterium/_metadata/index.jsonl`. Vérifier par grep avant de commit.
- `related_themes` : chaque slug DOIT exister dans `/home/realitix/git/catholique/magisterium/_metadata/concordance.jsonl`.
- `voices` : les **quatre courants** sont obligatoires et dans l'ordre canonique (conciliaire → ecclesia_dei → fsspx → sedevacantiste). Si on en omet un, on perd l'équilibre du panorama.

**Corps du markdown** :

- Structurer avec `##` (H2) pour les grandes sections (alimente la ToC) et `###` (H3) pour les sous-sections.
- Utiliser les blockquotes Markdown pour les citations :
  ```markdown
  > *« Citation en latin ou italien. »*
  >
  > **Traduction :** « Traduction française complète. »
  >
  > — *Source, référence*
  ```
- **Ne pas dupliquer** la liste des documents cités dans le corps : la grille en pied les affiche déjà automatiquement.

### Étape 7 — Revue par un agent reviewer

Lancer un dernier agent opus avec la checklist :

- [ ] Toutes les citations étrangères ont leur traduction française attenante.
- [ ] Tous les `related_documents` existent dans `index.jsonl`.
- [ ] Tous les `related_themes` existent dans `concordance.jsonl`.
- [ ] Les 4 voix sont présentes, dans l'ordre canonique, avec tagline + body.
- [ ] La posture est traditionnelle et cohérente d'une section à l'autre.
- [ ] Frontmatter YAML syntaxiquement valide.
- [ ] Sections canoniques toutes présentes (Contexte → Références).
- [ ] Build passe (`cd site && npm run build` sans erreur).

### Étape 8 — Test visuel

- Lancer `cd /home/realitix/git/catholique/site && npm run dev` en background.
- Ouvrir `http://localhost:4321/questions/<slug>/`.
- Vérifier :
  - ToC à droite affiche les bonnes sections avec scroll-spy actif.
  - Progress bar en haut.
  - Bouton retour en haut après scroll.
  - Grille des documents cités en pied, cartes regroupées par période.
  - Panorama des 4 voix en fin d'article.
  - Sur mobile (≤ 900px) : ToC remplacée par bouton « Plan » flottant.

## Composants Astro à connaître

- `site/src/components/Citation.astro` — composant de citation, accepte `lang`, `source`, `translation`, `doc_slug` (optionnel, rend la source cliquable vers le document).
- `site/src/components/FourVoices.astro` — panorama des 4 voix, alimenté par `voices` du frontmatter.
- `site/src/components/ToC.astro` — table des matières sticky + scroll-spy + overlay mobile.
- `site/src/components/ReadingProgress.astro` — barre de progression.
- `site/src/components/BackToTop.astro` — bouton retour en haut.
- `site/src/components/DocumentsGrid.astro` — grille enrichie des documents cités.
- `site/src/layouts/Question.astro` — layout qui orchestre tout ce qui précède.

## Anti-patterns à éviter

- **Ne pas citer sans traduire** — rédhibitoire.
- **Ne pas omettre une des 4 voix** — la cohérence visuelle du panorama exige les quatre.
- **Ne pas caricaturer les courants** dans le bloc `voices` — humour permis, déformation doctrinale interdite.
- **Ne pas rédiger soi-même les analyses** quand des agents opus peuvent être délégués en parallèle — c'est du gaspillage de temps.
- **Ne pas oublier les exclusions** dans la formulation de la question — sinon les agents répondent à côté.
- **Ne pas commit sans avoir fait tourner le build** et vérifié le rendu visuel.

## Mémoire complémentaire à consulter

- `/home/realitix/.claude/projects/-home-realitix-git-catholique/memory/feedback_always_translate_citations.md`
- `/home/realitix/.claude/projects/-home-realitix-git-catholique/memory/feedback_source_language_only.md`
- `/home/realitix/.claude/projects/-home-realitix-git-catholique/memory/feedback_orchestrator_mode.md`
