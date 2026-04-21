# Création d'une fiche Question/Réponse — Processus

## Objectif

Ce document décrit le processus reproductible de création d'une fiche Question/Réponse (Q/R) pour le site Magisterium. Chaque fiche s'appuie exclusivement sur le corpus magistériel local (1304 documents, langue source uniquement) et adopte une **posture apologétique traditionnelle assumée** : la lecture traditionnelle du magistère est privilégiée, et les tensions post-Vatican II sont explicitement traitées comme des problèmes à résoudre, dans le respect de la lettre des textes. Documenter ce flow permet d'assurer la cohérence éditoriale, la traçabilité des citations, et la reproductibilité par délégation parallèle à des agents opus.

## Pré-requis

- Corpus magistériel ingéré dans `/home/realitix/git/catholique/magisterium/` — confirmer via `just stats` (1304 documents attendus).
- Fichiers `/home/realitix/git/catholique/magisterium/_metadata/index.jsonl` et `/home/realitix/git/catholique/magisterium/_metadata/concordance.jsonl` à jour.
- Environnement Claude Code avec agents opus disponibles et mode orchestrateur actif (Claude principal orchestre et vérifie, les agents exécutent).
- Règle éditoriale fondamentale : **toute citation en langue étrangère (latin, italien, anglais, allemand) doit être immédiatement suivie d'une traduction française**. Le corpus lui-même est conservé en langue source uniquement — aucune traduction n'est téléchargée.

## Flow en 8 étapes

### 1. Formulation de la question

Une question bien posée conditionne la qualité de la fiche. Règles :

- Énoncer la question en une phrase interrogative claire en français.
- Expliciter ce qui est inclus et ce qui est exclu (par exemple : « Le salut par la loi naturelle inclut-il les non-baptisés adultes de bonne volonté ? »).
- Donner un cas concret pastoral (un ami, un catéchumène, un membre de la famille) qui sert de test final : la fiche doit répondre à cette situation.
- Identifier a priori les notions techniques mobilisées (baptême de désir, ignorance invincible, extra Ecclesiam nulla salus, etc.) pour orienter la recherche.

### 2. Identification des corpus pertinents

- Lire `concordance.jsonl` pour lister les thèmes proches de la question et noter les slugs de documents associés.
- Utiliser `Grep` sur les mots-clés latins, italiens, français attendus (par exemple : `salus`, `fides`, `gratia`, `ignorantia invincibilis`).
- Dresser la liste des « gros textes » à analyser séparément : Catéchisme du Concile de Trente, Catéchisme de saint Pie X, Catéchisme de Bellarmin, CEC latin 1997, encycliques volumineuses, actes conciliaires.
- Distinguer les trois familles : Tradition pré-1962, Vatican II + post-V2, catéchismes de référence.

### 3. Délégation parallèle aux agents opus

L'orchestrateur lance **un agent opus par corpus** en parallèle via `run_in_background: true`. Les agents typiques :

- Agent « Tradition pré-V2 » : conciles, papes, CDF avant 1962.
- Agent « Vatican II + post-V2 » : actes de V2, papes post-1962, CDF récent.
- Un agent dédié **par gros texte** : agent Trente, agent Pie X, agent Bellarmin, agent CEC latin, agent encyclique X.
- Agent « FSSPX / sedes » si la question touche les tensions ecclésiologiques post-V2.

L'orchestrateur ne lance jamais un agent séquentiellement quand le parallélisme est possible : c'est la clé du gain de temps.

### 4. Consignes à donner aux agents

Chaque prompt d'agent doit inclure explicitement :

- **Citer en langue source + traduction française systématique** (latin, italien, anglais, allemand → traduction française immédiate, entre parenthèses ou sur la ligne suivante).
- **Référence précise** pour chaque citation : slug du document (tel qu'il apparaît dans `index.jsonl`) + paragraphe ou numéro de ligne.
- **Qualification dogmatique** : distinguer explicitement dogme défini, doctrine commune, opinion théologique, enseignement pastoral.
- **Conclusion explicite** à la fin de la fiche de l'agent : réponse directe à la question, sans ambiguïté.
- **Limite de longueur** : 1000 à 1500 mots par rapport d'agent (au-delà, demander explicitement un recadrage).
- **Posture** : traditionnelle assumée, mais fidèle à la lettre des textes cités.

### 5. Synthèse par l'orchestrateur

L'orchestrateur reçoit les N rapports d'agents et produit la synthèse finale. Structure imposée :

1. **Contexte** — cadrage de la question, enjeu pastoral.
2. **Enseignement traditionnel** — synthèse des corpus pré-V2.
3. **Enseignement V2 et post-V2** — synthèse des corpus récents.
4. **Contradictions et tensions** — identification explicite des points de friction.
5. **Réponse dogmatique** — position traditionnelle défendue, avec qualification.
6. **Comment répondre pastoralement** — le paragraphe destiné à l'ami catholique concret identifié à l'étape 1.

Un tableau de synthèse (colonnes : source, date, position, citation-clé) est ajouté si pertinent pour les questions contentieuses. La posture apologétique traditionnelle est assumée tout au long.

### 5 bis. Panorama des quatre voix catholiques — obligatoire sur chaque fiche

À la fin de chaque fiche, **obligatoirement**, une section automatique rendue par le composant `FourVoices.astro` présente la position condensée de chacun des quatre grands courants catholiques contemporains. Cette section est alimentée par le champ `voices` du frontmatter (détails à l'étape 6). Elle existe pour deux raisons : allège le ton d'un site consacré à un sujet lourd (la rupture conciliaire), et donne au lecteur une carte d'identité doctrinale lisible de la controverse.

**Ordre canonique, toujours respecté :** conciliaire → ex-Ecclesia Dei → FSSPX → sédévacantiste. Du plus large au plus strict.

**Ton de chaque voix** — à respecter d'une fiche à l'autre pour la cohérence :

- **Rome post-conciliaire** : décrit la prédication pastorale dominante, souvent laxiste, par contraste avec la lettre des textes officiels qu'elle euphémise. Punchline = constat ironique sur les effets pastoraux (évangélisation effondrée, « accompagnement » au lieu de conversion, etc.).
- **Ex-Ecclesia Dei** (tradis ralliés : FSSP, ICRSP, IBP, Bon Pasteur) : équilibristes entre la lettre de Florence et LG 16, courtois avec l'évêque. Punchline = allusion à la retenue diplomatique de leur position publique.
- **Fraternité Saint-Pie X** (« Saint-Piedis » d'Écône) : tient la tradition antérieure intégralement, refus du modernisme, canoniquement irrégulière. Punchline = fermeté assumée sur la formule classique.
- **Sédévacantistes** : résolvent les contradictions V2 par l'hypothèse que le Siège est vacant. Punchline = ironique sur l'isolement ou la cohérence de la position. **Ne jamais laisser entendre que le site défend cette position** — le ton reste factuel et le lecteur tire ses conclusions.

**Règles de rédaction :**

- Chaque voix : une **tagline** (phrase condensée entre guillemets), un **body** de 60 à 90 mots doctrinalement juste, une **punchline** optionnelle en italique discret.
- **Humour** tradi, sec, jamais vulgaire. La punchline est un bonus — mieux vaut pas de punchline qu'une pénible.
- **Fond doctrinal** : techniquement juste, même condensé. Pas de caricature grossière — l'humour vient de la formulation, pas de la déformation.
- **Symboles et couleurs** : gérés par le composant (✦ gris conciliaire, ☩ doré Ecclesia Dei, ✠ cuivré FSSPX, † bordeaux sédévacantiste). Ne rien redéfinir dans la fiche.

### 6. Création du markdown dans le site

Le fichier final est placé à `/home/realitix/git/catholique/site/src/content/questions/<slug>.md`. Frontmatter YAML requis :

```yaml
---
title: "Titre lisible"
question: "La question complète"
slug: "kebab-case-slug"
date: "YYYY-MM-DD"
summary: "Résumé 1-2 phrases"
tags: [...]
related_documents: ["slug1", "slug2"]  # slugs présents dans index.jsonl
related_themes: ["theme1"]             # slugs présents dans concordance.jsonl
posture: "traditionnelle"
voices:
  conciliaire:
    tagline: "Phrase condensée entre guillemets."
    body: "60 à 90 mots, doctrinalement juste."
    punchline: "Optionnelle, italique discret."
  ecclesia_dei:
    tagline: "..."
    body: "..."
    punchline: "..."
  fsspx:
    tagline: "..."
    body: "..."
    punchline: "..."
  sedevacantiste:
    tagline: "..."
    body: "..."
    punchline: "..."
---
```

Pour chaque citation en langue étrangère, utiliser le composant Astro `<Citation>` situé à `/home/realitix/git/catholique/site/src/components/Citation.astro` — il prend en charge le texte source, la traduction française, et la référence au document d'origine.

Le bloc `voices` alimente automatiquement la section « Panorama des quatre voix catholiques » en bas de la fiche (voir étape 5 bis). Il est obligatoire, dans l'ordre canonique. Schéma Zod côté site : `/home/realitix/git/catholique/site/src/content/config.ts`. Composant de rendu : `/home/realitix/git/catholique/site/src/components/FourVoices.astro`.

### 6 bis. Composants automatiques sur chaque fiche

Le layout `Question.astro` injecte automatiquement, sans rien à faire côté markdown :

- **Table des matières sticky à droite** (`ToC.astro`) — générée depuis les `H2`/`H3` du markdown via `entry.render().headings`. Scroll-spy via `IntersectionObserver` : la section en cours de lecture est surlignée en bordeaux. Sur mobile, la ToC devient un bouton flottant « Plan » en bas à droite, qui ouvre un panneau latéral.
- **Barre de progression de lecture** (`ReadingProgress.astro`) — filet de 2 px en haut de l'écran qui se remplit proportionnellement au scroll dans l'article.
- **Bouton « Retour en haut »** (`BackToTop.astro`) — apparaît au-delà de 800 px de scroll.
- **Grille de documents cités enrichie en pied** (`DocumentsGrid.astro`) — cartes par document avec titre, auteur, date, type, regroupées par période (pré-V2 / V2 / post-V2 / FSSPX). Les `related_documents` du frontmatter alimentent cette grille directement.
- **Citations cliquables** (`Citation.astro`) — si le composant reçoit une prop `doc_slug`, la source devient un lien vers `/documents/<slug>/`. Privilégier cette forme pour les citations majeures, afin d'offrir un chemin direct vers le texte intégral.

Conséquence pour le rédacteur :

1. Structurer la fiche avec des `##` (H2) pour les grandes sections et des `###` (H3) pour les sous-sections. C'est cette hiérarchie qui alimente la ToC.
2. Nommer les H2/H3 avec des titres courts et explicites (ils apparaîtront tels quels dans la ToC).
3. Ne pas dupliquer les liens vers les documents cités dans le corps du markdown : la grille en pied les présente déjà. En revanche, utiliser `doc_slug` sur les composants `Citation` quand la source est dans le corpus, pour créer le lien contextuel.

Pattern Citation recommandé :

```mdx
<Citation
  lang="la"
  source="Florence, Cantate Domino, 1442"
  doc_slug="1431_florence"
  translation="..."
>
  Firmiter credit, profitetur et praedicat...
</Citation>
```

Notes techniques :

- `entry.render()` est appelé dans `site/src/pages/questions/[slug].astro` et transmet `headings` au layout.
- Les IDs sur les H2/H3 sont auto-générés par Astro (slug à partir du texte). Pas besoin d'ajouter `{#id}` manuellement.
- Le scroll-spy utilise un `rootMargin` de `-15% 0% -60% 0%` pour activer la section quand son haut est entre 15% et 40% du viewport — c'est le comportement standard des docs (Astro, MDN).

### 7. Revue par un agent reviewer

Un agent opus final est lancé en rôle de reviewer. Sa checklist :

- Toutes les citations étrangères ont leur traduction française attenante.
- Tous les `related_documents` existent effectivement dans `index.jsonl`.
- Tous les `related_themes` existent effectivement dans `concordance.jsonl`.
- Cohérence dogmatique : aucune contradiction interne entre les sections.
- Posture conforme : traditionnelle assumée, lettre des textes respectée.
- Frontmatter YAML valide (champs obligatoires présents, date au format ISO).
- **Bloc `voices` présent, complet (les quatre courants dans l'ordre canonique), et respectant le ton attendu** pour chaque courant (voir étape 5 bis).

Le reviewer retourne un rapport de conformité. L'orchestrateur corrige les écarts avant passage à l'étape 8.

### 8. Build et test

Commandes finales :

- `just build-site` — doit passer sans erreur Astro ni erreur de contenu.
- `just dev-site` — pour vérifier le rendu localement.
- Test visuel optionnel via Chrome MCP ou Playwright MCP pour contrôler la typographie, l'affichage des citations, les liens vers les documents sources.

## Annexe — Exemple : fiche « Salut par la loi naturelle »

La première fiche produite selon ce processus a traité la question du salut des non-baptisés adultes de bonne volonté. Les agents opus lancés en parallèle ont été :

- **Agent Trente** : analyse du Catéchisme du Concile de Trente (6 fichiers latins, environ 148 000 mots).
- **Agent Pie X** : analyse du Catéchisme de saint Pie X (16 fichiers italiens, environ 68 000 mots).
- **Agent Bellarmin** : analyse du Catéchisme de Bellarmin (6 fichiers italiens).
- **Agent CEC latin** : analyse du CEC editio typica latina 1997 (112 fichiers, environ 224 000 mots).
- Les agents transversaux **Tradition pré-V2** et **Post-V2** étaient déjà exécutés sur les conciles, les papes et la CDF.

Chaque agent a retourné une fiche structurée avec citations latines ou italiennes systématiquement traduites en français, conclusion explicite, et qualification dogmatique. L'orchestrateur a produit la synthèse finale d'environ 1500 mots, organisée selon les six sections canoniques, avec une **réponse pastorale adressée à des amis catholiques concrets**. Cette structure devient le patron des fiches suivantes.

## Troubleshooting

### Les agents retournent des citations sans traduction

Rappeler la règle explicitement dans le prompt et référer au fichier mémoire `feedback_always_translate_citations.md`. Renvoyer le rapport à l'agent avec la consigne de corriger les citations non traduites.

### Le slug n'existe pas dans `index.jsonl`

Relancer `just build-index` pour régénérer l'index. Vérifier que le document est bien scrapé dans `/home/realitix/git/catholique/magisterium/` ; si absent, relancer le scraper concerné.

### L'agent produit un résultat trop long

Le limiter explicitement dans le prompt : « Rapporte en 1000 à 1500 mots maximum ». Si le rapport dépasse, demander un résumé ciblé sur la question — ne pas tronquer manuellement, cela romprait la cohérence des citations.

### Un `related_theme` n'existe pas dans `concordance.jsonl`

Soit le thème est réel mais mal orthographié (corriger le slug), soit il doit être ajouté à la concordance — relancer `just build-concordance` après enrichissement, ou retirer le thème de la fiche.
