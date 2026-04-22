---
name: translate-corpus
description: Traduit par IA les documents du corpus magistériel dans les langues manquantes (par défaut français + anglais). Orchestrateur qui spawn un agent opus par document à traduire, ou N agents pour les très gros documents découpés sur les sections markdown. Met à jour le bloc `traductions[lang]` avec `kind: ia`, `source_sha256`, `model`, `translated_at`. Idempotent — saute tout (doc, lang) déjà traduit dont la source n'a pas changé. À utiliser quand l'utilisateur demande de combler les trous de traduction du corpus (« traduis le corpus », « complète les traductions IA pour fr/en », « traduis le CEC en français »).
---

# Skill `translate-corpus` — traductions IA du corpus

Tu orchestres la traduction par IA des documents du corpus magistériel qui
n'ont pas encore de traduction officielle dans les langues cibles. **Toutes
les traductions que tu produis ont `kind: ia`**, bien distinguées des
`originale` (langue source de l'auteur) et `officielle` (traduction publiée
par la source faisant autorité, déjà scrapée par `tools.complete_translations`).

**Principe non négociable** : tu es orchestrateur strict. Tu ne traduis
**jamais** toi-même. Chaque document (ou chaque chunk de gros document) est
délégué à un **agent opus dédié** spawné via `Agent`. Tu te limites à
identifier ce qui doit être traduit, préparer les prompts, distribuer les
jobs, recueillir les retours, assembler, et persister.

## Flow d'exécution

### Étape 1 — Parser les arguments

Arguments attendus du skill (donnés par l'utilisateur, parsés en tête) :

- `--langs fr,en` : langues cibles, **défaut `fr,en`**.
- `--slug <slug>` : cibler un seul doc (utile pour test / urgence).
- `--source <path-relatif-sous-magisterium/>` : cibler un sous-corpus.
- `--limit N` : cap sur le nombre de docs à traiter cette session.
- `--dry-run` : liste ce qui serait traduit sans rien générer.

Si l'utilisateur ne précise rien → `--langs fr,en`, tout le corpus, pas de limit.

### Étape 2 — Scan du corpus et identification des trous

Utilise `tools/translate_scan.py` (que tu lances via `uv run python -m tools.translate_scan ...`)
pour obtenir la liste JSONL des `(slug, lang, source_lang, source_sha256, source_path, source_tokens)`
qui ont besoin d'être traduits. Ce script :

1. Itère `magisterium/_metadata/index.jsonl`.
2. Pour chaque doc, pour chaque langue cible, regarde `traductions[lang]` :
   - absente → à traduire.
   - présente avec `kind in {originale, officielle}` → **sauter** (on ne remplace jamais une trad officielle par une IA).
   - présente avec `kind: ia` : comparer `traductions[lang].source_sha256` à la sha256 actuelle de la source → sauter si égal, sinon retraduire.
3. Choisit la source : `langue_originale` si disponible et `kind in {originale, officielle}`, sinon une officielle au choix (priorité fr > en > it > de > es).
4. Mesure la taille de la source (tokens approximés = `chars / 3.5`) pour décider de la stratégie de découpe.
5. Sort la liste de jobs.

### Étape 3 — Stratégie de découpe par taille

**Seuil** : 25 000 tokens source (~87 500 caractères).

- **Sous le seuil** : 1 agent par `(slug, lang)`, qui reçoit le markdown entier.
- **Au-dessus du seuil** : découpe sur les frontières `##` du markdown. N agents,
  un par chunk, traduisent en parallèle. L'orchestrateur assemble par
  concaténation dans l'ordre des chunks.

Pour les chunks 2…N, joindre au prompt un **rappel terminologique** issu
des dernières lignes traduites du chunk précédent (déjà produites), pour
garantir la cohérence.

### Étape 4 — Spawn des agents (parallélisme)

Lancer les agents en lots via `Agent(... run_in_background: true)` — au
maximum **8 en vol simultanés** pour ne pas saturer. Quand un agent se
termine, en relancer un autre. Chaque agent :

- **subagent_type** : general-purpose
- **model** : opus
- **prompt** : construit à partir du template (voir section Prompt agent).

**Critique** : chaque prompt d'agent embarque le **glossaire complet**
(section Glossaire ci-dessous). Pas de prompt caching — le glossaire est
petit relativement aux documents. On privilégie la simplicité.

### Étape 5 — Collecte et assemblage

- Chaque agent écrit sa traduction dans un fichier temporaire
  `magisterium/_translations_tmp/<slug>.<lang>.chunk-<N>.md` (ou `.full.md`
  pour un seul chunk).
- L'orchestrateur, une fois tous les chunks d'un `(slug, lang)` reçus sans
  erreur, concatène dans l'ordre et écrit le markdown final
  `<path-du-doc>/<slug>.<lang>.md`.
- Calcule le sha256 du fichier final, met à jour `meta.yaml` :
  ```yaml
  traductions:
    <lang>:
      kind: ia
      sha256: <sha256 du .md final>
      model: <nom du modèle, ex. claude-opus-4-7>
      translated_from: <lang source>
      source_sha256: <sha256 du .md source>
      translated_at: <now ISO>
  ```
  Puis resynchronise `langues_disponibles` et `sha256` via `DocMeta.sync_legacy_fields`.
- Nettoie les fichiers temporaires.

### Étape 6 — Rebuild index

À la fin de la session : `just build-index` pour que le site voie les nouvelles trads.

## Prompt agent (template)

Chaque agent reçoit un prompt ainsi construit :

```
Tu es un théologien catholique traducteur. Traduis le markdown ci-dessous
de la langue [SOURCE_LANG_NAME] vers [TARGET_LANG_NAME].

CONSIGNES STRICTES (non négociables) :

1. Conserve EXACTEMENT la structure markdown : titres (# ##), listes, blocs
   de citation (>), emphase (* _), liens, code inline.
2. Préserve la numérotation interne des documents (canons, articles,
   paragraphes) sans la renuméroter.
3. Ne traduis PAS les citations scripturaires dans leur référence
   (« Mt 5, 3 » reste « Mt 5, 3 »), mais traduis le texte cité lui-même.
4. Respecte impérativement le glossaire théologique fourni ci-dessous.
5. Pour les termes latins intraduisibles (motu proprio, ex cathedra,
   Novus Ordo, subsistit in, anathema sit…), utilise la forme latine et
   ajoute une traduction française entre parenthèses la première fois, puis
   garde le latin seul ensuite.
6. Ne paraphrase pas. Ne reformule pas les canons dogmatiques. Traduction
   littérale privilégiée.
7. Ne modernise pas les formulations dogmatiques. « Qu'il soit anathème »
   ne devient pas « qu'il soit exclu » ou « qu'il soit rejeté ».
8. Si tu rencontres un passage ambigu, traduis littéralement sans deviner.
9. Réponds UNIQUEMENT avec le markdown traduit, rien d'autre. Pas de
   préambule, pas de commentaire, pas d'explication.

GLOSSAIRE IMPOSÉ (termes → traduction obligatoire) :

[GLOSSAIRE SELON LA LANGUE CIBLE]

[CONTEXTE DE LIAISON, si chunk N>1 :
Dernières lignes traduites du chunk précédent, à titre de rappel stylistique
et terminologique. Ne pas les re-traduire ni les inclure dans ta sortie.]

MARKDOWN SOURCE :

[BODY]
```

## Glossaires

Le glossaire est embarqué dans le prompt de chaque agent. Il vit dans
`.claude/skills/translate-corpus/glossaires/` — un fichier `.md` par langue cible :

- `fr.md` — latin/grec/italien/anglais → français
- `en.md` — latin/grec/italien/français → anglais
- `es.md` — latin/grec/italien/anglais → español

Exemples d'entrées obligatoires (pour `fr.md`) :

```
gratia habitualis → grâce habituelle
gratia sanctificans → grâce sanctifiante
gratia actualis → grâce actuelle
anathema sit → qu'il soit anathème
subsistit in → subsiste dans
extra Ecclesiam nulla salus → hors de l'Église, pas de salut
ex cathedra → ex cathedra (invariant)
motu proprio → motu proprio (invariant)
baptismus flaminis → baptême de désir
baptismus sanguinis → baptême de sang
Novus Ordo Missae → Novus Ordo Missae (invariant)
transubstantiatio → transsubstantiation
consubstantialis Patri → consubstantiel au Père
Filioque → Filioque (invariant)
communio sanctorum → communion des saints
oeconomia salutis → économie du salut
```

## Invariants doctrinaux à surveiller à la relecture

- Les canons conciliaires (Trente, Vatican I) préservent « Si quelqu'un
  dit… qu'il soit anathème ».
- « Hors de l'Église, pas de salut » n'est jamais édulcoré.
- « Transsubstantiation » n'est jamais remplacé par « conversion » seul.
- Les dogmes mariaux gardent leur formulation technique (Immaculée
  Conception, Assomption corporelle).

## Commandes à lancer par l'orchestrateur

```bash
# 1. Scanner le corpus pour lister les jobs
uv run python -m tools.translate_scan --langs fr,en [--slug ... | --source ... | --limit N] [--dry-run]

# 2. [Orchestrateur] spawn agents, collecte résultats (cf. flow ci-dessus)

# 3. Après ingestion de toutes les traductions produites
uv run python -m tools.build_index
```

## Règles éditoriales rappelées

- Posture apologétique traditionnelle : ne jamais adoucir les dogmes, ne
  jamais moderniser les condamnations.
- Le texte de référence reste la version originale. Toute trad IA est
  marquée comme telle dans le site (bandeau + chip).
- Si un agent renvoie un texte qui a l'air incomplet, tronqué, ou qui dévie
  du glossaire : relancer avec un warning, ne JAMAIS concaténer un chunk
  suspect.
