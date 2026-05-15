# Corpus prosopographique du clergé catholique

Données structurées sur les évêques et prêtres catholiques, utilisées par le
site Magisterium pour exposer la **succession sacramentelle**, le **rite** de
chaque consécration / ordination, l'**obédience** et la **lignée ascendante**
de chaque clerc.

L'enjeu doctrinal est documenté dans la fiche Q/R `validite-rite-episcopal-1968`
(rite de Paul VI = douteux à la lettre des textes infaillibles). Ce corpus
applique mécaniquement ce verdict aux clercs : c'est le rite réellement utilisé
qui parle, sans verdict ajouté par le site.

## Arborescence

```
clerge/
├── eveques/{slug}.yaml      # une fiche par évêque (~30k)
├── pretres/{slug}.yaml      # une fiche par prêtre tradi/Ecclesia Dei/sédé/diocésain glanable
├── photos/{slug}.{jpg,png}  # photos téléchargées localement (Wikimedia Commons surtout)
├── _raw/
│   ├── wikidata.jsonl       # sortie brute phase 1
│   ├── catholic_hierarchy.jsonl  # sortie brute phase 2
│   ├── gcatholic.jsonl      # sortie brute phase 3
│   └── tradi_*.jsonl        # sortie brute phase 6
└── _metadata/
    ├── clerics.jsonl        # index plat pour la recherche MiniSearch (1 ligne/clerc)
    ├── consecrations.jsonl  # arêtes évêque → évêque (qui a consacré qui)
    ├── ordinations.jsonl    # arêtes évêque → prêtre
    ├── lineages.json        # chemins ascendants pré-calculés par clerc
    └── stats.json           # couverture, % rite annoté, % photo, etc.
```

## Conventions de slug

- `nom-prenom` en minuscules ASCII, tirets, sans accents
- Désambiguïsation par année de naissance si collision : `pierre-martin-1875`
- Slug stable : ne JAMAIS renommer une fois publié (URLs cassées sinon)
- Pour les évêques connus sous un nom religieux : `nom-religieux` (ex. `marcel-lefebvre`)

## Schéma `eveques/{slug}.yaml`

```yaml
slug: marcel-lefebvre
nom: Marcel Lefebvre
nom_complet: Marcel François Marie Joseph Lefebvre
naissance: 1905-11-29
deces: 1991-03-25
naissance_lieu: Tourcoing, France
deces_lieu: Martigny, Suisse
rang: eveque                 # eveque | archeveque | cardinal | patriarche | pape
sacre:
  date: 1947-09-18
  lieu: Tourcoing
  rite: ancien               # ancien | nouveau | mixte | inconnu
  rite_source: inferred      # explicit | inferred | manual
  consecrateur_principal: achille-lienart
  co_consecrateurs:
    - jean-baptiste-fauret
    - alfred-ancel
  source_urls:
    - https://www.catholic-hierarchy.org/bishop/blefe.html
obediences:                  # ordre chronologique
  - { du: 1929-09-21, au: 1968, statut: rome }
  - { du: 1970-11-01, au: 1991-03-25, statut: fsspx-fondateur }
fonctions:
  - { titre: archeveque-dakar, siege: Dakar, du: 1955-09-14, au: 1962-01-23 }
  - { titre: superieur-general, institut: Spiritains, du: 1962, au: 1968 }
  - { titre: fondateur, institut: FSSPX, du: 1970-11-01, au: 1991-03-25 }
fraternite: fsspx            # rome | fsspx | fssp | icrsp | ibp | sede-* | vieux-cath | orient-* | aucune
nationalite: FR
photo:
  fichier: marcel-lefebvre.jpg
  source: wikimedia-commons
  source_url: https://commons.wikimedia.org/wiki/File:...
  licence: CC-BY-SA-4.0
  auteur: Auteur Original
qids:                         # identifiants externes pour cross-ref
  wikidata: Q316395
  catholic_hierarchy: blefe
  gcatholic: blefe
sources:                      # journal de réconciliation
  - { source: wikidata, fetched_at: 2026-05-13T10:00:00Z, completeness: 0.7 }
  - { source: catholic-hierarchy.org, fetched_at: 2026-05-13T11:00:00Z, completeness: 0.95 }
  - { source: gcatholic.org, fetched_at: 2026-05-13T11:30:00Z, completeness: 0.6 }
notes: |
  Champs libres si nécessaire — par défaut vide.
```

## Schéma `pretres/{slug}.yaml`

Version allégée. Pas de `sacre`, mais un bloc `ordination` (presbytérale) qui
référence l'évêque ordinateur (ID d'évêque ou inconnu) + le rite.

```yaml
slug: jean-dupont-1965
nom: Jean Dupont
naissance: 1942-03-12
ordination:
  date: 1968-06-29
  lieu: Versailles
  rite: ancien
  rite_source: inferred
  ordinateur: marcel-lefebvre    # peut être null si inconnu
  source_urls: [https://...]
fraternite: fsspx
fonctions:
  - { titre: pretre-prieur, lieu: Saint-Nicolas-du-Chardonnet, du: 2010, au: null }
photo: { ... }                  # même schéma
sources: [...]
```

## Schéma JSONL des arêtes

`consecrations.jsonl` (1 par sacre épiscopal) :

```json
{"consacre":"marcel-lefebvre","consecrateur_principal":"achille-lienart","co_consecrateurs":["jean-baptiste-fauret","alfred-ancel"],"date":"1947-09-18","rite":"ancien","rite_source":"inferred","sources":["catholic-hierarchy.org"]}
```

`ordinations.jsonl` (1 par ordination presbytérale connue) :

```json
{"ordonne":"jean-dupont-1965","ordinateur":"marcel-lefebvre","date":"1968-06-29","rite":"ancien","rite_source":"inferred","sources":["fsspx.org"]}
```

## Schéma JSONL brut `_raw/*.jsonl`

Chaque agent de scraping (phases 1, 2, 3, 6) produit du JSONL **dénormalisé**
contenant **tout ce qu'il a trouvé** dans sa source, sans réconciliation.
Format flexible mais champs canoniques recommandés :

```json
{
  "source": "wikidata",
  "source_id": "Q316395",
  "names": ["Marcel Lefebvre", "Marcel François Marie Joseph Lefebvre"],
  "birth_date": "1905-11-29",
  "death_date": "1991-03-25",
  "consecrator_qids": ["Q123..."],
  "co_consecrator_qids": [],
  "consecration_date": "1947-09-18",
  "image_url": "https://commons.wikimedia.org/...",
  "image_license": "CC-BY-SA-4.0",
  "positions": [...],
  "religious_order_qid": "Q...",
  "raw": { ... payload entier du SPARQL ... }
}
```

L'unification se fait en **phase 4 (réconciliation)** par un outil dédié
(`tools/clerge_reconcile.py`), pas par les agents de scraping.

## Heuristique de détermination du rite

Ordre d'évaluation (premier match gagne) :

1. **Annotation manuelle** présente dans une table de cas frontières → utiliser
2. **Date de consécration ≤ 1968-06-29** → `rite=ancien`, `rite_source=inferred`
3. **Consécrateur principal appartient à FSSPX, sédévacantistes, vieux-cath.,
   communauté tradi schismatique** → `rite=ancien`
4. **Consécrateur principal en communion avec Rome ET date ≥ 1968-06-30** →
   `rite=nouveau` par défaut, sauf si consécrateur est Ecclesia Dei avec
   indult (cas frontière → annotation manuelle)
5. Sinon → `rite=inconnu`

Pour la propagation (tampon de la fiche du clerc) :

- **ordo_validus** : sacre ET tous les consécrateurs ascendants jusqu'à un
  ancêtre pré-1968 ont `rite=ancien`
- **ordo_dubius** : au moins une consécration `rite=nouveau` dans la chaîne
  ascendante
- **ordo_incertus** : au moins une consécration `rite=inconnu` et aucune
  `rite=nouveau` dans la chaîne

## Rate limits et politesse

- Wikidata SPARQL endpoint : 5 req/s max, User-Agent identifiable
- catholic-hierarchy.org : 1 req/s max, User-Agent identifiable
- gcatholic.org : 1 req/s max
- Wikimedia Commons (photos) : 1 req/s max

Réutiliser `scrapers/core/rate_limit.py` (`GLOBAL_LIMITER`) qui gère ça
par domaine.

## Idempotence

- Chaque scraper doit pouvoir reprendre où il s'est arrêté
- `_raw/*.jsonl` est append-only ; vérifier `source_id` déjà présent avant de
  re-fetch
- La réconciliation peut être ré-exécutée sans perte (les YAML sont régénérés)
- Les annotations manuelles sont préservées dans un fichier dédié
  `clerge/_metadata/manual_overrides.yaml` que la réconciliation lit en
  dernier
