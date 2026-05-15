# Sources du corpus prêtres tradi

État au 13 mai 2026, après extension phase 6 (module ``tradi_official.py``).

## Volume final par fraternité

| Fraternité | Nombre | Source principale |
|---|---|---|
| FSSPX | 280 | laportelatine.org (sitemap personne + lieux) |
| sede-allie (CMRI alliés non-CMRI) | 24 | cmri.org directory of latin masses |
| CMRI | 14 | cmri.org directory + page priests-religious |
| MHTS / Cincinnati line | 9 | sgg.org page clergy |
| IBP | 8 | fr.wikipedia.org/wiki/Institut_du_Bon-Pasteur (fallback) |
| FSSP | 6 | fr.wikipedia.org (phase 6 originale) |
| sede (français) | 3 | fr.wikipedia.org (phase 6 originale) |
| **Total** | **344** | |

Note : 26 prêtres datent de la phase 6 originale (Wikipédia FR), 318 ont été
ajoutés ou enrichis par la phase 6 étendue (sites officiels).

## Sources publiques utilisées

- ``laportelatine.org`` — sitemap Yoast (``personne-sitemap.xml``, 326 URLs ;
  ``lieux-sitemap.xml``, 253 URLs). Parsing CSS + regex sur les pages
  ``elementor-element``.
- ``cmri.org`` — snapshot statique de la directory of traditional latin
  masses (Incapsula bloque le scraping direct).
- ``sgg.org/clergy/`` — parsing direct (HTTP 200 sans protection).
- ``fr.wikipedia.org`` — catégories Wikipédia + API ``parse&prop=wikitext`` +
  Wikidata SPARQL pour les dates.

## Couverture par champ

- **Slug ASCII normalisé** : 344/344 (100 %)
- **Fraternité** : 344/344 (100 %)
- **Affectation actuelle** : 230/344 (67 %)
- **Date d'ordination** : 6/344 (~2 %) — quasi exclusivement les fiches
  Wikipédia FR. Ni LPL ni CMRI ne publient les dates d'ordination dans leur
  annuaire.
- **Évêque ordinateur** : 8/344 — tous résolus en slug (8/8 = 100 % de
  résolution sur les ordinateurs identifiés).
- **Photo** : 0/344 — LPL utilise un placeholder ``Priere-sans-photo.png``
  générique pour quasi toutes les fiches ; les photos réelles sont dans les
  pages lieux mais sans alt-text exploitable.

## Sources écartées

Voir ``_metadata/tradi_scraping_notes.md`` pour le détail des sites testés
et écartés (FSSP, ICRSP, IBP, FSSPX international, FSSPX USA, SSPV).

## Schéma JSONL brut

Le fichier ``_raw/tradi.jsonl`` est append-only. Chaque ligne représente
un prêtre vu par UN scrape, avec ``source`` distinct selon le module
(``fsspx-laportelatine``, ``cmri-directory``, ``mhts-sgg``,
``ibp-wikipedia-fr``, ou format minimaliste de phase 6 originale).
La déduplication et la production des YAML est faite par
``tradi_official.py`` à chaque run.

## Relance

```bash
just clerge-tradi              # toutes les sources
just clerge-tradi-lpl          # LPL seulement (~9 min cold cache)
just clerge-tradi-refresh      # vide le cache et re-fetch tout
```

Ou directement :

```bash
uv run python -m scrapers.clerge.sources.tradi_official [--only lpl cmri sgg ibp] [-v]
```

Le scraper est idempotent : les YAML existants sont enrichis sans écraser
les données manuelles ; la fusion préserve les multiples affectations
(un même prêtre listé comme prieur d'un lieu + professeur d'un autre).
