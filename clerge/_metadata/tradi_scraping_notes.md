# Notes sur le scraping des prêtres tradi

Deux modules complémentaires :

- ``scrapers/clerge/sources/tradi.py`` (phase 6 initiale) — Wikipédia FR pour
  les prêtres notoires (fondateurs, supérieurs, signatures publiques) ; ~30
  prêtres.
- ``scrapers/clerge/sources/tradi_official.py`` (phase 6 étendue, mai 2026) —
  sites officiels des fraternités, parsing structuré pour atteindre 400-600
  prêtres.

## Sources testées et état en mai 2026

| Site | Domaine | État | Stratégie retenue |
|---|---|---|---|
| FSSPX France | laportelatine.org | ✅ Sitemap YOAST `personne-sitemap.xml` (~326 URLs) + `lieux-sitemap.xml` (~253 URLs) | Scraping direct via sitemap + parsing par CSS + regex sur les pages lieux. |
| CMRI | cmri.org | ⚠️ Incapsula CDN bloque les bots non-browser (HTML 205 octets) | Snapshot statique extrait via WebFetch (~38 entrées). À actualiser ponctuellement. |
| MHTS / Cincinnati line | sgg.org | ✅ Page `/clergy/` accessible | Parsing regex `Most Rev./Rev./Fr.` + nom propre. |
| IBP | institutdubonpasteur.org | ❌ ECONNREFUSED + ERR_TLS_CERT_ALTNAME_INVALID en mai 2026 | Fallback : 11 fondateurs et dirigeants nommés dans l'article Wikipédia FR sur l'IBP. |
| ICRSP | icrsp.org | ❌ Toutes les pages `/our-clergy/`, `/canons/`, `/clergy/`, `/our-priests/`, `/en/` répondent 404 | Sources à trouver — non couvert par ce module. |
| FSSP | fssp.org | ❌ Formulaire géolocalisé sans annuaire ; pas de liste publique des prêtres | Non couvert. |
| FSSPX international | fsspx.org | ❌ 403 sur WebFetch ; curl fonctionne mais redondant avec LPL pour la France | Non couvert (LPL suffit pour le périmètre francophone). |
| SSPV | sspv.net | ❌ Domaine parking commercial en mai 2026, plus de contenu clérical | Non couvert. |
| FSSPX USA | sspx.org | ❌ 403 sur WebFetch | Non couvert (CMRI couvre les sédés US). |
| FSSPX DE | fsspx.de | Non testé | À investiguer. |

## Schéma de sortie

Le scraper officiel append dans ``clerge/_raw/tradi.jsonl`` avec un schéma
enrichi (vs. le schéma minimaliste de phase 6) :

```json
{
  "source": "fsspx-laportelatine",
  "source_id": "abbe-jean-dupont",
  "source_url": "https://laportelatine.org/personne/abbe-jean-dupont",
  "name": "Jean Dupont",
  "honorific": "Abbé",
  "fraternite": "fsspx",
  "ordinateur_name": null,
  "ordinateur_slug": null,
  "ordination_date": null,
  "ordination_place": null,
  "current_assignment": "Prieuré Saint-Pie-X, Saint-Nicolas-du-Chardonnet, Paris",
  "image_url": null,
  "rang": "pretre",
  "fetched_at": "2026-05-13T18:00:00+00:00"
}
```

Pour CHAQUE entrée, génération idempotente d'un ``clerge/pretres/{slug}.yaml``
qui fusionne avec l'existant (créé par phase 6 / Wikipédia) sans écraser.

## Limites de couverture (mai 2026)

- **Ordinateur presbytéral** : LPL et CMRI ne le publient pas. Seuls les
  prêtres déjà couverts par Wikipédia FR ont cette info (~10 % du corpus).
  Le module pourrait être étendu en lisant les bulletins de district FSSPX
  (PDF mensuels) qui annoncent les nouvelles ordinations + leur consécrateur,
  mais c'est de l'OCR fragile.
- **Date d'ordination** : idem, indisponible dans les annuaires officiels.
- **Photo** : LPL utilise un placeholder "Priere-sans-photo" générique pour
  ~80 % des fiches personne ; les photos réelles sont sur les pages lieux
  mais sans alt-text exploitable.
- **ICRSP / FSSP / IBP / FSSPX DE / FSSPX USA** : pas d'annuaire scrappable
  fiable identifié en mai 2026.

## Suggestions de sources non encore essayées

- **fsspx.de** (district allemand) — peut publier un annuaire.
- **Bulletins de district FSSPX** (PDF mensuels téléchargeables depuis
  laportelatine.org/publications) : extraits OCR pour ordinations annuelles.
- **Pages Facebook/Twitter** des prieurés pour photos et événements.
- **archive.org snapshots** des pages icrsp.org plus anciennes (avant le
  refonte qui a cassé les URLs `/our-clergy/`).
- **Annuaire Una Voce France** (uvfrance.org) pour les Ecclesia Dei et
  diocésains tradi.
