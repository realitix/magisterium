# Catéchisme de Canisius (1555) — MANQUANT (OCR inadéquat)

**Œuvre de référence :** *Summa doctrinae christianae*, saint Pierre
Canisius SJ (1521-1597), édition princeps Vienne 1555. Deux versions
plus courtes suivront : *Catechismus Minor* (1556) et *Catechismus
Parvus Catholicorum* (1558).

## Absent de la V1 — toutes les sources sont des OCR de mauvaise qualité

Réexamen en Phase 10 (21-04-2026). Aucune édition HTML propre n'existe en
ligne ; seules des numérisations PDF ou leurs OCR DjVu sont disponibles,
et toutes présentent des défauts systémiques (majuscules ornées lues en
cyrillique, coupures mid-mot, pieds de page `Digitized by Google`,
séparateurs décoratifs interprétés comme texte). Une ingestion fidèle
exigerait une relecture manuelle complète du latin — hors scope V1.

### Sources testées en Phase 10

| Source | Type | État | Remarque |
|--------|------|------|----------|
| `la.wikisource.org/wiki/Summa_doctrinae_christianae` | HTML | 404 | page absente |
| `documentacatholicaomnia.eu/04z/z_1521-1597__Canisius__Summa_Doctrinae_Christianae__LT.pdf.html` | HTML wrapper | 200 | ne sert qu'un lien vers le PDF (19,5 Mo), pas d'équivalent texte |
| `documentacatholicaomnia.eu/03d/1521-1597,_Canisius,_Summa_Doctrinae_Christianae,_LT.pdf` | PDF facsimilé | 200 (19,5 Mo) | trop volumineux pour WebFetch ; téléchargé par curl, contenu scanné |
| `archive.org/details/summa-doctrinae-christianae` | OCR + PDF | disponible | édition 1823 (J. Thomann, Landshut) ; OCR DjVu 527 Ko globalement lisible mais pollué par cruft de mise en page, lignes cassées, résidus « Digitized by Google » — besoin de segmentation manuelle |
| `archive.org/details/summadoctrinaec00kanigoog` | OCR + PDF | disponible | édition 1764 Viennae ; OCR **très dégradé** (caractères cyrilliques intercalés, séparateurs ornementaux pris pour du texte) |
| `archive.org/details/bub_gb_FmRS1Mew2g0C` | OCR + PDF | disponible | édition 1583 Basa, langue détectée « russe » (erronée), OCR ~800 Ko inexploitable |
| `archive.org/details/bub_gb_kzGIOIY_1uYC` | OCR + PDF | disponible | édition Anvers 1587 (Plantin) via Google Books ; OCR poor-to-fair |
| `archive.org/details/summa_doctrinae_christianae_1-4_1834-petri_canisii` | PDF 4 tomes | disponible | édition critique 1834 ; candidat V2 sérieux |
| `archive.org/details/legrandcatchisme04cani` | OCR | disponible | **français** — hors scope (langue source uniquement) |
| `quod.lib.umich.edu/.../A69066` (EEBO) | HTML | disponible | **anglais** 1622 — hors scope |
| `thelatinlibrary.com` | — | absent | aucune entrée Canisius |
| `intratext.com` | — | absent | aucune entrée Canisius |
| `corpuscorporum.org` | — | absent | aucune entrée |
| `maranatha.it` | — | absent | le site italien héberge Pie X mais pas Canisius |

### Pistes V2

1. Partir de `archive.org/details/summa-doctrinae-christianae` (édition
   1823) ou du 4 tomes 1834, appliquer un post-traitement OCR avec
   relecture manuelle des tituli (`DE FIDE ET SYMBOLO`, `DE SPE`,
   `DE CHARITATE`, `DE JUSTIFICATIONE`) — coût estimé plusieurs dizaines
   d'heures.
2. Alternative : transcription critique depuis la *Petri Canisii
   Catechismi Latini et Germanici* éd. F. Streicher (1933-1936) —
   édition scientifique de référence mais sous droits.
3. Stratégie hybride : ingérer les sections liminaires manuscrites
   (épître dédicatoire à Ferdinand Ier) là où l'OCR est propre, et
   documenter les sections manquantes.

Conformément à la règle « Ne pas forcer une ingestion bancale », on
laisse le stub et on note ce document comme candidat V2 prioritaire.
