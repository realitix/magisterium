# Catéchisme du concile de Trente (1566) — MANQUANT

**Autres noms :** Catechismus Romanus, Catechismus ad Parochos,
Catéchisme de Pie V.

## Pourquoi absent de la V1 du corpus

Aucune source en ligne stable ne sert le texte latin intégral avec une
arborescence d'URLs prévisible. Tentatives effectuées :

- `documentacatholicaomnia.eu` : pas d'entrée `Catechismus_Romanus` sous
  les patterns 30_10_, 04z_, 20vs, ni sous l'index alphabétique (tous
  404 lors de la Phase 7).
- `la.wikisource.org/wiki/Catechismus_Romanus` : page inexistante au
  20-04-2026.
- `archive.org` : plusieurs éditions historiques (1796, 1804, 1830, 1866)
  mais en tant que PDFs océrisés ou DJVU non segmentés — pas adapté au
  pipeline HTML→markdown.
- `intratext.com` : la recherche ne retourne pas de texte segmenté.

## Pistes pour une V2

1. Scraper une édition `archive.org` en PDF (p. ex.
   <https://archive.org/details/bub_gb_Oow_AAAAcAAJ>) puis OCR + split en
   partie I/II/III/IV.
2. Récupérer la version FR de `salve-regina.com` (SSL foiré, déjà dans
   `INSECURE_DOMAINS`) — mais c'est une traduction française, donc
   sort de la règle « langue source ».
3. Prendre la version anglophone de `catholicprimer.org` ou l'édition
   McHugh/Callan 1923 sur `archive.org` (traduction EN).
