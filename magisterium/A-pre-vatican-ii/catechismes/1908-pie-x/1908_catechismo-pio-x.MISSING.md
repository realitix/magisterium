# Catéchisme de Pie X (1908) — MANQUANT

**Nom officiel :** *Catechismo della Dottrina Cristiana*, Pie X, 1908
(remanié 1912, "Catechismo Maggiore di San Pio X"). Langue originale :
italien.

## Absent de la V1 car

- `vatican.va` ne sert PAS le Catechismo di Pio X en HTML (la Santa Sede
  publie le CCC de 1992 et son Compendium de 2005, pas le texte de 1908).
- `maranatha.it/Catechismo/PioX/*` : 404 sur tous les patterns testés.
- `cristianicattolici.net`, `credereoggi.it`, `catechesistradizionale.com` :
  respectivement 404, cert SSL invalide, ECONNREFUSED.
- `salve-regina.com` contient une version FR mais (a) traduction, (b) SSL
  obsolète, (c) sans arborescence MediaWiki fiable côté `title=…`.

## V2

- Trouver un mirror italien stable (forums tradi, Una Voce Italia).
- Scraper la traduction FR sur `salve-regina.com` si on accepte une
  exception à la règle « langue source ».
- Ingérer un PDF `archive.org` + OCR.
