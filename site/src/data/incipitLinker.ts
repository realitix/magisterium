/**
 * Auto-linker d'incipits dans les fiches Q/R.
 *
 * Les fiches Q/R mentionnent constamment des textes magistériels par leur
 * incipit en italique (`*Lumen Gentium*`, `*Dei Filius*`, `*Cantate Domino*`…)
 * ou par leur slug dans une section « Références » (`` `/documents/slug/` ``).
 * Ce module post-traite le HTML rendu pour transformer ces mentions en liens
 * cliquables vers la page du document correspondant — à condition que le
 * slug existe sans ambiguïté dans l'index.
 *
 * Non-ambigu : si deux documents partagent le même incipit normalisé, on
 * n'auto-linke pas (éviterait de pointer arbitrairement vers l'un d'eux).
 */
import { loadAllDocuments } from './loadDocuments.js';

/** Normalise un texte pour comparaison : casse basse, sans diacritiques,
 * ponctuation réduite aux espaces simples. */
function normalize(s: string): string {
  return s
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '') // strip combining marks
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .trim();
}

interface IncipitIndex {
  /** incipit normalisé → slug (null si ambigu). */
  bySignature: Map<string, string | null>;
  /** slug → slug (pour valider qu'un slug brut existe). */
  slugs: Set<string>;
}

let cached: IncipitIndex | null = null;

function buildIndex(): IncipitIndex {
  if (cached !== null) return cached;
  const bySignature = new Map<string, string | null>();
  const slugs = new Set<string>();
  for (const doc of loadAllDocuments()) {
    slugs.add(doc.slug);
    const candidates: (string | null)[] = [
      doc.incipit,
      doc.titre_fr,
    ];
    for (const raw of candidates) {
      if (raw === null || raw === undefined) continue;
      const key = normalize(raw);
      if (key.length < 4) continue; // trop court, trop risqué
      if (bySignature.has(key)) {
        const prev = bySignature.get(key);
        if (prev !== doc.slug) {
          // Collision avec un autre doc → ambigu, on désactive cette entrée.
          bySignature.set(key, null);
        }
      } else {
        bySignature.set(key, doc.slug);
      }
    }
  }
  cached = { bySignature, slugs };
  return cached;
}

/** True si la position `pos` dans `html` est à l'intérieur d'un `<a>`. */
function isInsideAnchor(html: string, pos: number): boolean {
  const before = html.slice(0, pos);
  const lastOpen = before.lastIndexOf('<a ');
  if (lastOpen === -1) return false;
  const lastClose = before.lastIndexOf('</a>');
  return lastOpen > lastClose;
}

/**
 * Lie les mentions `<em>incipit</em>` (ou `<code>/documents/slug/</code>`)
 * vers leur page canonique quand elles matchent un document unique du corpus.
 *
 * Ne retouche pas les `<em>` déjà à l'intérieur d'un `<a>`.
 */
export function linkIncipitMentions(html: string): string {
  const { bySignature, slugs } = buildIndex();

  // 1. <em>…</em> → <a><em>…</em></a> si incipit connu et non-ambigu.
  let out = html.replace(
    /<em>([^<]{3,80})<\/em>/gi,
    (match, inner, offset) => {
      if (isInsideAnchor(out, offset as number)) return match;
      const key = normalize(inner);
      if (key.length < 4) return match;
      const slug = bySignature.get(key);
      if (slug === undefined || slug === null) return match;
      return `<a class="incipit-link" href="/documents/${slug}/"><em>${inner}</em></a>`;
    }
  );

  // 2. <code>/documents/SLUG/</code> (ou `.la.md`, `.fr.md`…) → lien.
  //    Couvre les « Références » qui utilisent des backticks.
  out = out.replace(
    /<code>\/documents\/([a-z0-9_-]+)\/?<\/code>/gi,
    (match, slug) => {
      if (!slugs.has(slug)) return match;
      return `<a class="incipit-link" href="/documents/${slug}/">${slug}</a>`;
    }
  );

  // 3. <code>slug.la.md</code> / <code>slug.fr.md</code> — patterns utilisés
  //    dans les citations de source. On pointe vers la page dans cette langue.
  out = out.replace(
    /<code>([a-z0-9_-]+)\.([a-z]{2}(?:_[a-z]{2})?)\.md<\/code>/gi,
    (match, slug, lang) => {
      if (!slugs.has(slug)) return match;
      return `<a class="incipit-link" href="/documents/${slug}/${lang}/">${slug}.${lang}.md</a>`;
    }
  );

  return out;
}
