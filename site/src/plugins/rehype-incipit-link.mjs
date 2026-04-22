/**
 * Rehype plugin : auto-linker des mentions d'incipits dans les fiches Q/R.
 *
 * Deux (trois) transformations sur l'AST HTML produit par remark/astro :
 *   1. `<em>texte</em>` → `<a href="/documents/<slug>/"><em>texte</em></a>`
 *      quand `texte` (normalisé) matche un incipit ou un titre_fr unique
 *      dans l'index du corpus.
 *   2. `<code>/documents/<slug>/</code>` → `<a href="...">…</a>` pour la
 *      section « Références » en backticks.
 *   3. `<code>slug.lang.md</code>` → `<a href="/documents/<slug>/<lang>/">`
 *      pour les citations de source de type « (source : file.la.md, l.12) ».
 *
 * Non-ambigu : si deux documents partagent le même incipit normalisé,
 * l'entrée vaut `null` et on n'auto-linke pas (éviterait de pointer
 * arbitrairement vers l'un d'eux).
 */
import { visitParents } from 'unist-util-visit-parents';

/** Normalise un texte pour comparaison : casse basse, sans diacritiques,
 * ponctuation écrasée en simples espaces. */
export function normalizeIncipit(s) {
  return s
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    .replace(/[^\p{L}\p{N}]+/gu, ' ')
    .trim();
}

/** Construit (bySignature, slugs) depuis le JSONL du corpus. */
export function buildIncipitIndex(indexJsonl) {
  const bySignature = new Map();
  const slugs = new Set();
  for (const line of indexJsonl.split('\n')) {
    const trimmed = line.trim();
    if (trimmed.length === 0) continue;
    let entry;
    try {
      entry = JSON.parse(trimmed);
    } catch {
      continue;
    }
    if (!entry.slug) continue;
    slugs.add(entry.slug);
    const candidates = [entry.incipit, entry.titre_fr];
    for (const raw of candidates) {
      if (raw === null || raw === undefined) continue;
      const key = normalizeIncipit(String(raw));
      if (key.length < 4) continue;
      if (bySignature.has(key)) {
        if (bySignature.get(key) !== entry.slug) {
          bySignature.set(key, null); // collision → ambigu
        }
      } else {
        bySignature.set(key, entry.slug);
      }
    }
  }
  return { bySignature, slugs };
}

/** Factory du plugin rehype. Params : { bySignature, slugs }. */
export function rehypeIncipitLink(options = {}) {
  const { bySignature, slugs } = options;
  if (!bySignature || !slugs) return () => {};

  const DOC_PATH_RE = /^\/?documents\/([a-z0-9_-]+)\/?$/i;
  const LANG_MD_RE = /^([a-z0-9_-]+)\.([a-z]{2}(?:_[a-z]{2})?)\.md$/i;

  /** Texte concaténé d'un nœud, sans descendre dans les `<a>` déjà présents. */
  function extractText(node) {
    if (!node.children) return '';
    let out = '';
    for (const c of node.children) {
      if (c.type === 'text') out += c.value;
      else if (c.type === 'element' && c.tagName !== 'a') out += extractText(c);
    }
    return out;
  }

  function hasAnchorAncestor(ancestors) {
    for (const a of ancestors) {
      if (a.type === 'element' && a.tagName === 'a') return true;
    }
    return false;
  }

  function wrapInAnchor(href, node) {
    return {
      type: 'element',
      tagName: 'a',
      properties: { href, className: ['incipit-link'] },
      children: [node],
    };
  }

  return (tree) => {
    visitParents(tree, 'element', (node, ancestors) => {
      if (hasAnchorAncestor(ancestors)) return;
      const parent = ancestors[ancestors.length - 1];
      if (!parent || !Array.isArray(parent.children)) return;
      const idx = parent.children.indexOf(node);
      if (idx === -1) return;

      if (node.tagName === 'em') {
        const text = extractText(node).trim();
        if (text.length >= 4 && text.length <= 120) {
          const key = normalizeIncipit(text);
          const slug = bySignature.get(key);
          if (slug) {
            parent.children[idx] = wrapInAnchor(`/documents/${slug}/`, node);
            return;
          }
        }
      }

      if (node.tagName === 'code') {
        const text = extractText(node).trim();
        const pathMatch = DOC_PATH_RE.exec(text);
        if (pathMatch && slugs.has(pathMatch[1])) {
          parent.children[idx] = wrapInAnchor(`/documents/${pathMatch[1]}/`, node);
          return;
        }
        const mdMatch = LANG_MD_RE.exec(text);
        if (mdMatch && slugs.has(mdMatch[1])) {
          parent.children[idx] = wrapInAnchor(
            `/documents/${mdMatch[1]}/${mdMatch[2]}/`,
            node,
          );
          return;
        }
      }
    });
  };
}
