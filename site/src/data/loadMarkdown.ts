/**
 * Markdown + meta.yaml loader for a single document.
 *
 * Given a slug, resolves the `.meta.yaml` path via the index, parses it,
 * then reads the corresponding `<dir>/<filename>.<lang>.md` file. The
 * language preference order is:
 *   1. explicit `preferLang` argument (if a file exists for it);
 *   2. `langue_originale` from the meta.yaml;
 *   3. any language listed in `langues_disponibles` for which a file exists;
 *   4. fallback probe of known langs: la, fr, it, en.
 */
import fs from 'node:fs';
import path from 'node:path';
import yaml from 'js-yaml';
import { CORPUS_ROOT } from './paths.js';
import { getDocumentBySlug } from './loadDocuments.js';
import type { DocumentContent, DocumentMeta, Langue } from './types.js';

const KNOWN_LANGS: readonly Langue[] = ['la', 'fr', 'it', 'en'];

/** Cache of parsed meta.yaml files, keyed by absolute path. */
const metaCache = new Map<string, DocumentMeta>();

function readMeta(absMetaPath: string): DocumentMeta {
  const cached = metaCache.get(absMetaPath);
  if (cached !== undefined) return cached;
  const raw = fs.readFileSync(absMetaPath, 'utf8');
  const parsed = (yaml.load(raw) ?? {}) as DocumentMeta;
  metaCache.set(absMetaPath, parsed);
  return parsed;
}

/** Derive the `.md` path for a given language from a `.meta.yaml` path. */
function mdPathFor(absMetaPath: string, lang: Langue): string {
  // Strip the trailing `.meta.yaml` (10 chars) to get the base path.
  const base = absMetaPath.replace(/\.meta\.yaml$/, '');
  return `${base}.${lang}.md`;
}

/**
 * Pick the first language from `candidates` for which the `.md` file
 * exists on disk. Returns `null` if none is found.
 */
function pickExistingLang(
  absMetaPath: string,
  candidates: readonly Langue[],
): Langue | null {
  for (const lang of candidates) {
    if (fs.existsSync(mdPathFor(absMetaPath, lang))) return lang;
  }
  return null;
}

/**
 * Load the markdown content + parsed meta for a single document slug.
 * Returns `null` if the slug is unknown or no markdown file exists.
 */
export function loadDocumentContent(
  slug: string,
  preferLang?: Langue,
): DocumentContent | null {
  const doc = getDocumentBySlug(slug);
  if (doc === null) return null;

  const absMetaPath = path.join(CORPUS_ROOT, doc.path);
  if (!fs.existsSync(absMetaPath)) return null;

  const meta = readMeta(absMetaPath);

  // Build the ordered list of candidate languages, deduplicated.
  const order: Langue[] = [];
  const push = (l: Langue | undefined | null): void => {
    if (l !== undefined && l !== null && !order.includes(l)) order.push(l);
  };
  push(preferLang);
  push(meta.langue_originale ?? doc.langue_originale);
  for (const l of meta.langues_disponibles ?? []) push(l);
  for (const l of KNOWN_LANGS) push(l);

  const lang = pickExistingLang(absMetaPath, order);
  if (lang === null) return null;

  const content = fs.readFileSync(mdPathFor(absMetaPath, lang), 'utf8');
  return { content, lang, meta };
}
