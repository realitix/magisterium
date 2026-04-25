/**
 * Markdown + meta.yaml loader for a single document.
 *
 * Given a slug (and optional lang), résout le `.meta.yaml`, parse le bloc
 * `traductions` pour connaître les langues dispos + leur provenance, puis
 * lit le fichier `<dir>/<filename>.<lang>.md` correspondant.
 *
 * Ordre de préférence des langues :
 *   1. `preferLang` explicite (si un fichier existe pour cette langue) ;
 *   2. `langue_originale` du meta.yaml ;
 *   3. toute autre langue présente dans `traductions`.
 */
import fs from 'node:fs';
import path from 'node:path';
import yaml from 'js-yaml';
import { CORPUS_ROOT, LIVRES_ROOT } from './paths.js';
import { getDocumentBySlug } from './loadDocuments.js';
import type { DocumentContent, DocumentMeta, Langue, TraductionKind } from './types.js';

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
 * Construit l'ordre de préférence des langues pour un document donné,
 * en dédupliquant les entrées.
 */
function buildLangOrder(
  meta: DocumentMeta,
  fallbackOriginale: Langue,
  preferLang?: Langue,
): Langue[] {
  const order: Langue[] = [];
  const push = (l: Langue | undefined | null): void => {
    if (l !== undefined && l !== null && !order.includes(l)) order.push(l);
  };
  push(preferLang);
  push(meta.langue_originale ?? fallbackOriginale);
  if (meta.traductions !== undefined) {
    for (const l of Object.keys(meta.traductions)) push(l);
  }
  for (const l of meta.langues_disponibles ?? []) push(l);
  return order;
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

  // `doc.path` est relatif à la racine du document (magisterium ou livres).
  const root = doc.categorie === 'livre' ? LIVRES_ROOT : CORPUS_ROOT;
  const absMetaPath = path.join(root, doc.path);
  if (!fs.existsSync(absMetaPath)) return null;

  const meta = readMeta(absMetaPath);
  const order = buildLangOrder(meta, doc.langue_originale, preferLang);
  const lang = pickExistingLang(absMetaPath, order);
  if (lang === null) return null;

  const content = fs.readFileSync(mdPathFor(absMetaPath, lang), 'utf8');
  const kind: TraductionKind =
    meta.traductions?.[lang]?.kind ?? 'originale';
  return { content, lang, kind, meta };
}
