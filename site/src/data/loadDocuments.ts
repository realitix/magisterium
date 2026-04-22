/**
 * Document loader — reads `_metadata/index.jsonl` once, joins with the
 * concordance to populate `themes_doctrinaux`, and caches the result.
 */
import fs from 'node:fs';
import { INDEX_JSONL } from './paths.js';
import { loadAllThemes } from './loadThemes.js';
import type { Document, TraductionSummary } from './types.js';

/** Raw JSONL record shape — `sha256` is a string in `index.jsonl`. */
interface IndexRow {
  path: string;
  slug: string;
  incipit: string | null;
  titre_fr: string | null;
  auteur: string | null;
  periode: string;
  type: string;
  date: string | null;
  langue_originale: string;
  sha256: string;
  sujets?: string[];
  themes_doctrinaux?: string[];
  traductions?: TraductionSummary[];
}

let documentsCache: Document[] | null = null;
let documentsBySlug: Map<string, Document> | null = null;

/**
 * Build a slug -> themes map from the concordance, merging every bucket
 * (`pre_v2`, `v2`, `post_v2`, `fsspx`) into a single set of theme slugs.
 */
function buildSlugToThemes(): Map<string, Set<string>> {
  const map = new Map<string, Set<string>>();
  const themes = loadAllThemes();
  for (const theme of themes) {
    const buckets = [theme.pre_v2, theme.v2, theme.post_v2, theme.fsspx];
    for (const bucket of buckets) {
      for (const docSlug of bucket) {
        let set = map.get(docSlug);
        if (set === undefined) {
          set = new Set<string>();
          map.set(docSlug, set);
        }
        set.add(theme.slug);
      }
    }
  }
  return map;
}

/**
 * Parse one JSONL row and enrich `themes_doctrinaux` from the concordance
 * join. The row's own `themes_doctrinaux` (usually empty) is merged too so
 * we don't lose data.
 */
function parseRow(line: string, slugToThemes: Map<string, Set<string>>): Document {
  const row = JSON.parse(line) as IndexRow;
  const joined = slugToThemes.get(row.slug) ?? new Set<string>();
  for (const t of row.themes_doctrinaux ?? []) joined.add(t);
  const traductions: TraductionSummary[] = Array.isArray(row.traductions)
    ? row.traductions
    : [{ lang: row.langue_originale, kind: 'originale' }];
  return {
    path: row.path,
    slug: row.slug,
    incipit: row.incipit,
    titre_fr: row.titre_fr,
    auteur: row.auteur,
    periode: row.periode,
    type: row.type,
    date: row.date,
    langue_originale: row.langue_originale,
    sha256: row.sha256,
    sujets: row.sujets ?? [],
    themes_doctrinaux: Array.from(joined).sort(),
    traductions,
  };
}

/** Load all documents, caching the result in memory. */
export function loadAllDocuments(): Document[] {
  if (documentsCache !== null) return documentsCache;

  const slugToThemes = buildSlugToThemes();
  const raw = fs.readFileSync(INDEX_JSONL, 'utf8');
  const docs: Document[] = raw
    .split('\n')
    .filter((line) => line.trim().length > 0)
    .map((line) => parseRow(line, slugToThemes));

  documentsCache = docs;
  documentsBySlug = new Map(docs.map((d) => [d.slug, d]));
  return docs;
}

/** Look up a single document by its slug. */
export function getDocumentBySlug(slug: string): Document | null {
  if (documentsBySlug === null) loadAllDocuments();
  return documentsBySlug?.get(slug) ?? null;
}
