/**
 * URL → slug mapping for all documents in the corpus.
 *
 * Built at site-build time by reading every `*.meta.yaml` file's
 * `sources[].url` and mapping each URL to the corresponding document slug.
 * Used by the HTML post-processor to rewrite vatican.va links in document
 * bodies so that when a cited document is present in our corpus, the link
 * points internally instead of leaving the site.
 */
import fs from 'node:fs';
import path from 'node:path';
import yaml from 'js-yaml';
import { CORPUS_ROOT } from './paths.js';

let urlMapCache: Map<string, string> | null = null;

interface MetaSourceShape {
  sources?: Array<{ url?: string; site?: string }>;
}

/** Normalise a URL for matching : lower-case host, strip trailing slash and fragment. */
function normalizeUrl(raw: string): string {
  try {
    const u = new URL(raw);
    // Drop fragment and query (the source of truth is the path).
    u.hash = '';
    u.search = '';
    let out = `${u.protocol}//${u.host.toLowerCase()}${u.pathname}`;
    if (out.endsWith('/')) out = out.slice(0, -1);
    return out;
  } catch {
    return raw.trim();
  }
}

function walkMetaFiles(dir: string): string[] {
  const acc: string[] = [];
  const entries = fs.readdirSync(dir, { withFileTypes: true });
  for (const e of entries) {
    const full = path.join(dir, e.name);
    if (e.isDirectory()) {
      if (e.name === '_metadata' || e.name.startsWith('.')) continue;
      acc.push(...walkMetaFiles(full));
    } else if (e.isFile() && e.name.endsWith('.meta.yaml')) {
      acc.push(full);
    }
  }
  return acc;
}

/** Derive the document slug from the meta.yaml filename. */
function slugFromMetaPath(metaPath: string): string {
  // Ex : .../foo/2000-08-06_dominus-iesus_document.meta.yaml → 2000-08-06_dominus-iesus_document
  return path.basename(metaPath, '.meta.yaml');
}

/**
 * Build the URL → slug map. Cached on first call.
 * Multiple URLs may map to the same slug (one per language). Conversely,
 * one URL may only map to one slug (first wins).
 */
export function loadUrlMap(): Map<string, string> {
  if (urlMapCache !== null) return urlMapCache;
  const map = new Map<string, string>();
  const files = walkMetaFiles(CORPUS_ROOT);
  for (const file of files) {
    try {
      const raw = fs.readFileSync(file, 'utf-8');
      const parsed = yaml.load(raw) as MetaSourceShape | null;
      if (parsed === null || typeof parsed !== 'object') continue;
      const slug = slugFromMetaPath(file);
      for (const src of parsed.sources ?? []) {
        if (typeof src?.url !== 'string' || src.url.length === 0) continue;
        const key = normalizeUrl(src.url);
        if (!map.has(key)) {
          map.set(key, slug);
        }
      }
    } catch {
      // Skip broken meta files, they're caught elsewhere.
    }
  }
  urlMapCache = map;
  return map;
}

/**
 * Look up the internal slug for an external URL.
 * Returns null if the URL is not known to the corpus.
 */
export function slugForUrl(url: string): string | null {
  const map = loadUrlMap();
  const normalised = normalizeUrl(url);
  return map.get(normalised) ?? null;
}
