/**
 * Theme loader — reads `_metadata/concordance.jsonl` once and caches the
 * result in memory for the lifetime of the build process.
 */
import fs from 'node:fs';
import { CONCORDANCE_JSONL } from './paths.js';
import type { Theme } from './types.js';

/**
 * Human-readable French labels for the 11 doctrinal themes currently produced
 * by `build_concordance`. Keeping this explicit avoids brittle slug-to-label
 * heuristics for accents and punctuation.
 */
const THEME_LABELS: Readonly<Record<string, string>> = {
  'liberte-religieuse': 'Liberté religieuse',
  'oecumenisme-relations-autres-religions':
    'Œcuménisme et relations avec les autres religions',
  'liturgie-messe': 'Liturgie et messe',
  'ecclesiologie-salut-hors-eglise': "Ecclésiologie — hors de l'Église pas de salut",
  'rapport-eglise-etat': 'Rapport Église-État',
  'magistere-infaillibilite': 'Magistère et infaillibilité',
  'morale-sexuelle-mariage': 'Morale sexuelle et mariage',
  'doctrine-sociale': 'Doctrine sociale',
  modernisme: 'Modernisme',
  'collegialite-primaute': 'Collégialité et primauté',
  'sacerdoce-celibat': 'Sacerdoce et célibat',
};

/** Raw JSONL record — fields are optional because older files may omit them. */
interface ConcordanceRow {
  theme: string;
  pre_v2?: string[];
  v2?: string[];
  post_v2?: string[];
  fsspx?: string[];
}

let themesCache: Theme[] | null = null;
let themesBySlug: Map<string, Theme> | null = null;

/**
 * Fallback label: replace `-` by spaces and capitalize the first letter.
 * Used if a theme slug is not in `THEME_LABELS`.
 */
function deriveLabel(slug: string): string {
  const spaced = slug.replace(/-/g, ' ');
  return spaced.charAt(0).toUpperCase() + spaced.slice(1);
}

/**
 * Parse a single JSONL line into a `Theme`.
 * Missing bucket arrays are normalized to `[]`.
 */
function parseRow(line: string): Theme {
  const row = JSON.parse(line) as ConcordanceRow;
  const slug = row.theme;
  return {
    slug,
    label: THEME_LABELS[slug] ?? deriveLabel(slug),
    pre_v2: row.pre_v2 ?? [],
    v2: row.v2 ?? [],
    post_v2: row.post_v2 ?? [],
    fsspx: row.fsspx ?? [],
  };
}

/** Load all themes, caching the result in memory. */
export function loadAllThemes(): Theme[] {
  if (themesCache !== null) return themesCache;

  const raw = fs.readFileSync(CONCORDANCE_JSONL, 'utf8');
  const themes: Theme[] = raw
    .split('\n')
    .filter((line) => line.trim().length > 0)
    .map(parseRow);

  themesCache = themes;
  themesBySlug = new Map(themes.map((t) => [t.slug, t]));
  return themes;
}

/** Look up a single theme by its slug. */
export function getThemeBySlug(slug: string): Theme | null {
  if (themesBySlug === null) loadAllThemes();
  return themesBySlug?.get(slug) ?? null;
}
