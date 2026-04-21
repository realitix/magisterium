/**
 * Types for the Magisterium corpus data layer.
 */

export type Periode = 'pre-vatican-ii' | 'vatican-ii' | 'post-vatican-ii' | 'fsspx' | string;

export type Langue = 'la' | 'fr' | 'it' | 'en' | string;

/**
 * One entry of `_metadata/index.jsonl`, enriched with `themes_doctrinaux`
 * derived from the concordance (join on slug).
 */
export interface Document {
  /** Relative path of the `.meta.yaml` file inside the magisterium corpus. */
  path: string;
  slug: string;
  incipit: string | null;
  titre_fr: string | null;
  auteur: string | null;
  periode: Periode;
  type: string;
  /** ISO-like date string, e.g. "1964-11-21". */
  date: string | null;
  langue_originale: Langue;
  /**
   * SHA-256 checksum. In `index.jsonl` this is a single string, but in some
   * `meta.yaml` files it is keyed by language. We normalize to `string` on read.
   */
  sha256: string;
  sujets: string[];
  /**
   * Thematic tags — enriched on load with the concordance join. Slugs of themes
   * for which this document appears in `pre_v2`, `v2`, `post_v2` or `fsspx`.
   */
  themes_doctrinaux: string[];
}

/**
 * Concordance entry — one theme with its documents bucketed by period.
 */
export interface Theme {
  slug: string;
  /** Human-readable French label. */
  label: string;
  pre_v2: string[];
  v2: string[];
  post_v2: string[];
  fsspx: string[];
}

/**
 * Full metadata loaded from `<path>.meta.yaml`.
 * Kept loose because the YAML shape varies slightly across documents.
 */
export interface DocumentMeta {
  incipit?: string | null;
  titre_fr?: string | null;
  titre_original?: string | null;
  auteur?: string | null;
  periode?: Periode;
  type?: string;
  date?: string | null;
  autorite_magisterielle?: string | null;
  langues_disponibles?: Langue[];
  langue_originale?: Langue;
  denzinger?: unknown[];
  sujets?: string[];
  themes_doctrinaux?: string[];
  references_anterieures?: unknown[];
  references_posterieures?: unknown[];
  sources?: Array<{
    url?: string;
    site?: string;
    langue?: Langue;
    fetch_method?: string;
  }>;
  sha256?: string | Record<string, string>;
  [key: string]: unknown;
}

/** Return shape of `loadDocumentContent`. */
export interface DocumentContent {
  content: string;
  lang: Langue;
  meta: DocumentMeta;
}
