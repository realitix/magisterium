/**
 * Types for the MagisterIA corpus data layer.
 */

export type Periode = 'pre-vatican-ii' | 'vatican-ii' | 'post-vatican-ii' | 'fsspx' | string;

export type Langue = 'la' | 'fr' | 'it' | 'en' | string;

/**
 * Provenance d'une traduction :
 *  - `originale`  : langue source du document, telle qu'écrite par l'auteur.
 *  - `officielle` : traduction publiée par la source faisant autorité
 *                   (Saint-Siège, dicastère, éditeur officiel) et scrapée.
 *  - `ia`         : traduction générée automatiquement par un modèle IA
 *                   à partir de l'originale (ou à défaut d'une officielle).
 */
export type TraductionKind = 'originale' | 'officielle' | 'ia';

/** Vue compacte d'une traduction, telle qu'exposée dans `index.jsonl`. */
export interface TraductionSummary {
  lang: Langue;
  kind: TraductionKind;
}

/**
 * Entrée complète du bloc `traductions` dans un `.meta.yaml`, avec tous les
 * champs de provenance. Champs optionnels selon le `kind`.
 */
export interface TraductionEntry {
  kind: TraductionKind;
  sha256: string;
  source_url?: string;
  fetched_at?: string;
  fetch_method?: string;
  model?: string;
  translated_from?: Langue;
  source_sha256?: string;
  translated_at?: string;
}

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
   * SHA-256 checksum de la langue originale. On normalise à une `string` au
   * chargement pour simplifier les usages en aval.
   */
  sha256: string;
  sujets: string[];
  /**
   * Thematic tags — enriched on load with the concordance join. Slugs of themes
   * for which this document appears in `pre_v2`, `v2`, `post_v2` or `fsspx`.
   */
  themes_doctrinaux: string[];
  /**
   * Liste compacte des langues disponibles avec leur provenance.
   * Triée par code langue. Toujours non-vide (au minimum la langue originale).
   */
  traductions: TraductionSummary[];
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
  /** Bloc source de vérité — provenance par langue. */
  traductions?: Record<Langue, TraductionEntry>;
  /** Champs historiques, conservés pour compat ascendante. */
  langues_disponibles?: Langue[];
  sha256?: string | Record<string, string>;
  [key: string]: unknown;
}

/** Return shape of `loadDocumentContent`. */
export interface DocumentContent {
  content: string;
  lang: Langue;
  /** Provenance de la langue réellement servie. */
  kind: TraductionKind;
  meta: DocumentMeta;
}
