/**
 * Central resolver for paths into the corpus.
 *
 * The site lives at `<repo>/site/`. The corpus is split in two roots :
 *  - `<repo>/magisterium/` — magisterial texts (authority).
 *  - `<repo>/livres/`      — non-magisterial references (books, studies,
 *                            cardinal interventions, theology manuals).
 *
 * Both roots have their own `_metadata/index.jsonl`. The site loads both and
 * merges them, distinguishing entries via the `categorie` field.
 *
 * We resolve relative to `process.cwd()` at module load time.
 */
import path from 'node:path';

/** Absolute path to the magisterium corpus root. */
export const CORPUS_ROOT: string = path.resolve(process.cwd(), '../magisterium');

/** Absolute path to the books corpus root. */
export const LIVRES_ROOT: string = path.resolve(process.cwd(), '../livres');

/** Absolute path to the magisterium `_metadata` directory. */
export const METADATA_ROOT: string = path.join(CORPUS_ROOT, '_metadata');

/** Absolute path to the magisterium documents index JSONL. */
export const INDEX_JSONL: string = path.join(METADATA_ROOT, 'index.jsonl');

/** Absolute path to the books documents index JSONL. */
export const LIVRES_INDEX_JSONL: string = path.join(LIVRES_ROOT, '_metadata', 'index.jsonl');

/** Absolute path to the concordance JSONL (magisterium only — books are not bucketed by theme). */
export const CONCORDANCE_JSONL: string = path.join(METADATA_ROOT, 'concordance.jsonl');
