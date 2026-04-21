/**
 * Central resolver for paths into the magisterium corpus.
 *
 * The site lives at `<repo>/site/`, the corpus at `<repo>/magisterium/`.
 * We resolve relative to `process.cwd()` at module load time.
 */
import path from 'node:path';

/** Absolute path to the magisterium corpus root. */
export const CORPUS_ROOT: string = path.resolve(process.cwd(), '../magisterium');

/** Absolute path to the `_metadata` directory. */
export const METADATA_ROOT: string = path.join(CORPUS_ROOT, '_metadata');

/** Absolute path to the documents index JSONL. */
export const INDEX_JSONL: string = path.join(METADATA_ROOT, 'index.jsonl');

/** Absolute path to the concordance JSONL. */
export const CONCORDANCE_JSONL: string = path.join(METADATA_ROOT, 'concordance.jsonl');
