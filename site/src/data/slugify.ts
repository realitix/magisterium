/**
 * URL-safe slug utility: lowercase, accents stripped, non-alphanumerics
 * collapsed to single `-`, leading/trailing `-` removed.
 */
export function slugify(str: string): string {
  return str
    .normalize('NFD')
    // Strip combining diacritical marks (U+0300..U+036F).
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase()
    // Replace any run of non-alphanumeric characters by a single `-`.
    .replace(/[^a-z0-9]+/g, '-')
    // Trim leading/trailing `-`.
    .replace(/^-+|-+$/g, '');
}
