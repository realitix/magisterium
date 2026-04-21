/**
 * HTML post-processor for scraped document bodies.
 *
 * The markdown we store in `magisterium/` was converted from the original
 * vatican.va (and other sites') HTML by pandoc. It retains a lot of cruft:
 *
 *  - Social-share buttons with empty URLs
 *  - `javascript:history.go(-1)` navigation links
 *  - `mailto:` with no address
 *  - Empty fragment-only anchors (`#`)
 *  - Images from the vatican.va chrome (share icons, spacer GIFs, etc.)
 *
 * We also want to rewrite cross-references: when the body links to a
 * vatican.va URL that we have in our corpus, the link should point to the
 * internal `/documents/<slug>/` page instead of leaving the site.
 *
 * This module takes the HTML produced by `marked` and returns a cleaned
 * version. It uses regex (not a full HTML parser) because the input is
 * small, well-structured and we only care about anchor tags.
 */
import { slugForUrl } from './loadUrlMap.js';

/** Anchors whose href should cause the `<a>` to be stripped (contents kept). */
const DEAD_HREF_PATTERNS: RegExp[] = [
  /^javascript:/i,
  /^mailto:\s*$/i,
  /^#\s*$/, // empty fragment
  /facebook\.com\/sharer/i,
  /twitter\.com\/(home|share|intent)/i,
  /plus\.google\.com\/share/i,
  /linkedin\.com\/.*sharing/i,
  /api\.whatsapp\.com/i,
  /t\.me\/share/i,
];

/** Images whose src matches one of these patterns are removed entirely. */
const DEAD_IMG_PATTERNS: RegExp[] = [
  // vatican.va chrome (layout/navigation only)
  /\/img\/vuoto\.gif/i,
  /\/img\/back\.(jpg|png)/i,
  /\/img\/up\.(jpg|png)/i,
  /\/img\/top\.(jpg|png)/i,
  /\/img\/print\.(jpg|png)/i,
  /\/img\/pkeys\.jpg/i,
  /\/img\/logo-vatican\.png/i,
  /\/img\/riga_int\.jpg/i,
  /\/img\/psearch_fill\.jpg/i,
  /\/share\/(facebook|twitter|gplus|mail)\.png/i,
];

interface CleanResult {
  html: string;
  stats: {
    strippedDead: number;
    rewrittenInternal: number;
    externalKept: number;
    strippedImages: number;
    anchorsRestored: number;
    fragmentsUnwrapped: number;
  };
}

/** Escape characters that have special meaning in a regular expression. */
function escapeRegExp(s: string): string {
  return s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&');
}

/**
 * Re-inject lost anchor IDs on target elements.
 *
 * Many scraped documents contain a table of contents where each entry links
 * to `#AnchorName`. The original HTML had `<a name="AnchorName"></a>` tags
 * marking the target, but pandoc drops empty anchors during the conversion
 * to markdown. Consequently the rendered page has 500+ broken fragment
 * links.
 *
 * Heuristic: for each `<a href="#X">text</a>` we find, locate the first
 * `<strong>text</strong>` (or `<b>text</b>`) in the document that doesn't
 * already carry an id and that sits outside an anchor, and inject `id="X"`.
 * This mirrors the table-of-contents structure used by vatican.va (section
 * headings are rendered as `<b>…</b>` in their source, not `<h2>`).
 */
function restoreAnchors(html: string, stats: CleanResult['stats']): string {
  // Collect fragment references: (fragment, text).
  // We dedupe on fragment — the first occurrence wins.
  const refs = new Map<string, string>();
  const refPattern = /<a\s+href="#([^"]+)"[^>]*>([^<]{1,200})<\/a>/gi;
  let m: RegExpExecArray | null;
  while ((m = refPattern.exec(html)) !== null) {
    const frag = m[1];
    const text = m[2].trim();
    if (text.length === 0) continue;
    if (!refs.has(frag)) refs.set(frag, text);
  }

  let out = html;
  for (const [frag, text] of refs) {
    // Build a regex that finds the first <strong> or <b> containing exactly
    // the text and not yet carrying an id. Allow (but don't require) minor
    // whitespace variations inside.
    const safe = escapeRegExp(text);
    const re = new RegExp(
      `(<(strong|b)(?![^>]*\\bid=)[^>]*)>(\\s*${safe}\\s*)(</\\2>)`,
      'i'
    );
    const before = out;
    out = out.replace(re, (_match, openHead, tag, inner, close) => {
      return `${openHead} id="${frag}">${inner}${close}`;
    });
    if (out !== before) stats.anchorsRestored += 1;
  }
  return out;
}

/**
 * Strip `<a href="#X">` whose target was not restored — keeps the inner
 * text so the table of contents stays readable, just not clickable.
 */
function unwrapBrokenFragments(html: string, stats: CleanResult['stats']): string {
  // Build the set of ids present in the cleaned HTML.
  const presentIds = new Set<string>();
  const idRe = /\bid="([^"]+)"/gi;
  let m: RegExpExecArray | null;
  while ((m = idRe.exec(html)) !== null) presentIds.add(m[1]);

  return html.replace(
    /<a\s+href="#([^"]+)"[^>]*>([\s\S]*?)<\/a>/gi,
    (match, frag, inner) => {
      if (presentIds.has(frag)) return match; // anchor exists, keep link
      stats.fragmentsUnwrapped += 1;
      return inner; // drop the <a> wrapper, keep content
    }
  );
}

/**
 * Extract attribute value from an opening tag string.
 * Returns null if not present.
 */
function getAttr(tag: string, name: string): string | null {
  // Matches href="…" or href='…' or href=…
  const re = new RegExp(`\\s${name}\\s*=\\s*(?:"([^"]*)"|'([^']*)'|([^\\s>]+))`, 'i');
  const m = tag.match(re);
  if (m === null) return null;
  return m[1] ?? m[2] ?? m[3] ?? null;
}

/** True if the URL is an absolute external HTTP(S) URL. */
function isAbsolute(url: string): boolean {
  return /^https?:\/\//i.test(url);
}

/** True if the URL targets vatican.va (any subdomain, any scheme). */
function isVaticanUrl(url: string): boolean {
  return /^https?:\/\/(www\.)?vatican\.va/i.test(url);
}

/** Rewrite an `<a …>opening tag</a>` block. */
function transformAnchor(
  fullMatch: string,
  openingTag: string,
  innerHtml: string,
  stats: CleanResult['stats']
): string {
  const href = getAttr(openingTag, 'href');

  if (href === null) {
    // No href attribute: keep as-is (rare edge case).
    return fullMatch;
  }

  // Dead links : strip the <a> tag but keep inner text.
  if (DEAD_HREF_PATTERNS.some((re) => re.test(href))) {
    stats.strippedDead += 1;
    return innerHtml;
  }

  // Internal rewrite : vatican.va URL that matches a document in the corpus.
  if (isVaticanUrl(href)) {
    const slug = slugForUrl(href);
    if (slug !== null) {
      const newHref = `/documents/${slug}/`;
      stats.rewrittenInternal += 1;
      // Replace the href attribute ; preserve other attributes.
      const newOpening = openingTag.replace(
        /\s(href)\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]+)/i,
        ` href="${newHref}"`
      );
      return `${newOpening}${innerHtml}</a>`;
    }
  }

  // External link : keep but add target="_blank" + rel + a marker class.
  if (isAbsolute(href)) {
    stats.externalKept += 1;
    // Avoid adding target/rel twice if they already exist.
    let newOpening = openingTag;
    if (!/\starget\s*=/i.test(newOpening)) {
      newOpening = newOpening.replace(/^<a\b/i, '<a target="_blank"');
    }
    if (!/\srel\s*=/i.test(newOpening)) {
      newOpening = newOpening.replace(/^<a\b/i, '<a rel="noopener nofollow"');
    }
    if (!/\bclass\s*=/i.test(newOpening)) {
      newOpening = newOpening.replace(/^<a\b/i, '<a class="ext-link"');
    } else {
      newOpening = newOpening.replace(/class\s*=\s*"([^"]*)"/i, (_, cls) => `class="${cls} ext-link"`);
    }
    return `${newOpening}${innerHtml}</a>`;
  }

  // Relative or fragment link : keep as-is.
  return fullMatch;
}

/**
 * Clean the HTML body of a scraped document :
 *   - strip dead anchor tags (share, javascript, mailto, empty fragment)
 *   - rewrite vatican.va → internal /documents/<slug>/ when possible
 *   - mark other absolute links as external
 *   - remove vatican.va chrome images
 */
export function cleanDocumentHtml(html: string): CleanResult {
  const stats: CleanResult['stats'] = {
    strippedDead: 0,
    rewrittenInternal: 0,
    externalKept: 0,
    strippedImages: 0,
    anchorsRestored: 0,
    fragmentsUnwrapped: 0,
  };

  // Pass 1 : strip junk images.
  let out = html.replace(/<img\b[^>]*>/gi, (imgTag) => {
    const src = getAttr(imgTag, 'src');
    if (src === null) return imgTag;
    if (DEAD_IMG_PATTERNS.some((re) => re.test(src))) {
      stats.strippedImages += 1;
      return '';
    }
    return imgTag;
  });

  // Pass 2 : re-inject lost anchor ids (e.g. `<strong>Presentazione</strong>`
  // becomes `<strong id="Presentazione">…`). Must happen BEFORE anchor
  // transformation so the unwrap step below can tell which fragments are
  // still usable.
  out = restoreAnchors(out, stats);

  // Pass 3 : transform external / internal anchors.
  out = out.replace(
    /<a\b([^>]*)>([\s\S]*?)<\/a>/gi,
    (match, attrs, inner) => transformAnchor(match, `<a${attrs}>`, inner, stats)
  );

  // Pass 4 : any remaining `<a href="#X">` whose target doesn't exist is
  // unwrapped (the visible text stays, the broken link disappears).
  out = unwrapBrokenFragments(out, stats);

  // Pass 5 : remove anchors whose content became empty after image/child
  // strips (e.g. `<a>` that only wrapped a now-removed vatican.va logo).
  // We loop until stable : a link inside a list item can leave the <li>
  // empty, which another pass then removes.
  let previous = '';
  while (previous !== out) {
    previous = out;
    out = out.replace(
      /<a\b[^>]*>([\s\S]*?)<\/a>/gi,
      (match, inner) => (inner.replace(/\s|&nbsp;|<[^>]+\/?>/g, '').length === 0 ? '' : match)
    );
    // Collapse now-empty block-level wrappers.
    out = out.replace(/<(li|p|div|span)\b[^>]*>\s*(?:&nbsp;|\s)*\s*<\/\1>/gi, '');
  }

  return { html: out, stats };
}
