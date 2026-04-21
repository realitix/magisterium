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
  /\/img\/vuoto\.gif/i,
  /\/img\/back\.jpg/i,
  /\/img\/up\.jpg/i,
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
  };
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
  const stats = { strippedDead: 0, rewrittenInternal: 0, externalKept: 0, strippedImages: 0 };

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

  // Pass 2 : transform anchors. Non-greedy inner content, allows nested-ish
  // HTML but not nested <a> (which is invalid in HTML anyway).
  out = out.replace(
    /<a\b([^>]*)>([\s\S]*?)<\/a>/gi,
    (match, attrs, inner) => transformAnchor(match, `<a${attrs}>`, inner, stats)
  );

  // Pass 3 : collapse now-empty list items / paragraphs left by the strip.
  out = out.replace(/<(li|p)\b[^>]*>\s*<\/\1>/gi, '');

  return { html: out, stats };
}
