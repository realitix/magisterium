import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import remarkSmartypants from 'remark-smartypants';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import {
  rehypeIncipitLink,
  buildIncipitIndex,
} from './src/plugins/rehype-incipit-link.mjs';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const MAG_INDEX = path.join(
  __dirname, '..', 'magisterium', '_metadata', 'index.jsonl',
);
const LIVRES_INDEX = path.join(
  __dirname, '..', 'livres', '_metadata', 'index.jsonl',
);
// Lit les deux index (magistère + livres) — l'auto-linking des incipits
// fonctionne ainsi pour les ouvrages non magistériels (Billot, Cajetan,
// Bellarmin, Suárez, Hippolyte, Rore Sanctifica…).
const indexRaw = [MAG_INDEX, LIVRES_INDEX]
  .filter((p) => fs.existsSync(p))
  .map((p) => fs.readFileSync(p, 'utf8'))
  .join('\n');
const { bySignature, slugs, langueOriginaleBySlug } = buildIncipitIndex(indexRaw);

export default defineConfig({
  site: 'https://magisteria.app',
  output: 'static',
  trailingSlash: 'always',
  build: {
    format: 'directory',
  },
  integrations: [mdx()],
  markdown: {
    remarkPlugins: [remarkSmartypants],
    rehypePlugins: [[rehypeIncipitLink, { bySignature, slugs, langueOriginaleBySlug }]],
    shikiConfig: {
      theme: 'github-light',
    },
  },
  vite: {
    server: {
      fs: {
        // Autoriser la lecture du corpus parent
        allow: ['..'],
      },
    },
  },
});
