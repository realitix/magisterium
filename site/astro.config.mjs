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
const INDEX_JSONL = path.join(
  __dirname,
  '..',
  'magisterium',
  '_metadata',
  'index.jsonl',
);
const indexRaw = fs.readFileSync(INDEX_JSONL, 'utf8');
const { bySignature, slugs } = buildIncipitIndex(indexRaw);

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
    rehypePlugins: [[rehypeIncipitLink, { bySignature, slugs }]],
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
