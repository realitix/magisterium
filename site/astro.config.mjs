import { defineConfig } from 'astro/config';
import mdx from '@astrojs/mdx';
import remarkSmartypants from 'remark-smartypants';

export default defineConfig({
  site: 'https://magisterium.netlify.app',
  output: 'static',
  trailingSlash: 'always',
  build: {
    format: 'directory',
  },
  integrations: [mdx()],
  markdown: {
    remarkPlugins: [remarkSmartypants],
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
