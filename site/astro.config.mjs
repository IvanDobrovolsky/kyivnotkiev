import { defineConfig } from 'astro/config';

import cloudflare from '@astrojs/cloudflare';

export default defineConfig({
  output: 'static',
  i18n: {
    locales: ['en', 'uk'],
    defaultLocale: 'en',
    routing: { prefixDefaultLocale: false },
  },
  redirects: { '/methodology': '/', '/uk/methodology': '/uk/' },
  site: 'https://kyivnotkiev.org',
  adapter: cloudflare(),
});