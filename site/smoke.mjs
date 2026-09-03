// Runtime smoke: load pages headless, fail on any console error / pageerror.
import { chromium } from 'playwright-core';
const base = process.argv[2] || 'http://localhost:4173';
const paths = ['/', '/uk/', '/pair/kyiv/', '/uk/pair/kyiv/'];
const exe = process.env.CHROME_BIN ||
  '/Users/tati/Library/Caches/ms-playwright/chromium_headless_shell-1234/chrome-headless-shell-mac-arm64/chrome-headless-shell';
const b = await chromium.launch({ executablePath: exe });
const p = await b.newPage();
let failed = false;
p.on('pageerror', e => { console.log('PAGEERROR', e.message.slice(0, 160)); failed = true; });
p.on('console', m => {
  if (m.type() === 'error' && !m.text().includes('cloudflareinsights'))
    { console.log('CONSOLE-ERROR', m.text().slice(0, 160)); failed = true; }
});
for (const path of paths) {
  await p.goto(base + path, { waitUntil: 'load', timeout: 30000 }).catch(e => { console.log('NAV-FAIL', path, e.message.slice(0,80)); failed = true; });
  await p.waitForTimeout(900);
}
await b.close();
console.log(failed ? 'SMOKE FAILED' : 'SMOKE OK');
process.exit(failed ? 1 : 0);
