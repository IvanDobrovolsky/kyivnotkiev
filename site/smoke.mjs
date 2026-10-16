// Runtime smoke: load pages headless, fail on any console error / pageerror.
import { chromium } from 'playwright-core';
const base = process.argv[2] || 'http://localhost:4173';
// Source deep-links exercise per-source render paths (holdouts, clusters) that
// the default view never runs — the `filtered` ReferenceError shipped because
// nothing loaded ?source=gdelt.
const paths = ['/', '/uk/', '/pair/kyiv/', '/uk/pair/kyiv/',
               '/pair/chicken-kyiv/?source=gdelt', '/pair/kyiv/?source=youtube'];
// Let playwright-core resolve the browser for its own version. A hard-coded
// chromium_headless_shell-1234 path blocked every site/ push once another
// project's `playwright install` garbage-collected 1234 and left only 1223
// (2026-10-13). If it is missing, the error names the fix:
//   cd site && npx playwright-core install chromium-headless-shell
// Installing that way also registers a link, so the next GC keeps it.
const b = await chromium.launch(process.env.CHROME_BIN ? { executablePath: process.env.CHROME_BIN } : {});
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
  // Adoption mode is a click away from the default view; a ReferenceError in
  // its draw path shipped because nothing ever clicked it.
  const btn = await p.$('.chart-mode[data-chart="adoption"]');
  if (btn) { await btn.click().catch(() => {}); await p.waitForTimeout(500); }
}
await b.close();
console.log(failed ? 'SMOKE FAILED' : 'SMOKE OK');
process.exit(failed ? 1 : 0);
