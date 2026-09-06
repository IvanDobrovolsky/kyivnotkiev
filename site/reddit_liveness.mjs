// Probe Reddit holdout posts for liveness via headless Chrome (passes the
// bot-wall that blocks plain HTTP). Results cached in data/audit/
// reddit_liveness.json — only unknown ids are probed, so routine runs are
// cheap. Usage: node site/reddit_liveness.mjs [--recheck-days N]
import { chromium } from 'playwright-core';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';

const ROOT = new URL('..', import.meta.url).pathname;
const HOLDOUTS = ROOT + 'site/src/data/holdouts_by_pair.json';
const CACHE = ROOT + 'data/audit/reddit_liveness.json';
const RECHECK_DAYS = Number(process.argv.find(a => a.startsWith('--recheck-days='))?.split('=')[1] || 45);

const h = JSON.parse(readFileSync(HOLDOUTS, 'utf8'));
const cache = existsSync(CACHE) ? JSON.parse(readFileSync(CACHE, 'utf8')) : {};
const now = Date.now();
const stale = (e) => !e || (now - (e.checked_at || 0)) > RECHECK_DAYS * 864e5;

const targets = [];
for (const [slug, src] of Object.entries(h)) {
  for (const e of (src.reddit || [])) {
    const id = (e.url || '').split('/').pop();
    if (id && stale(cache[id])) targets.push({ id, url: e.url });
  }
}
console.log(`${targets.length} post(s) to probe`);
if (targets.length) {
  const exe = process.env.CHROME_BIN ||
    '/Users/tati/Library/Caches/ms-playwright/chromium_headless_shell-1234/chrome-headless-shell-mac-arm64/chrome-headless-shell';
  const b = await chromium.launch({ executablePath: exe });
  const ctx = await b.newContext({ userAgent: 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36' });
  const p = await ctx.newPage();
  for (const t of targets) {
    let status = 'unknown';
    for (let attempt = 0; attempt < 2 && status === 'unknown'; attempt++) {
      try {
        await p.goto('https://www.reddit.com/comments/' + t.id, { waitUntil: 'domcontentloaded', timeout: 25000 });
        await p.waitForSelector('body', { timeout: 8000 });
        await p.waitForTimeout(1800);
        const txt = await p.evaluate(() => (document.body ? document.body.innerText.slice(0, 4000) : ''));
        if (!txt) continue;
        const walled = /Welcome to Reddit|whoa there|verify you are human/i.test(txt) && txt.length < 900;
        if (walled) continue;                      // don't record a wall as a status
        const removed = /removed by|deleted by|Sorry, this post (was|has been) (removed|deleted)|\[removed\]|\[deleted\]|content is not available|Post not found/i.test(txt);
        status = removed ? 'removed' : 'live';
      } catch { /* retry once */ }
    }
    if (status !== 'unknown') cache[t.id] = { status, checked_at: now };
    console.log(t.id, status);
    await p.waitForTimeout(700);
  }
  await b.close();
}
writeFileSync(CACHE, JSON.stringify(cache, null, 1));
const counts = Object.values(cache).reduce((a, e) => ((a[e.status] = (a[e.status] || 0) + 1), a), {});
console.log('cache totals:', JSON.stringify(counts));
