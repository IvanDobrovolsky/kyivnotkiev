// Probe Reddit holdout posts for liveness via headless Chrome (passes the bot
// wall that blocks plain HTTP — the JSON endpoint returns 403).
//
// Reads BOTH the shipped holdouts and the ranked-but-unprobed candidate pool
// written by the exporter, because the table now ships probed-live posts ONLY.
// Probing just what shipped could never grow a table: the unprobed remainder
// is where the replacements come from.
//
// Results cached in data/audit/reddit_liveness.json; only unknown or stale ids
// are probed, so repeat runs are cheap.
//
//   node site/reddit_liveness.mjs [--recheck-days=45] [--concurrency=6] [--limit=N]
import { chromium } from 'playwright-core';
import { readFileSync, writeFileSync, existsSync } from 'node:fs';

const ROOT = new URL('..', import.meta.url).pathname;
const HOLDOUTS = ROOT + 'site/src/data/holdouts_by_pair.json';
const CANDIDATES = ROOT + 'data/audit/reddit_holdout_candidates.json';
const CACHE = ROOT + 'data/audit/reddit_liveness.json';
const arg = (k, d) => Number(process.argv.find(a => a.startsWith(`--${k}=`))?.split('=')[1] || d);
const RECHECK_DAYS = arg('recheck-days', 45);
const CONCURRENCY = arg('concurrency', 6);
const LIMIT = arg('limit', 0);

const cache = existsSync(CACHE) ? JSON.parse(readFileSync(CACHE, 'utf8')) : {};
const now = Date.now();
const stale = (e) => !e || (now - (e.checked_at || 0)) > RECHECK_DAYS * 864e5;

const idOf = (u) => (u || '').split('?')[0].replace(/\/$/, '').split('/').pop();
const seen = new Set();
const targets = [];
const add = (url) => {
  const id = idOf(url);
  if (id && !seen.has(id) && stale(cache[id])) { seen.add(id); targets.push({ id }); }
};
if (existsSync(HOLDOUTS)) {
  const h = JSON.parse(readFileSync(HOLDOUTS, 'utf8'));
  for (const src of Object.values(h)) for (const e of (src.reddit || [])) add(e.url);
}
// Candidates are ranked by score, so probing in file order fills the best
// posts first and a partial run still improves every table.
if (existsSync(CANDIDATES)) {
  const c = JSON.parse(readFileSync(CANDIDATES, 'utf8'));
  for (const urls of Object.values(c)) for (const u of urls) add(u);
}
const work = LIMIT ? targets.slice(0, LIMIT) : targets;
console.log(`${work.length} post(s) to probe (concurrency ${CONCURRENCY})`);
if (!work.length) process.exit(0);

const exe = process.env.CHROME_BIN ||
  '/Users/tati/Library/Caches/ms-playwright/chromium_headless_shell-1234/chrome-headless-shell-mac-arm64/chrome-headless-shell';
const b = await chromium.launch({ executablePath: exe });
const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36';

let done = 0, live = 0, removed = 0, unknown = 0;
const save = () => writeFileSync(CACHE, JSON.stringify(cache, null, 1));

async function worker(slice) {
  const ctx = await b.newContext({ userAgent: UA });
  const p = await ctx.newPage();
  // Images and fonts are irrelevant to whether a post exists.
  await p.route('**/*', (route) =>
    ['image', 'font', 'media'].includes(route.request().resourceType()) ? route.abort() : route.continue());
  for (const t of slice) {
    let status = 'unknown';
    for (let attempt = 0; attempt < 2 && status === 'unknown'; attempt++) {
      try {
        await p.goto('https://www.reddit.com/comments/' + t.id, { waitUntil: 'domcontentloaded', timeout: 25000 });
        await p.waitForTimeout(1200);
        const txt = await p.evaluate(() => (document.body ? document.body.innerText.slice(0, 4000) : ''));
        if (!txt) continue;
        if (/Welcome to Reddit|whoa there|verify you are human/i.test(txt) && txt.length < 900) continue;
        // Banned/private/quarantined subreddits count as not-viewable too: the
        // exhibit is "click this and read it", and none of these allow that.
        const dead = /removed by|deleted by|Sorry, this post (was|has been) (removed|deleted)|\[removed\]|\[deleted\]|content is not available|Post not found|page not found|This community is private|has been banned|community is quarantined/i.test(txt);
        status = dead ? 'removed' : 'live';
      } catch { /* retry once */ }
    }
    if (status !== 'unknown') cache[t.id] = { status, checked_at: now };
    status === 'live' ? live++ : status === 'removed' ? removed++ : unknown++;
    if (++done % 50 === 0) { save(); console.log(`${done}/${work.length}  live ${live}  removed ${removed}  unknown ${unknown}`); }
    await p.waitForTimeout(400);
  }
  await ctx.close();
}

const slices = Array.from({ length: CONCURRENCY }, (_, i) =>
  work.filter((_, j) => j % CONCURRENCY === i));
await Promise.all(slices.map(worker));
await b.close();
save();
const totals = Object.values(cache).reduce((a, e) => ((a[e.status] = (a[e.status] || 0) + 1), a), {});
console.log(`done: live ${live}, removed ${removed}, unknown ${unknown}`);
console.log('cache totals:', JSON.stringify(totals));
