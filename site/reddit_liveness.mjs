// Probe Reddit holdout posts for liveness via headless Chrome (passes the bot
// wall that blocks plain HTTP — the JSON endpoint returns 403).
//
// Classification reads the SSR'd <shreddit-post> element's own attributes, NOT
// the page text. Page text was the previous approach and it was wrong: the
// first 4KB of body innerText runs into the COMMENT TREE, so a healthy post
// with one removed comment matched /\[removed\]/ and was filed as dead. That
// single defect put 2,300 false "removed" verdicts in the cache and starved
// every table of the posts it should have shipped.
//
// The attributes are unambiguous and server-rendered:
//   post-title        "[deleted by user]" / "[removed]" on a dead post
//   author            "[deleted]" when the ACCOUNT went, post may still stand
//   archived          present when the post is frozen (no new votes/comments)
//   item-state        "UNMODERATED" on a healthy post
//   moderation-verdict non-empty when a mod acted
//
// Reads BOTH the shipped holdouts and the ranked-but-unprobed candidate pool
// written by the exporter, because the table ships probed-live posts ONLY.
//
//   node site/reddit_liveness.mjs [--recheck-days=45] [--concurrency=4] [--limit=N]
import { chromium } from 'playwright-core';
import { readFileSync, writeFileSync, existsSync, renameSync } from 'node:fs';

const ROOT = new URL('..', import.meta.url).pathname;
const HOLDOUTS = ROOT + 'site/src/data/holdouts_by_pair.json';
const CANDIDATES = ROOT + 'data/audit/reddit_holdout_candidates.json';
const CACHE = ROOT + 'data/audit/reddit_liveness.json';
const arg = (k, d) => Number(process.argv.find(a => a.startsWith(`--${k}=`))?.split('=')[1] || d);
const RECHECK_DAYS = arg('recheck-days', 45);
const CONCURRENCY = arg('concurrency', 4);
const LIMIT = arg('limit', 0);
// Bump when the classifier changes, so every stale-schema verdict is re-probed
// instead of being trusted. This is what retires the comment-tree defect.
const SCHEMA = 2;

const cache = existsSync(CACHE) ? JSON.parse(readFileSync(CACHE, 'utf8')) : {};
const now = Date.now();
const stale = (e) => !e || e.schema !== SCHEMA || (now - (e.checked_at || 0)) > RECHECK_DAYS * 864e5;

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
console.log(`${work.length} post(s) to probe (concurrency ${CONCURRENCY}, schema ${SCHEMA})`);
if (!work.length) process.exit(0);

const exe = process.env.CHROME_BIN ||
  '/Users/tati/Library/Caches/ms-playwright/chromium_headless_shell-1234/chrome-headless-shell-mac-arm64/chrome-headless-shell';
const b = await chromium.launch({ executablePath: exe });
const UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0 Safari/537.36';

let done = 0;
const tally = {};
const save = () => {
  writeFileSync(CACHE + '.tmp', JSON.stringify(cache, null, 1));
  renameSync(CACHE + '.tmp', CACHE);
};

// Runs in the page. Returns the post's own attributes, never the comment tree.
const READ = () => {
  const post = document.querySelector('shreddit-post');
  const txt = (document.body ? document.body.innerText : '').slice(0, 1500);
  if (!post) {
    if (/whoa there|verify you are human/i.test(txt) && txt.length < 900) return { wall: true };
    if (/community is private|has been banned|community is quarantined/i.test(txt)) return { gone: 'banned' };
    if (/Post not found|page not found|Sorry, there doesn.t seem to be anything here/i.test(txt)) return { gone: 'notfound' };
    return { wall: true };            // shell without a post: treat as retryable
  }
  const at = (n) => post.getAttribute(n);
  return {
    title: at('post-title') || '',
    author: at('author') || '',
    archived: post.hasAttribute('archived'),
    locked: post.hasAttribute('locked'),
    verdict: at('moderation-verdict') || '',
    state: at('item-state') || '',
    score: Number(at('score') || 0),
    created: at('created-timestamp') || '',
    sub: at('subreddit-prefixed-name') || '',
  };
};

function classify(d) {
  if (d.wall) return null;                       // retry
  if (d.gone) return { status: d.gone };
  const dead = /^\[(deleted|removed)( by user)?\]$/i.test(d.title.trim());
  if (dead) return { status: 'removed', reason: 'post deleted' };
  if (d.verdict) return { status: 'removed', reason: 'moderated: ' + d.verdict };
  return {
    status: 'live',
    archived: d.archived, locked: d.locked,
    author_deleted: d.author === '[deleted]',
    score: d.score, created: d.created.slice(0, 10), sub: d.sub,
  };
}

async function worker(slice, wi) {
  let ctx = await b.newContext({ userAgent: UA });
  let p = await ctx.newPage();
  const route = (pg) => pg.route('**/*', (r) =>
    ['image', 'font', 'media', 'stylesheet'].includes(r.request().resourceType()) ? r.abort() : r.continue());
  await route(p);
  let walls = 0;
  for (const t of slice) {
    let verdict = null;
    for (let attempt = 0; attempt < 3 && !verdict; attempt++) {
      try {
        await p.goto('https://www.reddit.com/comments/' + t.id,
          { waitUntil: 'domcontentloaded', timeout: 30000 });
        await p.waitForTimeout(900);
        verdict = classify(await p.evaluate(READ));
      } catch { /* navigation raced or timed out — retry */ }
      if (!verdict) await p.waitForTimeout(1500 * (attempt + 1));
    }
    if (verdict) {
      cache[t.id] = { ...verdict, schema: SCHEMA, checked_at: now };
      walls = 0;
    } else if (++walls >= 5) {
      // Sustained failure means this context is walled. A fresh context with
      // fresh cookies clears it; sleeping without one does not.
      console.log(`  worker ${wi}: 5 consecutive failures, recycling context`);
      await ctx.close();
      ctx = await b.newContext({ userAgent: UA });
      p = await ctx.newPage();
      await route(p);
      await p.waitForTimeout(20000);
      walls = 0;
    }
    const k = verdict ? verdict.status : 'unknown';
    tally[k] = (tally[k] || 0) + 1;
    if (++done % 50 === 0) {
      save();
      console.log(`${done}/${work.length}  ` +
        Object.entries(tally).map(([a, c]) => `${a} ${c}`).join('  '));
    }
    await p.waitForTimeout(350);
  }
  await ctx.close();
}

const slices = Array.from({ length: CONCURRENCY }, (_, i) =>
  work.filter((_, j) => j % CONCURRENCY === i));
await Promise.all(slices.map(worker));
await b.close();
save();
const totals = Object.values(cache).reduce((a, e) => ((a[e.status] = (a[e.status] || 0) + 1), a), {});
const liveArr = Object.values(cache).filter((e) => e.status === 'live');
console.log(`done: ${JSON.stringify(tally)}`);
console.log('cache totals:', JSON.stringify(totals));
console.log(`live: ${liveArr.length}, of which archived ${liveArr.filter(e => e.archived).length}, ` +
  `locked ${liveArr.filter(e => e.locked).length}, author-deleted ${liveArr.filter(e => e.author_deleted).length}`);
