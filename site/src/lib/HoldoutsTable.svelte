<script lang="ts">
  import { activeSource, holdoutFilter } from './stores';
  import type { Source } from './stores';

  export let holdouts: Record<string, any[]> = {};
  export let russianTerm: string = '';

  // Map source → holdout key
  const sourceToKey: Record<string, string> = {
    gdelt: 'news', trends: 'news', wikipedia: 'wikipedia',
    reddit: 'reddit', youtube: 'youtube',
    ngrams: 'news', openalex: 'news', telegram: 'news', religious: 'news',
  };

  $: holdoutKey = sourceToKey[$activeSource] || 'news';
  $: items = holdouts[holdoutKey] || [];
  $: isNews = holdoutKey === 'news';

  // News filter
  $: filteredNews = isNews ? filterNews(items, $holdoutFilter) : items;

  function filterNews(items: any[], filter: string) {
    if (filter === 'all') return items;
    if (filter === 'intl') return items.filter((h: any) => !h.name.endsWith('.ru') && !h.name.endsWith('.ua'));
    if (filter === 'ru') return items.filter((h: any) => h.name.endsWith('.ru'));
    if (filter === 'ua') return items.filter((h: any) => h.name.endsWith('.ua'));
    return items;
  }

  function setFilter(f: string) {
    holdoutFilter.set(f as any);
  }

  const labels: Record<string, string> = {
    news: 'News domains still using old spelling (2025)',
    wikipedia: 'Wikipedia pages still using old spelling (2025)',
    reddit: 'Reddit posts still using old spelling (2025)',
    youtube: 'YouTube videos still using old spelling (2025)',
  };
</script>

<div class="holdouts">
  {#if items.length === 0}
    <p class="empty">No holdout data for this source</p>
  {:else}
    <p class="subtitle">{labels[holdoutKey] || 'Holdouts'}</p>

    {#if isNews}
      <div class="filters">
        {#each [['all','All'],['intl','International'],['ru','.ru domains'],['ua','.ua domains']] as [val, label]}
          <button class:active={$holdoutFilter === val} on:click={() => setFilter(val)}>{label}</button>
        {/each}
      </div>
    {/if}

    <div class="table-wrap">
      <table>
        {#if isNews}
          <thead>
            <tr>
              <th>Domain</th>
              <th>Old %</th>
              <th>Vol</th>
              <th></th>
            </tr>
          </thead>
          <tbody>
            {#each filteredNews as h}
              {@const isRu = h.name.endsWith('.ru')}
              {@const isUa = h.name.endsWith('.ua')}
              <tr class:ru-row={isRu}>
                <td>
                  <a href="https://{h.name}" target="_blank" rel="noopener">{h.name}</a>
                  {#if isRu}<span class="tag ru">.ru</span>{/if}
                  {#if isUa}<span class="tag ua">.ua</span>{/if}
                </td>
                <td class="pct">{h.russian_pct}%</td>
                <td class="vol">{(h.total || 0).toLocaleString()}</td>
                <td><a href="https://www.google.com/search?q=site:{h.name}+{russianTerm}" target="_blank" rel="noopener" class="verify">verify →</a></td>
              </tr>
            {/each}
          </tbody>
        {:else if holdoutKey === 'wikipedia'}
          <tbody>
            {#each items as w}
              <tr>
                <td><a href={w.url} target="_blank" rel="noopener">{w.name}</a></td>
                <td class="vol">{(w.views || 0).toLocaleString()} views</td>
              </tr>
            {/each}
          </tbody>
        {:else if holdoutKey === 'reddit'}
          <tbody>
            {#each items as r}
              <tr>
                <td><a href={r.url} target="_blank" rel="noopener" class="reddit-link">{r.name}</a></td>
                <td class="vol">{r.score ? r.score.toLocaleString() + ' ↑' : ''}</td>
              </tr>
            {/each}
          </tbody>
        {:else if holdoutKey === 'youtube'}
          <tbody>
            {#each items as y}
              <tr>
                <td><a href={y.url} target="_blank" rel="noopener" class="yt-link">{y.name}</a></td>
              </tr>
            {/each}
          </tbody>
        {/if}
      </table>
    </div>
  {/if}
</div>

<style>
  .holdouts { margin-top: 1rem; }
  .empty { color: #9ca3af; text-align: center; padding: 2rem; font-size: 0.85rem; }
  .subtitle { font-size: 0.75rem; color: #6b7280; margin: 0 0 0.5rem; }
  .filters { display: flex; gap: 0.3rem; margin-bottom: 0.5rem; }
  .filters button {
    padding: 0.25rem 0.6rem; border: 1px solid #e5e7eb; border-radius: 4px;
    background: white; font-size: 0.7rem; color: #6b7280; cursor: pointer;
  }
  .filters button.active { border-color: #0057B8; color: #0057B8; font-weight: 600; }
  .table-wrap { max-height: 400px; overflow-y: auto; }
  table { width: 100%; border-collapse: collapse; }
  th { padding: 0.4rem 0.6rem; text-align: left; font-size: 0.65rem; text-transform: uppercase; color: #6b7280; border-bottom: 2px solid #e5e7eb; }
  td { padding: 0.4rem 0.6rem; font-size: 0.8rem; border-bottom: 1px solid #f3f4f6; }
  a { color: var(--text, #1a1a2e); text-decoration: none; border-bottom: 1px dotted #d1d5db; }
  a:hover { border-bottom-style: solid; }
  .reddit-link { color: #FF4500; border-color: #ffccc0; }
  .yt-link { color: #FF0000; border-color: #ffcccc; }
  .pct { color: #dc2626; font-weight: 600; font-family: var(--font-mono, monospace); }
  .vol { color: #9ca3af; font-family: var(--font-mono, monospace); }
  .verify { font-size: 0.65rem; color: #4285F4; border: none; }
  .tag { font-size: 0.6rem; padding: 0.1rem 0.3rem; border-radius: 3px; margin-left: 0.3rem; }
  .tag.ru { color: #dc2626; background: #fef2f2; }
  .tag.ua { color: #0057B8; background: #f0f9ff; }
  .ru-row { background: #fef2f2; }
</style>
