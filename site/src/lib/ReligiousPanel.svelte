<script lang="ts">
  import { data } from './stores';

  export let pairSlug: string;

  $: denominations = (data.religious?.denominations || []).map((d: any) => {
    const pairData = (d.pairs || []).find((p: any) => p.pair_slug === pairSlug || p.pair_id === pairSlug);
    return { ...d, pairData };
  }).filter((d: any) => d.pairData);

  $: hasData = denominations.length > 0;
</script>

<div class="religious-section">
  <h3>Religious Institutions</h3>
  <p class="subtitle">How major religious bodies refer to this pair in English</p>

  {#if !hasData}
    <p class="empty">No religious-press mentions for this pair across the {data.religious?.n_denominations || 0} tracked institutions.</p>
  {:else}
    <div class="denoms">
      {#each denominations as d}
        {@const p = d.pairData}
        <div class="denom">
          <div class="denom-header">
            <span class="denom-name" style="color:{d.color}">{d.short || d.name}</span>
            <span class="denom-stance">{d.stance}</span>
          </div>
          <div class="denom-stats">
            <span class="ua-pct" style="color:{p.ua_pct > 50 ? '#0057B8' : '#dc2626'}">{p.ua_pct}% UA</span>
            <span class="counts">({p.ua} UA / {p.ru} RU)</span>
          </div>
        </div>
      {/each}
    </div>
  {/if}
</div>

<style>
  .religious-section { margin-top: 1.5rem; }
  h3 { font-size: 1.1rem; font-weight: 700; margin-bottom: 0.25rem; }
  .subtitle { font-size: 0.75rem; color: #6b7280; margin-bottom: 0.75rem; }
  .empty { color: #9ca3af; font-size: 0.85rem; }
  .denoms { display: flex; flex-direction: column; gap: 0.5rem; }
  .denom { display: flex; justify-content: space-between; align-items: center; padding: 0.5rem 0.75rem; background: #fafafa; border-radius: 6px; }
  .denom-header { display: flex; flex-direction: column; }
  .denom-name { font-size: 0.85rem; font-weight: 600; }
  .denom-stance { font-size: 0.7rem; color: #9ca3af; }
  .denom-stats { text-align: right; }
  .ua-pct { font-size: 1rem; font-weight: 700; }
  .counts { font-size: 0.7rem; color: #9ca3af; display: block; }
</style>
