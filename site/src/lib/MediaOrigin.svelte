<script lang="ts">
  import { activeSource, data } from './stores';

  export let pairSlug: string;

  $: visible = $activeSource === 'gdelt';
  $: origins = data.domainOrigins[pairSlug] || {};
  $: hasData = visible && Object.keys(origins).length > 0;

  $: slices = hasData ? [
    { label: 'Ukrainian (.ua)', value: origins.ua?.total || 0, color: '#0057B8', pct: origins.ua?.adoption },
    { label: 'Russian (.ru)', value: origins.ru?.total || 0, color: '#dc2626', pct: origins.ru?.adoption },
    { label: 'International', value: origins.intl?.total || 0, color: '#6b7280', pct: origins.intl?.adoption },
  ].filter(s => s.value > 0) : [];

  $: total = slices.reduce((s, x) => s + x.value, 0);
</script>

{#if visible && hasData}
  <div class="origin">
    <h3>Media Origin</h3>
    <p class="subtitle">News domain breakdown by country TLD</p>
    <div class="bars">
      {#each slices as s}
        <div class="bar-row">
          <span class="bar-label">{s.label}</span>
          <div class="bar-track">
            <div class="bar-fill" style="width:{s.value / total * 100}%; background:{s.color};"></div>
          </div>
          <span class="bar-val">{(s.value / total * 100).toFixed(0)}%</span>
          <span class="bar-adopt" style="color:{s.color};">{s.pct?.toFixed(0) || 0}% UA</span>
        </div>
      {/each}
    </div>
  </div>
{/if}

<style>
  .origin { margin-top: 1.5rem; }
  h3 { font-size: 1.1rem; font-weight: 700; margin-bottom: 0.25rem; }
  .subtitle { font-size: 0.75rem; color: #6b7280; margin-bottom: 0.75rem; }
  .bars { display: flex; flex-direction: column; gap: 0.5rem; }
  .bar-row { display: flex; align-items: center; gap: 0.5rem; }
  .bar-label { font-size: 0.8rem; width: 120px; flex-shrink: 0; }
  .bar-track { flex: 1; height: 20px; background: #f3f4f6; border-radius: 4px; overflow: hidden; }
  .bar-fill { height: 100%; border-radius: 4px; transition: width 0.3s; }
  .bar-val { font-size: 0.75rem; color: #6b7280; width: 35px; text-align: right; font-family: var(--font-mono); }
  .bar-adopt { font-size: 0.7rem; width: 50px; text-align: right; font-weight: 600; }
</style>
