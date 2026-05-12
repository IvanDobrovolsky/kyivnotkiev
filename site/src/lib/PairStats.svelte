<script lang="ts">
  import { data, SOURCES, SOURCE_LABELS, SOURCE_COLORS } from './stores';

  export let pairSlug: string;

  $: pair = data.manifest.pairs.find((p: any) => p.slug === pairSlug);
  $: ts = data.timeseries[pairSlug] || {};

  // Per-source stats
  $: sourceStats = SOURCES.map(src => {
    const series = ts[src] || [];
    if (series.length === 0) return null;
    const last = series[series.length - 1];
    const total = series.reduce((s: number, d: any) => s + (d.ukr || 0) + (d.rus || 0), 0);
    return {
      source: src,
      label: SOURCE_LABELS[src],
      color: SOURCE_COLORS[src],
      adoption: last?.adoption ?? 0,
      total,
      points: series.length,
    };
  }).filter(Boolean);

  function fmtN(n: number) {
    if (n >= 1e6) return (n / 1e6).toFixed(1) + 'M';
    if (n >= 1e3) return (n / 1e3).toFixed(1) + 'K';
    return String(n);
  }
</script>

{#if pair}
  <div class="stats">
    <div class="header">
      <div class="pair-name">
        <span class="old">{pair.russian}</span>
        <span class="arrow">→</span>
        <span class="new">{pair.ukrainian}</span>
      </div>
      <div class="adoption" style="color: {pair.adoption >= 70 ? '#059669' : pair.adoption >= 40 ? '#d97706' : '#dc2626'}">
        {pair.adoption}% adopted
      </div>
    </div>

    <div class="source-grid">
      {#each sourceStats as s}
        <div class="source-stat">
          <span class="dot" style="background:{s.color}"></span>
          <span class="source-name">{s.label}</span>
          <span class="source-adopt" style="color:{s.color}">{s.adoption.toFixed(0)}%</span>
          <span class="source-vol">{fmtN(s.total)}</span>
        </div>
      {/each}
    </div>
  </div>
{/if}

<style>
  .stats { margin-bottom: 1rem; }
  .header { display: flex; justify-content: space-between; align-items: baseline; margin-bottom: 0.75rem; flex-wrap: wrap; gap: 0.5rem; }
  .pair-name { font-size: 1.3rem; font-weight: 700; }
  .old { color: #dc2626; }
  .arrow { color: #9ca3af; margin: 0 0.3rem; }
  .new { color: #0057B8; }
  .adoption { font-size: 1.1rem; font-weight: 700; }
  .source-grid { display: grid; grid-template-columns: repeat(auto-fill, minmax(140px, 1fr)); gap: 0.4rem; }
  .source-stat { display: flex; align-items: center; gap: 0.3rem; font-size: 0.78rem; padding: 0.3rem 0.5rem; background: #fafafa; border-radius: 4px; }
  .dot { width: 6px; height: 6px; border-radius: 50%; flex-shrink: 0; }
  .source-name { flex: 1; color: #6b7280; }
  .source-adopt { font-weight: 600; font-family: var(--font-mono); }
  .source-vol { color: #9ca3af; font-size: 0.7rem; font-family: var(--font-mono); }
</style>
