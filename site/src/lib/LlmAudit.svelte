<script lang="ts">
  import { data } from './stores';

  export let pairSlug: string;

  $: llm = data.llmPerPair?.pairs?.[pairSlug];
  $: hasData = llm && llm.models && llm.models.length > 0;

  $: summary = hasData ? {
    total: llm.models.length,
    ua_pct: Math.round(llm.models.filter((m: any) => m.x_open === 1).length / llm.models.length * 100),
    families: [...new Set(llm.models.map((m: any) => m.family))].length,
  } : null;
</script>

<div class="llm-section">
  <h3>What AI says about this pair</h3>
  <p class="subtitle">{data.llmPerPair?.n_models || 0} LLMs from {data.llmPerPair?.families?.length || 0} labs, 3 prompt variants each</p>

  {#if !hasData}
    <p class="empty">No LLM data for this pair</p>
  {:else}
    <div class="summary">
      <div class="stat">
        <span class="val">{summary.total}</span>
        <span class="lbl">models tested</span>
      </div>
      <div class="stat">
        <span class="val" style="color:#0057B8">{summary.ua_pct}%</span>
        <span class="lbl">use Ukrainian spelling</span>
      </div>
      <div class="stat">
        <span class="val">{summary.families}</span>
        <span class="lbl">model families</span>
      </div>
    </div>

    <div class="models">
      {#each llm.models.sort((a, b) => (b.x_open || 0) - (a.x_open || 0)) as m}
        <div class="model-row" class:ua={m.x_open === 1} class:ru={m.x_open === 0}>
          <span class="model-name">{m.key || m.model}</span>
          <span class="model-family">{m.family}</span>
          <span class="model-result">{m.x_open === 1 ? '✓ UA' : '✗ RU'}</span>
        </div>
      {/each}
    </div>
  {/if}
</div>

<style>
  .llm-section { margin-top: 1.5rem; }
  h3 { font-size: 1.1rem; font-weight: 700; margin-bottom: 0.25rem; }
  .subtitle { font-size: 0.75rem; color: #6b7280; margin-bottom: 0.75rem; }
  .empty { color: #9ca3af; text-align: center; padding: 2rem; }
  .summary { display: flex; gap: 1.5rem; margin-bottom: 1rem; }
  .stat { display: flex; flex-direction: column; }
  .val { font-size: 1.5rem; font-weight: 700; }
  .lbl { font-size: 0.7rem; color: #6b7280; }
  .models { max-height: 300px; overflow-y: auto; }
  .model-row { display: flex; align-items: center; gap: 0.5rem; padding: 0.3rem 0.5rem; border-bottom: 1px solid #f3f4f6; font-size: 0.8rem; }
  .model-row.ua { background: #f0fdf4; }
  .model-row.ru { background: #fef2f2; }
  .model-name { flex: 1; font-weight: 500; }
  .model-family { color: #6b7280; font-size: 0.7rem; }
  .model-result { font-weight: 600; font-size: 0.75rem; }
  .model-row.ua .model-result { color: #059669; }
  .model-row.ru .model-result { color: #dc2626; }
</style>
