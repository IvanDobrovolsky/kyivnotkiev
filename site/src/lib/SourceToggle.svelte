<script lang="ts">
  import { activeSource, SOURCES, SOURCE_LABELS, SOURCE_COLORS, type Source } from './stores';

  export let pairSlug: string;
  export let availableSources: string[] = [];

  function select(src: Source) {
    activeSource.set(src);
  }

  // Determine which sources have data for this pair
  $: sources = SOURCES.filter(s => availableSources.includes(s));
</script>

<div class="source-toggles">
  {#each sources as src}
    <button
      class="toggle"
      class:active={$activeSource === src}
      style="--src-color: {SOURCE_COLORS[src]}"
      on:click={() => select(src)}
    >
      <span class="dot" style="background: {SOURCE_COLORS[src]}"></span>
      {SOURCE_LABELS[src]}
    </button>
  {/each}
</div>

<style>
  .source-toggles {
    display: flex;
    flex-wrap: wrap;
    gap: 0.35rem;
    margin-bottom: 0.75rem;
  }
  .toggle {
    display: flex;
    align-items: center;
    gap: 0.3rem;
    padding: 0.35rem 0.7rem;
    border: 1px solid #e5e7eb;
    border-radius: 6px;
    background: white;
    font-size: 0.78rem;
    color: #6b7280;
    cursor: pointer;
    transition: all 0.15s;
  }
  .toggle:hover {
    border-color: var(--src-color);
    color: var(--src-color);
  }
  .toggle.active {
    border-color: var(--src-color);
    background: color-mix(in srgb, var(--src-color) 8%, white);
    color: var(--src-color);
    font-weight: 600;
  }
  .dot {
    width: 8px;
    height: 8px;
    border-radius: 50%;
    flex-shrink: 0;
  }
</style>
