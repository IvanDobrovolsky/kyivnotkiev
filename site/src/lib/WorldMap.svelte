<script lang="ts">
  import { onMount } from 'svelte';
  import * as d3 from 'd3';
  import * as topojson from 'topojson-client';
  import { activeSource, data } from './stores';

  export let pairSlug: string;

  let container: HTMLDivElement;

  $: visible = $activeSource === 'gdelt';
  $: if (container && visible && pairSlug) drawMap();

  async function drawMap() {
    if (!container) return;
    container.innerHTML = '';

    const countries = data.countriesByPair[pairSlug] || {};
    if (Object.keys(countries).length === 0) {
      container.innerHTML = '<div style="padding:2rem;text-align:center;color:#9ca3af;font-size:0.85rem;">No country data for this pair</div>';
      return;
    }

    const rect = container.getBoundingClientRect();
    const w = rect.width;
    const h = Math.min(w * 0.55, 400);

    const svg = d3.select(container).append('svg')
      .attr('viewBox', `0 0 ${w} ${h}`)
      .style('width', '100%');

    const projection = d3.geoNaturalEarth1().fitSize([w, h], { type: 'Sphere' } as any);
    const path = d3.geoPath(projection);

    // Color scale
    const adoptionColor = d3.scaleLinear<string>()
      .domain([0, 50, 100])
      .range(['#dc2626', '#f5f5f5', '#0057B8']);

    // Load world topology
    const world = await fetch('https://cdn.jsdelivr.net/npm/world-atlas@2/countries-110m.json').then(r => r.json());
    const features = topojson.feature(world, world.objects.countries) as any;

    // Tooltip
    const tooltip = d3.select(container).append('div')
      .style('position', 'absolute').style('background', 'white')
      .style('border', '1px solid #d1d5db').style('border-radius', '6px')
      .style('padding', '6px 10px').style('font-size', '12px')
      .style('pointer-events', 'none').style('opacity', '0')
      .style('box-shadow', '0 2px 8px rgba(0,0,0,0.1)').style('z-index', '10');

    svg.selectAll('path')
      .data(features.features)
      .enter().append('path')
      .attr('d', path as any)
      .attr('fill', (d: any) => {
        const iso = String(d.id).padStart(3, '0');
        const c = countries[iso];
        return c ? adoptionColor(c.adoption) : '#f5f5f5';
      })
      .attr('stroke', '#e5e7eb').attr('stroke-width', 0.4)
      .on('mouseenter', function(event: MouseEvent, d: any) {
        const iso = String(d.id).padStart(3, '0');
        const c = countries[iso];
        if (!c) return;
        d3.select(this).attr('stroke', '#333').attr('stroke-width', 1.5);
        tooltip.style('opacity', '1')
          .html(`<strong>${c.name}</strong><br/>Adoption: ${c.adoption}%<br/>Volume: ${(c.total || 0).toLocaleString()}`);
        const cr = container.getBoundingClientRect();
        const x = event.clientX - cr.left;
        const y = event.clientY - cr.top;
        tooltip.style('left', (x + 16) + 'px').style('top', (y - 20) + 'px');
      })
      .on('mouseleave', function() {
        d3.select(this).attr('stroke', '#e5e7eb').attr('stroke-width', 0.4);
        tooltip.style('opacity', '0');
      });
  }

  onMount(() => { if (container && visible) drawMap(); });
</script>

{#if visible}
  <div class="map-section">
    <h3>Country Distribution</h3>
    <p class="subtitle">Adoption by country based on news domain ccTLD mapping (2025)</p>
    <div bind:this={container} class="map" style="position:relative;"></div>
  </div>
{/if}

<style>
  .map-section { margin-top: 1.5rem; }
  h3 { font-size: 1.1rem; font-weight: 700; margin-bottom: 0.25rem; }
  .subtitle { font-size: 0.75rem; color: #6b7280; margin-bottom: 0.75rem; }
  .map { border: 1px solid #f3f4f6; border-radius: 8px; overflow: hidden; }
</style>
