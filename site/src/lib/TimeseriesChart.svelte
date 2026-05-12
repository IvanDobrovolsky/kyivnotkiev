<script lang="ts">
  import { onMount, afterUpdate } from 'svelte';
  import * as d3 from 'd3';
  import { activeSource, chartMode, lineMode, showEvents, SOURCE_COLORS, SOURCE_LABELS, data } from './stores';
  import type { Source } from './stores';

  export let pairSlug: string;

  let container: HTMLDivElement;
  let legendContainer: HTMLDivElement;

  // Reactive redraws
  $: if (container && pairSlug) draw($activeSource, $chartMode, $lineMode, $showEvents);

  function draw(source: Source, mode: string, lm: string, events: boolean) {
    if (!container) return;
    container.innerHTML = '';
    if (legendContainer) legendContainer.innerHTML = '';

    const pairData = data.timeseries[pairSlug];
    if (!pairData) return;

    const series = pairData[source];
    if (!series || series.length < 2) {
      container.innerHTML = `<div style="display:flex;align-items:center;justify-content:center;height:100%;color:#9ca3af;font-size:0.85rem;">No ${SOURCE_LABELS[source]} data for this pair</div>`;
      return;
    }

    const rect = container.getBoundingClientRect();
    const margin = { top: 30, right: 10, bottom: 28, left: 45 };
    const width = rect.width - margin.left - margin.right;
    const height = rect.height - margin.top - margin.bottom;
    if (width <= 0 || height <= 0) return;

    const parseDate = d3.timeParse('%Y-%m');
    const svg = d3.select(container)
      .append('svg')
      .attr('viewBox', `0 0 ${rect.width} ${rect.height}`)
      .append('g')
      .attr('transform', `translate(${margin.left},${margin.top})`);

    const allDates = series.map((d: any) => parseDate(d.date)!).filter(Boolean);
    const dataMinDate = d3.min(allDates) as Date;
    const dataMaxDate = d3.max(allDates) as Date;
    const xStart = new Date(dataMinDate.getFullYear(), dataMinDate.getMonth() - 1, 1);
    const xEnd = new Date(dataMaxDate.getFullYear(), dataMaxDate.getMonth() + 1, 15);
    const x = d3.scaleTime().domain([xStart, xEnd]).range([0, width]);

    // Y axis
    let y: d3.ScaleLinear<number, number>;
    let yTicks: number[];

    if (mode === 'volume') {
      const getVal = (d: any) => {
        if (lm === 'ukrainian') return d.ukr || 0;
        if (lm === 'russian') return d.rus || 0;
        return Math.max(d.ukr || 0, d.rus || 0);
      };
      const maxVal = Math.max(...series.map(getVal).filter((v: number) => v > 0), 1);
      const yMax = maxVal * 1.1;
      y = d3.scaleLinear().domain([0, yMax]).range([height, 0]);
      const step = yMax / 4;
      yTicks = [0, step, step * 2, step * 3, yMax].map(v => Math.round(v));
    } else {
      y = d3.scaleLinear().domain([0, 100]).range([height, 0]);
      yTicks = [0, 25, 50, 75, 100];
    }

    // Grid
    svg.append('g').selectAll('line').data(yTicks).enter().append('line')
      .attr('x1', 0).attr('x2', width)
      .attr('y1', (d: number) => y(d)).attr('y2', (d: number) => y(d))
      .attr('stroke', '#e5e7eb').attr('stroke-dasharray', '3,4');

    // Y labels
    const fmtY = (d: number) => {
      if (mode === 'volume') {
        if (d >= 1e6) return (d / 1e6).toFixed(1) + 'M';
        if (d >= 1e3) return (d / 1e3).toFixed(0) + 'K';
        return d === 0 ? '0' : String(Math.round(d));
      }
      return d + '%';
    };
    svg.append('g').selectAll('text').data(yTicks).enter().append('text')
      .attr('x', -8).attr('y', (d: number) => y(d) + 4)
      .attr('text-anchor', 'end').attr('fill', '#9ca3af').attr('font-size', '10px')
      .text(fmtY);

    // X axis
    const xAxis = source === 'ngrams'
      ? d3.axisBottom(x).ticks(d3.timeYear.every(10)).tickSize(0).tickPadding(8).tickFormat(d3.timeFormat('%Y') as any)
      : d3.axisBottom(x).ticks(d3.timeYear.every(1)).tickSize(0).tickPadding(8).tickFormat(d3.timeFormat('%Y') as any);
    svg.append('g').attr('transform', `translate(0,${height})`).call(xAxis)
      .selectAll('text').attr('fill', '#9ca3af').attr('font-size', '12px');
    svg.selectAll('.domain').attr('stroke', '#e5e7eb');

    // Event markers
    if (events) {
      const globalEvents = (data.timeseries.events || []).map((e: any) => ({...e, type: 'global'}));
      const customEvents = (data.pairEvents[pairSlug] || []).map((e: any) => ({...e, type: 'pair'}));
      const allEvents = [...globalEvents, ...customEvents].sort((a: any, b: any) => a.date.localeCompare(b.date));

      let num = 0;
      allEvents.forEach((evt: any) => {
        const date = parseDate(evt.date);
        if (!date || date < xStart || date > xEnd) return;
        num++;
        const xPos = x(date);
        const isPair = evt.type === 'pair';

        svg.append('line').attr('x1', xPos).attr('x2', xPos)
          .attr('y1', -10).attr('y2', height)
          .attr('stroke', isPair ? '#0057B8' : '#d1d5db').attr('stroke-width', 1)
          .attr('stroke-dasharray', '4,3').attr('opacity', isPair ? 0.5 : 0.3);

        svg.append('circle').attr('cx', xPos).attr('cy', -18).attr('r', 9)
          .attr('fill', isPair ? '#0057B8' : '#e5e7eb');
        svg.append('text').attr('x', xPos).attr('y', -14)
          .attr('text-anchor', 'middle').attr('fill', isPair ? '#fff' : '#6b7280')
          .attr('font-size', '10px').attr('font-weight', '700').text(num);

        if (legendContainer) {
          const item = document.createElement('span');
          item.className = 'legend-item';
          item.style.cssText = `font-size:0.78rem; color:${isPair ? '#0057B8' : '#6b7280'}; ${isPair ? 'font-weight:600;' : ''}`;
          item.innerHTML = `<span style="display:inline-flex;align-items:center;justify-content:center;width:18px;height:18px;border-radius:50%;background:${isPair ? '#0057B8' : '#e5e7eb'};color:${isPair ? '#fff' : '#6b7280'};font-size:10px;font-weight:700;margin-right:4px;">${num}</span>${evt.label} (${evt.date})`;
          legendContainer.appendChild(item);
        }
      });
    }

    // Lines
    const color = SOURCE_COLORS[source];

    if (mode === 'volume') {
      if (lm !== 'russian') {
        svg.append('path').datum(series).attr('fill', 'none')
          .attr('stroke', color).attr('stroke-width', 3)
          .attr('d', d3.line<any>().x(d => x(parseDate(d.date)!)).y(d => y(d.ukr || 0)).curve(d3.curveMonotoneX));
      }
      if (lm !== 'ukrainian') {
        svg.append('path').datum(series).attr('fill', 'none')
          .attr('stroke', '#dc2626').attr('stroke-width', lm === 'russian' ? 3 : 1.5)
          .attr('stroke-dasharray', lm === 'russian' ? 'none' : '6,4')
          .attr('opacity', lm === 'russian' ? 1 : 0.5)
          .attr('d', d3.line<any>().x(d => x(parseDate(d.date)!)).y(d => y(d.rus || 0)).curve(d3.curveMonotoneX));
      }
    } else {
      if (lm !== 'russian') {
        svg.append('path').datum(series).attr('fill', 'none')
          .attr('stroke', color).attr('stroke-width', 3)
          .attr('d', d3.line<any>().x(d => x(parseDate(d.date)!)).y(d => y(d.adoption)).curve(d3.curveMonotoneX));
      }
      if (lm !== 'ukrainian') {
        svg.append('path').datum(series).attr('fill', 'none')
          .attr('stroke', '#dc2626').attr('stroke-width', lm === 'russian' ? 3 : 1.5)
          .attr('stroke-dasharray', lm === 'russian' ? 'none' : '6,4')
          .attr('opacity', lm === 'russian' ? 1 : 0.5)
          .attr('d', d3.line<any>().x(d => x(parseDate(d.date)!)).y(d => y(100 - d.adoption)).curve(d3.curveMonotoneX));
      }
    }

    // Crosshair tooltip
    const tooltip = d3.select(container).append('div')
      .style('position', 'absolute').style('background', 'white')
      .style('border', '1px solid #d1d5db').style('border-radius', '8px')
      .style('padding', '10px 14px').style('font-size', '13px')
      .style('pointer-events', 'none').style('opacity', '0')
      .style('box-shadow', '0 4px 16px rgba(0,0,0,0.12)').style('z-index', '10')
      .style('white-space', 'nowrap').style('line-height', '1.6');

    const crosshair = svg.append('line').attr('y1', 0).attr('y2', height)
      .attr('stroke', '#d1d5db').attr('stroke-width', 1)
      .attr('stroke-dasharray', '4,3').style('opacity', 0);

    const dates = series.map((d: any) => parseDate(d.date)!.getTime());

    svg.append('rect').attr('width', width).attr('height', height).attr('fill', 'transparent')
      .on('mousemove', function(event: MouseEvent) {
        const [mx] = d3.pointer(event);
        const hoveredDate = x.invert(mx);
        const t = hoveredDate.getTime();
        const dateStr = d3.timeFormat('%b %Y')(hoveredDate);

        if (t < dates[0] || t > dates[dates.length - 1] + 45 * 86400000) {
          tooltip.style('opacity', '0');
          crosshair.style('opacity', 0);
          return;
        }

        crosshair.attr('x1', mx).attr('x2', mx).style('opacity', 1);

        let lo = 0, hi = dates.length - 1;
        while (hi - lo > 1) { const mid = (lo + hi) >> 1; if (dates[mid] <= t) lo = mid; else hi = mid; }
        const rawFrac = (dates[hi] - dates[lo]) > 0 ? (t - dates[lo]) / (dates[hi] - dates[lo]) : 0;
        const frac = Math.max(0, Math.min(1, rawFrac));

        let html = `<strong>${dateStr}</strong>`;
        if (mode === 'volume') {
          const ukr = Math.round((series[lo].ukr || 0) + frac * ((series[hi].ukr || 0) - (series[lo].ukr || 0)));
          const rus = Math.round((series[lo].rus || 0) + frac * ((series[hi].rus || 0) - (series[lo].rus || 0)));
          const total = ukr + rus;
          const adopt = total > 0 ? Math.round(ukr / total * 1000) / 10 : 0;
          html += `<br/><span style="color:${color}">● ${SOURCE_LABELS[source]}</span>`;
          html += `<br/>&nbsp;&nbsp;<span style="color:${color}">UA: <strong>${ukr.toLocaleString()}</strong></span>`;
          html += `<br/>&nbsp;&nbsp;<span style="color:#dc2626">RU: <strong>${rus.toLocaleString()}</strong></span>`;
          html += `<br/>&nbsp;&nbsp;<span style="color:#6b7280">Adoption: ${adopt}%</span>`;
        } else {
          const value = Math.round((series[lo].adoption + frac * (series[hi].adoption - series[lo].adoption)) * 10) / 10;
          html += `<br/><span style="color:${color}">● ${SOURCE_LABELS[source]}: <strong>${value}%</strong></span>`;
        }

        tooltip.style('opacity', '1').html(html);
        const cr = container.getBoundingClientRect();
        const left = event.clientX - cr.left;
        const top = event.clientY - cr.top;
        tooltip.style('left', (left > width / 2 ? left - 200 : left + 20) + 'px')
          .style('top', Math.max(0, top - 40) + 'px');
      })
      .on('mouseleave', () => { crosshair.style('opacity', 0); tooltip.style('opacity', '0'); });
  }

  onMount(() => { if (container) draw($activeSource, $chartMode, $lineMode, $showEvents); });
</script>

<div class="chart-wrap" style="position:relative;">
  <div class="controls">
    <button class:active={$chartMode === 'volume'} on:click={() => chartMode.set('volume')}>Volume</button>
    <button class:active={$chartMode === 'adoption'} on:click={() => chartMode.set('adoption')}>Adoption %</button>
    <span class="sep"></span>
    <button class:active={$lineMode === 'both'} on:click={() => lineMode.set('both')}>All</button>
    <button class:active={$lineMode === 'ukrainian'} on:click={() => lineMode.set('ukrainian')}>UA</button>
    <button class:active={$lineMode === 'russian'} on:click={() => lineMode.set('russian')}>RU</button>
    <span class="sep"></span>
    <button class:active={$showEvents} on:click={() => showEvents.update(v => !v)}>Events</button>
  </div>
  <div bind:this={container} class="chart" style="width:100%;height:350px;position:relative;"></div>
  <div bind:this={legendContainer} class="legend"></div>
</div>

<style>
  .controls { display: flex; gap: 0.25rem; margin-bottom: 0.5rem; flex-wrap: wrap; }
  .controls button {
    padding: 0.25rem 0.6rem; border: 1px solid #e5e7eb; border-radius: 4px;
    background: white; font-size: 0.72rem; color: #6b7280; cursor: pointer;
  }
  .controls button.active { border-color: #0057B8; color: #0057B8; font-weight: 600; }
  .sep { width: 1px; background: #e5e7eb; margin: 0 0.15rem; }
  .chart { border: 1px solid #f3f4f6; border-radius: 8px; overflow: hidden; }
  .legend { display: flex; flex-wrap: wrap; gap: 0.75rem; margin-top: 0.5rem; }
</style>
