// Every window label on the site is generated here from manifest.study_window,
// which the exporter writes from pipeline/config.py. Nothing else may spell a
// study year: the window used to be hard-coded in six site files, so moving it
// to 2026 left "2010-2025", "Adoption in 2025" and an axis pinned to 2025-12-31
// in places no test looked at. A partial year is always named as one: the
// series runs to a closed month, and "2010-2026" would claim a complete 2026.

export type StudyWindow = {
  start: string;          // 'YYYY-MM', first plotted month
  end: string;            // 'YYYY-MM', last closed month on the axis
  text_end: string;       // 'YYYY-MM', last month keyness/collocations/clusters read
  headline_year: number;  // the complete year headline numbers and holdouts describe
  sources: Record<string, string>;   // per-source last plotted month
};

const MON: Record<string, string[]> = {
  en: ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'],
  // Ukrainian needs case agreement, so each phrase picks its own form:
  // nominative for a bare range ("січень–вересень 2026"), genitive after
  // "до" ("до вересня 2026 року"), and the dictionary abbreviations for an
  // axis label where space is short.
  uk_nom: ['січень', 'лютий', 'березень', 'квітень', 'травень', 'червень',
           'липень', 'серпень', 'вересень', 'жовтень', 'листопад', 'грудень'],
  uk_gen: ['січня', 'лютого', 'березня', 'квітня', 'травня', 'червня',
           'липня', 'серпня', 'вересня', 'жовтня', 'листопада', 'грудня'],
  uk_abbr: ['січ.', 'лют.', 'бер.', 'квіт.', 'трав.', 'черв.',
            'лип.', 'серп.', 'вер.', 'жовт.', 'лист.', 'груд.'],
};

const yr = (ym: string) => +String(ym).slice(0, 4);
const mo = (ym: string) => +String(ym).slice(5, 7);
const fullYear = (ym: string) => mo(ym) === 12;

/** Last day of a 'YYYY-MM' month as a local Date (the chart's axis end). */
export function monthEnd(ym: string): Date {
  return new Date(yr(ym), mo(ym), 0);
}

/** First day of a 'YYYY-MM' month as a local Date. */
export function monthStart(ym: string): Date {
  return new Date(yr(ym), mo(ym) - 1, 1);
}

/** The window as a span: "2010 – Sep 2026" | "з 2010 до вересня 2026 року".
 *  A December end prints the year alone ("2010–2025" | "з 2010 до 2025 року"). */
export function spanLabel(lang: string, start: string, end: string): string {
  if (lang === 'uk') {
    return fullYear(end)
      ? `з ${yr(start)} до ${yr(end)} року`
      : `з ${yr(start)} до ${MON.uk_gen[mo(end) - 1]} ${yr(end)} року`;
  }
  return fullYear(end) ? `${yr(start)}–${yr(end)}` : `${yr(start)} – ${MON.en[mo(end) - 1]} ${yr(end)}`;
}

/** One end point for an axis label: "2025" | "Sep 2026" | "вер. 2026". */
export function pointLabel(lang: string, ym: string): string {
  if (fullYear(ym)) return String(yr(ym));
  return lang === 'uk' ? `${MON.uk_abbr[mo(ym) - 1]} ${yr(ym)}` : `${MON.en[mo(ym) - 1]} ${yr(ym)}`;
}

/** The covered months of a final year: "Jan–Sep 2026" | "січень–вересень 2026". */
export function monthsOfYear(lang: string, ym: string): string {
  const m = mo(ym), y = yr(ym);
  const names = lang === 'uk' ? MON.uk_nom : MON.en;
  return m === 1 ? `${names[0]} ${y}` : `${names[0]}–${names[m - 1]} ${y}`;
}

/** Whether the axis ends inside a year (the final year is partial). */
export function partialYear(sw: StudyWindow): boolean {
  return !fullYear(sw.end);
}

/** The text-analysis window as a parenthetical: "texts from 2010–2025" |
 *  "тексти за 2010–2025 рр.". Chips and clusters are frozen at TEXT_END while
 *  the charts run on; without saying so a reader assumes chips include 2026. */
export function textsLabel(lang: string, sw: StudyWindow): string {
  const s = yr(sw.start), e = sw.text_end;
  if (lang === 'uk') {
    return fullYear(e)
      ? `тексти за ${s}–${yr(e)} рр.`
      : `тексти з ${s} до ${MON.uk_gen[mo(e) - 1]} ${yr(e)} р.`;
  }
  return fullYear(e) ? `texts from ${s}–${yr(e)}` : `texts from ${s} – ${MON.en[mo(e) - 1]} ${yr(e)}`;
}

/** Last month that every source with headline-year data for a pair covers.
 *  Cross-source aggregates (the home sparkline, its switch-year label) stop
 *  here: with GDELT alone extended into 2026, the quarterly mean became a
 *  GDELT-only number and gave four home cards a false '2026' switch label
 *  (odesa, dnipro-river, kyivan-rus, babyn-yar; measured 2026-10-15).
 *  A source's cover is the earlier of its window end and its own last point
 *  for this pair: YouTube's 2026 arrives pair by pair as the census runs, so a
 *  window end of 2026-09 does not mean this pair's YouTube reaches it. */
const YEARLY = new Set(['openalex', 'ngrams']);

export function aggregateEnd(sw: StudyWindow, pairSeries: Record<string, any>): string {
  const hy = String(sw.headline_year);
  let end = sw.end;
  for (const [src, rows] of Object.entries(pairSeries || {})) {
    if (!Array.isArray(rows)) continue;
    if (!rows.some((r: any) => r?.date?.slice(0, 4) === hy && !r.provisional)) continue;
    let e = sw.sources[src] || sw.end;
    // A yearly point (OpenAlex, Ngrams) is dated YYYY-01 and stands for its year.
    let last = '';
    for (const r of rows) {
      if (!r?.date || r.provisional) continue;
      const ym = YEARLY.has(src) ? `${String(r.date).slice(0, 4)}-12` : String(r.date).slice(0, 7);
      if (ym > last) last = ym;
    }
    if (last && last < e) e = last;
    if (e < end) end = e;
  }
  return end;
}

/** manifest.study_window, or the window an export from before it existed
 *  implies. Such an export carries only time_span ('2010-2025') and was
 *  clamped to whole years, so the window is those years with no partial one.
 *  The committed site data predates study_window until the next export is
 *  committed, and the build must not break in between. Anything else is an
 *  error, never a guessed year. */
export function studyWindow(manifest: any): StudyWindow {
  const sw = manifest?.study_window;
  if (sw && sw.start && sw.end) return sw as StudyWindow;
  const mm = /^(\d{4})\s*[-\u2013]\s*(\d{4})$/.exec(String(manifest?.time_span || ''));
  if (!mm) throw new Error('manifest.json has no study_window: re-run python -m pipeline.export_site_data');
  return { start: `${mm[1]}-01`, end: `${mm[2]}-12`, text_end: `${mm[2]}-12`,
           headline_year: +mm[2], sources: {} };
}
