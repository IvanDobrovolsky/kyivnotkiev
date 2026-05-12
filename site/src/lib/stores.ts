import { writable, derived } from 'svelte/store';

// All data imports — single source of truth
import timeseriesData from '../data/timeseries.json';
import manifestData from '../data/manifest.json';
import holdoutsByPairData from '../data/holdouts_by_pair.json';
import holdoutsGlobalData from '../data/holdouts.json';
import countriesByPairData from '../data/countries_by_pair.json';
import pairEventsData from '../data/pair_events.json';
import domainOriginsData from '../data/domain_origins.json';
import llmPerPairData from '../data/llm_per_pair.json';
import llmTrajectoryData from '../data/llm_trajectory.json';
import religiousData from '../data/religious.json';
import toolsPerPairData from '../data/tools_per_pair.json';
import clCollocationsData from '../data/cl_collocations.json';
import dictionaryData from '../data/dictionary_combined.json';
import errorAnalysisData from '../data/error_analysis.json';
import enforcementData from '../data/enforcement.json';
import confusionMatrixData from '../data/confusion_matrix.json';
import clAnalysisData from '../data/cl_analysis.json';
import analysisData from '../data/analysis.json';
import regexPrecisionData from '../data/regex_precision.json';

// Export all data as plain objects (no store needed — they don't change)
export const data = {
  timeseries: timeseriesData as any,
  manifest: manifestData as any,
  holdoutsByPair: holdoutsByPairData as any,
  holdoutsGlobal: holdoutsGlobalData as any,
  countriesByPair: countriesByPairData as any,
  pairEvents: pairEventsData as any,
  domainOrigins: domainOriginsData as any,
  llmPerPair: llmPerPairData as any,
  llmTrajectory: llmTrajectoryData as any,
  religious: religiousData as any,
  toolsPerPair: toolsPerPairData as any,
  clCollocations: clCollocationsData as any,
  dictionary: dictionaryData as any,
  errorAnalysis: errorAnalysisData as any,
  enforcement: enforcementData as any,
  confusionMatrix: confusionMatrixData as any,
  clAnalysis: clAnalysisData as any,
  analysis: analysisData as any,
  regexPrecision: regexPrecisionData as any,
};

// Valid sources
export const SOURCES = ['gdelt', 'trends', 'wikipedia', 'reddit', 'youtube', 'ngrams', 'openalex', 'telegram', 'religious'] as const;
export type Source = typeof SOURCES[number];

export const SOURCE_LABELS: Record<Source, string> = {
  gdelt: 'News', trends: 'Trends', wikipedia: 'Wiki', reddit: 'Reddit',
  youtube: 'YouTube', ngrams: 'Books', openalex: 'Academic',
  telegram: 'Telegram', religious: 'Religious',
};

export const SOURCE_COLORS: Record<Source, string> = {
  gdelt: '#1e3a5f', trends: '#4285F4', wikipedia: '#636466', reddit: '#FF4500',
  youtube: '#FF0000', ngrams: '#7c3aed', openalex: '#06b6d4',
  telegram: '#26A5E4', religious: '#8B0000',
};

// Reactive state
export const activeSource = writable<Source>(getSourceFromUrl());
export const chartMode = writable<'adoption' | 'volume'>('volume');
export const lineMode = writable<'both' | 'ukrainian' | 'russian'>('both');
export const showEvents = writable(true);
export const holdoutFilter = writable<'all' | 'intl' | 'ru' | 'ua'>('intl');

// Read source from URL on init
function getSourceFromUrl(): Source {
  if (typeof window === 'undefined') return 'gdelt';
  const param = new URLSearchParams(window.location.search).get('source');
  if (param && SOURCES.includes(param as Source)) return param as Source;
  return 'gdelt';
}

// Sync source to URL
activeSource.subscribe(src => {
  if (typeof window === 'undefined') return;
  const url = new URL(window.location.href);
  if (src === 'gdelt') {
    url.searchParams.delete('source');
  } else {
    url.searchParams.set('source', src);
  }
  window.history.replaceState({}, '', url.toString());
});

// Derived: current pair data
export function getPairData(slug: string) {
  return {
    timeseries: data.timeseries[slug] || {},
    holdouts: data.holdoutsByPair[slug] || {},
    countries: data.countriesByPair[slug] || {},
    events: data.pairEvents[slug] || [],
    domainOrigins: data.domainOrigins[slug] || {},
    llm: data.llmPerPair?.pairs?.[slug] || null,
    collocations: data.clCollocations[slug] || null,
    dictionary: data.dictionary[slug] || null,
    pair: data.manifest.pairs.find((p: any) => p.slug === slug),
  };
}
