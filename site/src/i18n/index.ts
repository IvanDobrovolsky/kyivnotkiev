import en from './en.json';
import uk from './uk.json';

const locales: Record<string, any> = { en, uk };

/** Dot-path lookup with EN fallback; {var} interpolation via vars. */
export function t(lang: string, path: string, vars?: Record<string, string | number>): string {
  const get = (dict: any) => path.split('.').reduce((o, k) => (o ? o[k] : undefined), dict);
  let s = get(locales[lang]) ?? get(en) ?? path;
  if (typeof s !== 'string') return path;
  if (vars) for (const [k, v] of Object.entries(vars)) s = s.replaceAll(`{${k}}`, String(v));
  return s;
}

/** The same path in the other locale, preserving the route. */
export function altPath(lang: string, pathname: string): string {
  if (lang === 'uk') return pathname.replace(/^\/uk\/?/, '/') || '/';
  return pathname === '/' ? '/uk/' : `/uk${pathname}`;
}

/** Serializable dict for client scripts (window.__T). */
export function clientDict(lang: string): any {
  return locales[lang] ?? en;
}
