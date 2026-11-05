"""Which en.wikipedia title measures which spelling, declared once per pair.

The variant is written NEXT TO the title, never inferred from where a title sits
in a list. The previous collector took list index 0 as Russian and listed
FC_Dynamo_Kyiv first, so dynamo-kyiv shipped inverted: FC_Dynamo_Kyiv
(3,936,720 views, 2015-07..2025-12) labelled Russian and FC_Dynamo_Kiev
(18,173) labelled Ukrainian, and the site drew 0.2-0.4% Ukrainian where the
pageviews say 99.5%. The same rule labelled the city article 'Chernobyl'
(7,041,000 views) Ukrainian in pair_10_monthly.csv. check() refuses any title
that carries the OTHER spelling's distinctive word, which catches both.

Titles are spelled exactly as dataset/raw_wikipedia.parquet stores them in
page_title, spaces, underscores and the %27 in Kievan_Rus%27 included: the
holdout table groups and links by that string, so writing 'Babi_Yar' beside a
stored 'Babi Yar' would split one page into two rows.

Redirect titles are collected like articles. A redirect's own pageviews are the
visits that arrived through that spelling, which is what 'Kiev' has measured
since the 2020 move. Status measured 2026-10-19 with one prop=info call:

  redirect  Kiev, Kharkov, Odessa, Lvov, Lugansk, Ternopol, Vareniki,
            Artemovsk, FC Dynamo Kiev, Chicken Kyiv, Chornobyl disaster,
            Dnipro (river), Feodosiia, Kazymyr Malevych, Kyivan Rus',
            Mykola Hohol, Volodymyr the Great, Serhii Korolov
  missing   Ihor Sikorsky
  article   the other 24

'Serhii Korolov' is the pairs.yaml Ukrainian term (corrected from Korolyov in
81e4d270); its redirect has pageviews from 2022-10 only, 391 views to 2026-09.
'Ihor Sikorsky' does not exist, so ihor-sikorsky has no Ukrainian title to
measure. The collector records that as status 'missing' rather than as zeros:
pivoting with fill_value=0 turned the absence into 126 months of 0.0% adoption.

Five pairs resolve both spellings to ONE article (babyn-yar, donbas,
oleksandr-usyk, volodymyr-zelenskyy, zaporizhzhia). They are kept as stored,
with the same title under both variants, and marked shared: the pageviews are
identical by construction, say nothing about either spelling, and the exporter
drops them.
"""

import re
import urllib.parse

VARIANTS = ("russian", "ukrainian")

TITLES: dict[str, dict[str, str]] = {
    "kyiv":                {"russian": "Kiev",               "ukrainian": "Kyiv"},
    "kharkiv":             {"russian": "Kharkov",            "ukrainian": "Kharkiv"},
    "odesa":               {"russian": "Odessa",             "ukrainian": "Odesa"},
    "lviv":                {"russian": "Lvov",               "ukrainian": "Lviv"},
    "zaporizhzhia":        {"russian": "Zaporizhzhia",       "ukrainian": "Zaporizhzhia"},
    "chornobyl":           {"russian": "Chernobyl_disaster", "ukrainian": "Chornobyl_disaster"},
    "luhansk":             {"russian": "Lugansk",            "ukrainian": "Luhansk"},
    "dnipro-river":        {"russian": "Dnieper",            "ukrainian": "Dnipro (river)"},
    "donbas":              {"russian": "Donbas",             "ukrainian": "Donbas"},
    "chicken-kyiv":        {"russian": "Chicken_Kiev",       "ukrainian": "Chicken_Kyiv"},
    "dynamo-kyiv":         {"russian": "FC_Dynamo_Kiev",     "ukrainian": "FC_Dynamo_Kyiv"},
    "kyivan-rus":          {"russian": "Kievan_Rus%27",      "ukrainian": "Kyivan_Rus%27"},
    "ternopil":            {"russian": "Ternopol",           "ukrainian": "Ternopil"},
    "varenyky":            {"russian": "Vareniki",           "ukrainian": "Varenyky"},
    "babyn-yar":           {"russian": "Babi Yar",           "ukrainian": "Babi Yar"},
    "oleksandr-usyk":      {"russian": "Oleksandr Usyk",     "ukrainian": "Oleksandr Usyk"},
    "volodymyr-zelenskyy": {"russian": "Volodymyr Zelenskyy", "ukrainian": "Volodymyr Zelenskyy"},
    "volodymyr-the-great": {"russian": "Vladimir the Great", "ukrainian": "Volodymyr the Great"},
    "bakhmut":             {"russian": "Artemovsk",          "ukrainian": "Bakhmut"},
    "mykola-hohol":        {"russian": "Nikolai_Gogol",      "ukrainian": "Mykola_Hohol"},
    "feodosiia":           {"russian": "Feodosia",           "ukrainian": "Feodosiia"},
    "ihor-sikorsky":       {"russian": "Igor_Sikorsky",      "ukrainian": "Ihor_Sikorsky"},
    "serhii-korolyov":     {"russian": "Sergei_Korolev",     "ukrainian": "Serhii_Korolov"},
    "kazymyr-malevych":    {"russian": "Kazimir_Malevich",   "ukrainian": "Kazymyr_Malevych"},
}


def api_title(title: str) -> str:
    """Stored title -> the path segment the pageviews API expects."""
    return urllib.parse.quote(urllib.parse.unquote(title).replace(" ", "_"), safe="")


def display_title(title: str) -> str:
    """Stored title -> the form MediaWiki reports ('Kievan_Rus%27' -> "Kievan Rus'")."""
    return urllib.parse.unquote(title).replace("_", " ")


def same_page(a: str, b: str) -> bool:
    return display_title(a) == display_title(b)


def is_shared(slug: str) -> bool:
    t = TITLES[slug]
    return same_page(t["russian"], t["ukrainian"])


def _tokens(s: str) -> set[str]:
    return set(re.findall(r"[a-z0-9]+", display_title(s).lower()))


def check(pairs: list[dict]) -> list[str]:
    """Problems with the map against config/pairs.yaml; empty means usable.

    `pairs` is the enabled pair list. A title may not contain a word that only
    the OTHER spelling has. That is deliberately weaker than requiring its own
    word ('Feodosia' is the article for 'Feodosiya'), and strong enough to
    refuse both inversions that shipped.
    """
    problems = []
    by_slug = {p["slug"]: p for p in pairs}
    for slug in sorted(set(by_slug) - set(TITLES)):
        problems.append(f"{slug}: enabled but has no Wikipedia titles declared")
    for slug, t in TITLES.items():
        p = by_slug.get(slug)
        if p is None:
            continue                       # disabled pair: its stored rows stay untouched
        if set(t) != set(VARIANTS):
            problems.append(f"{slug}: needs exactly {VARIANTS}, has {sorted(t)}")
            continue
        if is_shared(slug):
            continue
        ru_only = _tokens(p["russian"]) - _tokens(p["ukrainian"])
        ua_only = _tokens(p["ukrainian"]) - _tokens(p["russian"])
        for variant, other_only in (("russian", ua_only), ("ukrainian", ru_only)):
            hit = _tokens(t[variant]) & other_only
            if hit:
                problems.append(f"{slug}: {variant} title {t[variant]!r} carries the other "
                                f"spelling's word {sorted(hit)}")
    return problems
