#!/usr/bin/env python3
"""Generate full audit HTML reports for all 59 toponym pairs.

Reads from site/src/data/*.json and produces site/public/report/pair-{id}.html

Usage:
    python3 pipeline/generate_reports.py [--pair 1]  # single pair
    python3 pipeline/generate_reports.py              # all pairs
"""

import argparse
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
DATA = ROOT / "site" / "src" / "data"
OUT = ROOT / "site" / "public" / "report"
OUT.mkdir(parents=True, exist_ok=True)


def load_json(name):
    with open(DATA / name) as f:
        return json.load(f)


# Hand-crafted analysis texts (loaded at runtime)
HAND_ANALYSIS = None


SOURCE_COLORS = {
    "trends": "#f59e0b", "gdelt": "#ef4444", "wikipedia": "#10b981",
    "reddit": "#f97316", "youtube": "#dc2626", "ngrams": "#8b5cf6",
    "openalex": "#10b981", "openlibrary": "#6366f1",
}
SOURCE_NAMES = {
    "trends": "Google Trends", "gdelt": "GDELT News", "wikipedia": "Wikipedia",
    "reddit": "Reddit", "youtube": "YouTube", "ngrams": "Google Ngrams",
    "openalex": "OpenAlex Papers", "openlibrary": "Open Library Books",
}
SOURCE_URLS = {
    "trends": "https://trends.google.com/trends/", "gdelt": "https://blog.gdeltproject.org/",
    "wikipedia": "https://en.wikipedia.org/wiki/", "reddit": "https://arctic-shift.photon-reddit.com/",
    "youtube": "https://www.youtube.com/", "ngrams": "https://books.google.com/ngrams/",
    "openalex": "https://openalex.org/", "openlibrary": "https://openlibrary.org/",
}
EVENTS = [
    (2018 + 10 / 12, "#KyivNotKiev"),
    (2019 + 8 / 12, "AP Stylebook"),
    (2022 + 2 / 12, "Full-scale war"),
]
FAMILY_COLORS = {
    "Anthropic Claude": "#d97706", "OpenAI GPT": "#10b981", "Google Gemini": "#3b82f6",
    "Google Gemma": "#6366f1", "xAI Grok": "#ef4444", "Meta Llama": "#8b5cf6",
    "Mistral": "#f97316", "Alibaba Qwen": "#0d9488",
}


def fmt_number(n):
    if n >= 1_000_000:
        return f"{n / 1_000_000:.1f}M"
    if n >= 1_000:
        return f"{n / 1_000:.1f}K"
    return str(int(n))


def make_svg_chart(data_points, source_color, start_year, end_year, pair_events=None, spike_annotations=None):
    if not data_points:
        return '<div style="color:#9ca3af;font-size:0.8rem;padding:2rem;text-align:center;">No data available</div>'

    W = 1000
    top_h = 118
    sep_y = 126
    bar_bot = 248
    span = max(end_year - start_year, 1)
    n = len(data_points)

    positions = []
    for i, dp in enumerate(data_points):
        x = i / max(n - 1, 1) * W
        adoption = dp.get("adoption", 0) or 0
        y = top_h - (adoption / 100 * (top_h - 15))
        y = max(15, min(top_h, y))
        positions.append((x, y, dp))

    poly_pts = " ".join(f"{x:.1f},{y:.1f}" for x, y, _ in positions)
    line_pts = poly_pts

    max_vol = max(((dp.get("rus", 0) or 0) + (dp.get("ukr", 0) or 0)) for _, _, dp in positions) or 1
    bar_h_max = bar_bot - 130
    bar_w = max(W / n * 0.35, 1.5)
    bar_gap = max(bar_w * 0.1, 0.5)

    bars = []
    for x, _, dp in positions:
        rus = dp.get("rus", 0) or 0
        ukr = dp.get("ukr", 0) or 0
        rus_h = max(rus / max_vol * bar_h_max, 0.5) if rus else 0.5
        ukr_h = max(ukr / max_vol * bar_h_max, 0.5) if ukr else 0.5
        rx = max(0, x - bar_w - bar_gap / 2)
        ux = x + bar_gap / 2
        bars.append(
            f'<rect x="{rx:.1f}" y="{bar_bot - rus_h:.1f}" width="{bar_w:.1f}" height="{rus_h:.1f}" fill="#dc2626" opacity="0.5" rx="1"/>'
            f'<rect x="{ux:.1f}" y="{bar_bot - ukr_h:.1f}" width="{bar_w:.1f}" height="{ukr_h:.1f}" fill="#0057B8" opacity="0.7" rx="1"/>'
        )

    # Event markers — global + pair-specific from pair_events.json
    all_events = list(EVENTS)
    if pair_events:
        for ev in pair_events:
            date_str = ev.get("date", "")
            parts = date_str.split("-")
            if len(parts) >= 2:
                ev_year = int(parts[0]) + int(parts[1]) / 12
                all_events.append((ev_year, ev.get("label", "")))

    ev_svg = []
    for vi, (ev_year, ev_label) in enumerate(all_events):
        if start_year <= ev_year <= end_year:
            ex = (ev_year - start_year) / span * W
            yt = -32 + vi * 14
            if yt > 10:
                continue  # too many, skip
            anc = ' text-anchor="end"' if ex > W * 0.95 else ""
            tx = ex - 4 if ex > W * 0.95 else ex + 4
            color = "#d97706" if vi >= len(EVENTS) else "#9ca3af"
            ev_svg.append(
                f'<line x1="{ex:.1f}" y1="0" x2="{ex:.1f}" y2="248" stroke="{color}" stroke-width="0.8" stroke-dasharray="4,4" opacity="0.5"/>'
                f'<text x="{tx:.1f}" y="{yt}" font-size="10" fill="{color}" font-weight="600"{anc}>{ev_label}</text>'
            )

    # X-axis year labels
    x_axis = [f'<line x1="0" y1="248" x2="{W}" y2="248" stroke="#e5e7eb" stroke-width="0.5"/>']
    step = 2 if span <= 12 else (5 if span <= 25 else (10 if span <= 60 else 20))
    first_yr = start_year + (step - start_year % step) if start_year % step else start_year
    yr = first_yr
    while yr <= end_year:
        lx = (yr - start_year) / span * W
        x_axis.append(f'<text x="{lx:.0f}" y="268" font-size="10" fill="#9ca3af" text-anchor="middle">{yr}</text>'
                       f'<line x1="{lx:.0f}" y1="248" x2="{lx:.0f}" y2="254" stroke="#d1d5db" stroke-width="0.8"/>')
        yr += step

    # Y-axis labels for adoption %
    max_adoption = max((dp.get("adoption", 0) or 0) for _, _, dp in positions) or 100
    # Round up to nice number
    if max_adoption > 80: y_max = 100
    elif max_adoption > 60: y_max = 80
    elif max_adoption > 40: y_max = 60
    elif max_adoption > 20: y_max = 40
    else: y_max = max(20, int((max_adoption // 10 + 1) * 10))

    y_axis = []
    for pct in range(0, y_max + 1, 20 if y_max > 40 else 10):
        y_pos = top_h - (pct / 100 * (top_h - 15))
        y_axis.append(f'<text x="-8" y="{y_pos + 3:.0f}" font-size="9" fill="#9ca3af" text-anchor="end">{pct}%</text>')
        if pct > 0:
            y_axis.append(f'<line x1="0" y1="{y_pos:.0f}" x2="{W}" y2="{y_pos:.0f}" stroke="#f3f4f6" stroke-width="0.5"/>')

    # Spike annotations — arrows pointing down to data with callout labels
    anno_svg = []
    if spike_annotations:
        for ai, ann in enumerate(spike_annotations[:3]):  # max 3 per chart
            date_str = ann.get("date", "")
            label = ann.get("label", "")
            parts = date_str.split("-")
            if len(parts) < 2:
                continue
            ann_year = int(parts[0]) + int(parts[1]) / 12
            if not (start_year <= ann_year <= end_year):
                continue
            ax = (ann_year - start_year) / span * W
            # Find nearest data point y
            nearest_y = top_h
            for px, py, dp in positions:
                if abs(px - ax) < W / n * 1.5:
                    nearest_y = min(nearest_y, py)
            # Arrow from above down to the data point
            arrow_top = nearest_y - 28 - ai * 16
            arrow_top = max(-45, arrow_top)
            anc = ' text-anchor="end"' if ax > W * 0.8 else (' text-anchor="start"' if ax < W * 0.2 else ' text-anchor="middle"')
            tx = ax
            anno_svg.append(
                f'<line x1="{ax:.0f}" y1="{arrow_top + 12}" x2="{ax:.0f}" y2="{nearest_y - 3}" stroke="#d97706" stroke-width="1.2" marker-end="url(#arrowhead)"/>'
                f'<text x="{tx:.0f}" y="{arrow_top + 8}" font-size="8.5" fill="#d97706" font-weight="600"{anc}>{label}</text>'
            )

    # Arrow marker definition
    arrow_def = '<defs><marker id="arrowhead" markerWidth="6" markerHeight="4" refX="5" refY="2" orient="auto"><polygon points="0 0, 6 2, 0 4" fill="#d97706"/></marker></defs>' if anno_svg else ''

    return f'''<svg viewBox="-40 -50 1075 331" style="display:block;width:100%;height:auto;">
      {arrow_def}
      {"".join(y_axis)}
      {"".join(ev_svg)}
      <polygon points="{poly_pts} {W},{top_h} 0,{top_h}" fill="{source_color}" opacity="0.06"/>
      <polyline points="{line_pts}" fill="none" stroke="{source_color}" stroke-width="2.5"/>
      <line x1="0" y1="{sep_y}" x2="{W}" y2="{sep_y}" stroke="#e5e7eb" stroke-width=".5"/>
      {"".join(bars)}
      {"".join(x_axis)}
      {"".join(anno_svg)}
    </svg>'''


def make_collocation_cloud(collocates, color, label, strikethrough=False):
    if not collocates:
        return ""
    top = collocates[:10]
    mx = max(c.get("pmi", 1) for c in top) or 1
    spans = []
    for c in top:
        sz = 1.1 + (c.get("pmi", 0) / mx) * 0.3
        wt = "700" if c.get("pmi", 0) > mx * 0.7 else "400"
        spans.append(f'<span style="font-size:{sz:.2f}rem;color:{color};font-weight:{wt};margin-right:0.4rem;">{c["word"]}</span> ')
    lbl = f'<span style="text-decoration:line-through;opacity:0.6;">{label}</span> form' if strikethrough else f'{label} form'
    return f'''<div style="margin-bottom:0.8rem;">
    <div style="font-size:0.65rem;color:{color};font-weight:700;text-transform:uppercase;margin-bottom:0.25rem;letter-spacing:0.04em;">{lbl}</div>
    <div style="line-height:2;">{"".join(spans)}</div>
  </div>'''


def generate_analysis_text(pair, ts_data, llm_summary, coll_data, religious_data, stat, ctx_dist):
    """Generate rich data-driven analysis with spike detection, collocation insights, and recommendations."""
    ru, ua = pair["russian"], pair["ukrainian"]
    pid = pair["id"]
    adoption = pair.get("adoption", 0) or 0
    total = pair.get("total", 0) or 0
    tas = llm_summary.get("tas_mean", 0) or 0
    recall = llm_summary.get("open_pct", 0) or 0
    forced = ((llm_summary.get("forced_ru_pct", 0) or 0) + (llm_summary.get("forced_ua_pct", 0) or 0)) / 2

    paragraphs = []

    # --- P1: Headline + source hierarchy ---
    src_rankings = []
    for src in ["wikipedia", "openalex", "gdelt", "trends", "reddit", "youtube", "openlibrary", "ngrams"]:
        sd = ts_data.get(src, [])
        if not sd:
            continue
        last = sd[-min(12, len(sd)):]
        avg = sum(d.get("adoption", 0) or 0 for d in last) / len(last) if last else 0
        total_src = sum((d.get("rus", 0) or 0) + (d.get("ukr", 0) or 0) for d in sd)
        src_rankings.append((SOURCE_NAMES.get(src, src), avg, total_src, src))
    src_rankings.sort(key=lambda x: -x[1])

    ci_lo, ci_hi = stat.get("lo", 0), stat.get("hi", 0)
    ci_str = f" (95% CI: [{ci_lo:.1f}%, {ci_hi:.1f}%])" if ci_lo and ci_hi else ""

    leader = src_rankings[0] if src_rankings else None
    laggard = src_rankings[-1] if len(src_rankings) > 1 else None
    src_detail = ""
    if leader and laggard:
        src_detail = f" {leader[0]} leads at {leader[1]:.0f}%, while {laggard[0]} lags at {laggard[1]:.0f}%."

    paragraphs.append(
        f'<p style="margin-bottom:0.7rem;"><strong>{adoption:.1f}% adoption across {fmt_number(total)} mentions</strong>{ci_str}.{src_detail}</p>'
    )

    # --- P2: Spike detection — find biggest volume jumps in YouTube/Reddit ---
    spike_insights = []
    for src_key, src_name in [("youtube", "YouTube"), ("reddit", "Reddit")]:
        sd = ts_data.get(src_key, [])
        if len(sd) < 3:
            continue
        for i in range(1, len(sd)):
            prev_total = (sd[i-1].get("rus", 0) or 0) + (sd[i-1].get("ukr", 0) or 0)
            curr_total = (sd[i].get("rus", 0) or 0) + (sd[i].get("ukr", 0) or 0)
            curr_adoption = sd[i].get("adoption", 0) or 0
            prev_adoption = sd[i-1].get("adoption", 0) or 0
            if prev_total > 5 and curr_total > prev_total * 2:
                year = sd[i].get("date", "")[:4]
                spike_insights.append((src_name, year, curr_total, prev_total, curr_adoption, curr_adoption - prev_adoption))
            elif curr_total > 50 and abs(curr_adoption - prev_adoption) > 15:
                year = sd[i].get("date", "")[:4]
                spike_insights.append((src_name, year, curr_total, prev_total, curr_adoption, curr_adoption - prev_adoption))

    if spike_insights:
        # Pick top 2 most interesting spikes
        spike_insights.sort(key=lambda x: abs(x[5]) * x[2], reverse=True)
        parts = []
        for src_name, year, curr, prev, adopt, delta in spike_insights[:2]:
            if curr > prev * 1.5:
                parts.append(f"{src_name} {year}: {curr:,} mentions ({curr/max(prev,1):.0f}x volume spike), {adopt:.0f}% adoption")
            else:
                direction = "+" if delta > 0 else ""
                parts.append(f"{src_name} {year}: {direction}{delta:.0f}pp adoption shift ({curr:,} mentions)")
        paragraphs.append(f'<p style="margin-bottom:0.7rem;"><strong>Volume spikes detected:</strong> {"; ".join(parts)}. These correlate with major cultural or geopolitical events that drive public discourse and spelling adoption.</p>')

    # --- P3: Collocation analysis (PMI-ranked) ---
    ru_coll = coll_data.get("russian", {}).get("collocates", []) if isinstance(coll_data.get("russian"), dict) else []
    ua_coll = coll_data.get("ukrainian", {}).get("collocates", []) if isinstance(coll_data.get("ukrainian"), dict) else []

    if ru_coll and ua_coll:
        # Find distinctive words (high PMI, not shared)
        ru_words = {c["word"] for c in ru_coll[:10]}
        ua_words = {c["word"] for c in ua_coll[:10]}
        ru_only = [c for c in ru_coll[:10] if c["word"] not in ua_words][:4]
        ua_only = [c for c in ua_coll[:10] if c["word"] not in ru_words][:4]
        shared = ru_words & ua_words

        parts = []
        if ru_only:
            words = ", ".join(f'<em>{c["word"]}</em>' for c in ru_only)
            parts.append(f'"{ru}" distinctively co-occurs with {words}')
        if ua_only:
            words = ", ".join(f'<em>{c["word"]}</em>' for c in ua_only)
            parts.append(f'"{ua}" clusters with {words}')
        if shared:
            parts.append(f'both forms share <em>{", ".join(list(shared)[:3])}</em>')

        paragraphs.append(f'<p style="margin-bottom:0.7rem;"><strong>Collocation analysis (PMI-ranked):</strong> {"; ".join(parts)}. These patterns reveal how each spelling form lives in distinct discourse communities.</p>')

    # --- P4: Context distribution ---
    ru_ctx = ctx_dist.get("russian", {})
    ua_ctx = ctx_dist.get("ukrainian", {})
    if ru_ctx and ua_ctx:
        ctx_items = []
        for cn in set(list(ru_ctx.keys()) + list(ua_ctx.keys())):
            rp = ru_ctx.get(cn, 0) or 0
            up = ua_ctx.get(cn, 0) or 0
            if rp + up > 0.04:  # same threshold as display bucketing
                ua_share = up / (rp + up) * 100
                ctx_items.append((cn.replace("_", " "), ua_share, rp, up))
        ctx_items.sort(key=lambda x: -x[1])
        if len(ctx_items) >= 2:
            top = ctx_items[0]
            bot = ctx_items[-1]
            paragraphs.append(f'<p style="margin-bottom:0.7rem;"><strong>Context matters:</strong> "{ua}" dominates in <em>{top[0]}</em> contexts ({top[1]:.0f}% UA share), while "{ru}" persists strongest in <em>{bot[0]}</em> ({100-bot[1]:.0f}% RU share). Domain-specific adoption varies by up to {top[1]-bot[1]:.0f} percentage points.</p>')

    # --- P5: LLM + Religious ---
    extra = []
    if tas > 0:
        gap = forced - recall
        extra.append(f'AI models show {tas:.0f}% alignment (TAS) with {recall:.0f}% spontaneous generation — a {abs(gap):.0f}pp recognition-recall gap indicating pre-training corpus inertia')

    mospat_added = False
    for denom in religious_data.get("denominations", []):
        if mospat_added:
            break
        for pp in denom.get("pairs", []):
            if pp.get("pair_id") == pid and pp.get("ru", 0) + pp.get("ua", 0) > 20:
                if pp.get("ua_pct", 50) < 20 and "Moscow" in denom.get("name", ""):
                    extra.append(f'Moscow Patriarchate maintains {100 - (pp["ua_pct"] or 0):.0f}% preference for "{ru}" — institutional resistance')
                    mospat_added = True
                    break

    if extra:
        paragraphs.append(f'<p style="margin-bottom:0.7rem;">{". ".join(extra)}.</p>')

    # --- P6: Recommendation ---
    rec_parts = []
    if adoption < 30:
        rec_parts.append(f'Adoption of "{ua}" remains very low ({adoption:.0f}%)')
        rec_parts.append("institutional endorsement and media style guide adoption would be the most effective levers")
    elif adoption < 60:
        rec_parts.append(f'Adoption is progressing but contested ({adoption:.0f}%)')
        if laggard:
            rec_parts.append(f'{laggard[0]} ({laggard[1]:.0f}%) represents the biggest opportunity for improvement')
    else:
        rec_parts.append(f'Adoption is strong ({adoption:.0f}%) with mainstream momentum')
        if any(s[1] < 40 for s in src_rankings):
            lagging = [s for s in src_rankings if s[1] < 40]
            rec_parts.append(f'remaining holdouts in {", ".join(s[0] for s in lagging[:2])} likely reflect legacy content rather than active resistance')

    if tas > 0 and recall < 90:
        rec_parts.append(f'LLM training data updates would increase AI alignment from current {recall:.0f}% free recall')

    if rec_parts:
        paragraphs.append(f'<p><strong>Recommendation:</strong> {". ".join(rec_parts)}.</p>')

    return "\n  ".join(paragraphs)


def _build_evidence_section(pid, ru, ua, article_examples):
    """Build Source Evidence section with actual article URLs."""
    if not article_examples:
        return ""
    pair_data = article_examples.get(str(pid))
    if not pair_data or not pair_data.get("examples"):
        return ""

    examples = pair_data["examples"]
    ru_examples = [e for e in examples if e["variant"] == "russian"]
    ua_examples = [e for e in examples if e["variant"] == "ukrainian"]

    def _row(ex):
        domain = ex.get("domain", "")
        url = ex.get("url", "#")
        origin = ex.get("origin", "")
        year = ex.get("year")
        year_str = f' ({year})' if year else ""
        origin_badge = ""
        if origin == "ukrainian":
            origin_badge = '<span style="background:#0057B8;color:white;padding:1px 6px;border-radius:3px;font-size:0.7rem;margin-left:4px;">.ua</span>'
        elif origin == "state_media":
            origin_badge = '<span style="background:#dc2626;color:white;padding:1px 6px;border-radius:3px;font-size:0.7rem;margin-left:4px;">state media</span>'
        return f'<li style="margin-bottom:4px;word-break:break-all;"><a href="{url}" target="_blank" rel="noopener" style="color:#3b82f6;text-decoration:none;font-size:0.85rem;">{domain}{year_str}</a>{origin_badge}</li>'

    html = '<h2 id="evidence">Source Evidence</h2>\n<div style="display:grid;grid-template-columns:1fr 1fr;gap:1.5rem;">\n'

    if ru_examples:
        html += f'<div><h3 style="color:#E74C3C;font-size:0.95rem;">Uses "{ru}" (Russian form)</h3>\n<ul style="list-style:none;padding:0;">\n'
        for ex in ru_examples[:12]:
            html += _row(ex) + "\n"
        html += "</ul></div>\n"

    if ua_examples:
        html += f'<div><h3 style="color:#0057B8;font-size:0.95rem;">Uses "{ua}" (Ukrainian form)</h3>\n<ul style="list-style:none;padding:0;">\n'
        for ex in ua_examples[:12]:
            html += _row(ex) + "\n"
        html += "</ul></div>\n"

    html += "</div>\n"
    return html


def generate_report(pair, timeseries, llm_data, collocations, religious_data, pair_events_data, stats, ctx_distributions, cm_svg, chart_annotations, all_pairs, article_examples=None):
    pid = pair["id"]
    ru, ua = pair["russian"], pair["ukrainian"]
    cat = pair["category"]
    adoption = pair.get("adoption", 0) or 0
    total = pair.get("total", 0) or 0
    starred = pair.get("starred", False)
    starred_label = pair.get("starred_label", "")

    llm = llm_data.get(str(pid), {})
    llm_summary = llm.get("summary", {})
    tas = llm_summary.get("tas_mean", 0) or 0
    forced = ((llm_summary.get("forced_ru_pct", 0) or 0) + (llm_summary.get("forced_ua_pct", 0) or 0)) / 2
    recall = llm_summary.get("open_pct", 0) or 0
    n_models = llm_summary.get("n_models", 72)
    by_family = llm_summary.get("by_family", {})

    stat = stats.get(str(pid), {})
    ci_point = stat.get("point", adoption)
    ci_lo = stat.get("lo", 0)
    ci_hi = stat.get("hi", 0)
    ci_n = stat.get("n", total)

    ts = timeseries.get(str(pid), {})
    coll = collocations.get(str(pid), {})

    # Subtitle — no "Pair #N"
    star_str = f" · {starred_label}" if starred else ""
    subtitle = f"{cat.title()}{star_str}"

    # ---- Analysis text ----
    ctx_dist_pair = ctx_distributions.get(str(pid), {})
    # Use hand-crafted analysis if available, else generate
    global HAND_ANALYSIS
    if HAND_ANALYSIS is None:
        try:
            HAND_ANALYSIS = load_json("pair_analysis.json")
        except Exception:
            HAND_ANALYSIS = {}
    analysis_text = HAND_ANALYSIS.get(str(pid), "")
    if not analysis_text:
        analysis_text = generate_analysis_text(pair, ts, llm_summary, coll, religious_data, stat, ctx_dist_pair)

    # ---- Source charts ----
    p_events = pair_events_data.get(str(pid), [])
    source_charts = []
    for src in ["trends", "gdelt", "wikipedia", "reddit", "youtube", "ngrams", "openalex", "openlibrary"]:
        sd = ts.get(src, [])
        if not sd:
            continue
        color = SOURCE_COLORS.get(src, "#6b7280")
        name = SOURCE_NAMES.get(src, src)
        url = SOURCE_URLS.get(src, "#")
        if src == "trends":
            url = f"https://trends.google.com/trends/explore?q={ru},{ua}"
        elif src == "wikipedia":
            url = f"https://en.wikipedia.org/wiki/{ua}"
        elif src == "ngrams":
            url = f"https://books.google.com/ngrams/graph?content={ru},{ua}&corpus=37"

        total_src = sum((d.get("rus", 0) or 0) + (d.get("ukr", 0) or 0) for d in sd)
        adoptions = [d.get("adoption", 0) or 0 for d in sd]
        alltime = sum(adoptions) / len(adoptions) if adoptions else 0
        last12 = sd[-12:] if len(sd) >= 12 else sd
        last12_avg = sum(d.get("adoption", 0) or 0 for d in last12) / len(last12) if last12 else 0

        dates = [d.get("date", "") for d in sd if d.get("date")]
        start_yr = int(dates[0][:4]) if dates else 2010
        end_yr = int(dates[-1][:4]) if dates else 2026

        pair_annos = chart_annotations.get(str(pid), {}).get(src, [])
        chart = make_svg_chart(sd, color, start_yr, end_yr, p_events, pair_annos)
        source_charts.append(f'''<div class="card" style="padding:1.25rem 1.75rem;">
  <div class="source-head">
    <div style="display:flex;align-items:center;gap:0.6rem;">
      <span style="display:inline-block;width:14px;height:14px;background:{color};border-radius:4px;"></span>
      <a href="{url}" target="_blank" style="font-weight:700;font-size:1.05rem;color:#1f2937;text-decoration:none;">{name} ↗</a>
    </div>
    <div class="source-meta">
      <span>Total: <strong>{fmt_number(total_src)}</strong></span>
      <span>All-time: <strong>{alltime:.1f}%</strong></span>
      <span>Last 12mo: <strong style="color:#0057B8;">{last12_avg:.1f}%</strong></span>
    </div>
  </div>
  <div style="display:flex;gap:1rem;font-size:0.7rem;color:#6b7280;margin-bottom:0.25rem;">
    <span>↑ Adoption % (more {ua})</span>
    <span>↓ <span style="color:#dc2626;">■</span> {ru} / <span style="color:#0057B8;">■</span> {ua} volume</span>
  </div>
  {chart}
</div>''')

    # ---- Context distribution (from cl_analysis.json) — smart bucketing ----
    ctx_dist = ctx_distributions.get(str(pid), {})
    ru_ctx = ctx_dist.get("russian", {})
    ua_ctx = ctx_dist.get("ukrainian", {})
    coll_total = coll.get("total", 0)
    coll_ru_n = coll.get("russian", {}).get("n_texts", 0) if isinstance(coll.get("russian"), dict) else 0
    coll_ua_n = coll.get("ukrainian", {}).get("n_texts", 0) if isinstance(coll.get("ukrainian"), dict) else 0
    coll_label = f'{coll_total} texts ({coll_ru_n} {ru} + {coll_ua_n} {ua})' if coll_ru_n else f'{coll_total} texts'
    ctx_rows = []

    CTX_LABELS = {
        "war_conflict": "War", "academic_science": "Academic", "politics": "Politics",
        "history": "History", "travel_tourism": "Travel", "sports": "Sports",
        "general_news": "News", "culture_arts": "Culture", "food_cuisine": "Food",
        "business_economy": "Business", "religion": "Religion",
    }

    if ru_ctx or ua_ctx:
        all_contexts = set(list(ru_ctx.keys()) + list(ua_ctx.keys()))
        ctx_items = [(cn, ru_ctx.get(cn, 0) or 0, ua_ctx.get(cn, 0) or 0) for cn in all_contexts]
        ctx_items.sort(key=lambda x: x[1] + x[2], reverse=True)

        # Smart bucketing: try 4%+ threshold, then 2%, then 1%, then all — max 10
        selected = []
        for threshold in [0.04, 0.02, 0.01, 0]:
            selected = [(cn, rp, up) for cn, rp, up in ctx_items if rp + up > threshold]
            if len(selected) >= 4:
                break
        selected = selected[:10]

        for cn, rp, up in selected:
            total_p = rp + up
            if total_p < 0.005:
                continue
            ru_pct = rp / total_p * 100
            ua_pct = up / total_p * 100
            share_pct = total_p / 2 * 100
            label = CTX_LABELS.get(cn, cn.replace("_", " ").title())
            ctx_rows.append(
                f'<div class="ctx-row"><div>{label}</div>'
                f'<div style="display:flex;gap:2px;">'
                f'<div style="height:10px;width:{ru_pct:.0f}%;background:#ef4444;opacity:0.6;border-radius:3px;"></div>'
                f'<div style="height:10px;width:{ua_pct:.0f}%;background:#3b82f6;opacity:0.6;border-radius:3px;"></div></div>'
                f'<div style="font-size:0.68rem;"><span style="color:#ef4444;">{rp*100:.0f}%</span>/<span style="color:#3b82f6;">{up*100:.0f}%</span></div>'
                f'<div style="font-size:0.68rem;color:#6b7280;">{share_pct:.0f}%</div></div>')

    ctx_html = ""
    if ctx_rows:
        ctx_html = f'''<div class="card">
  <div style="font-weight:700;font-size:0.95rem;margin-bottom:0.3rem;">Context Distribution</div>
  <p style="color:#6b7280;font-size:0.72rem;margin-bottom:0.75rem;">XLM-RoBERTa-large · F1=83.8% · {coll_label}</p>
{"".join(ctx_rows)}
</div>'''

    # ---- Collocations ----
    ru_coll = coll.get("russian", {}).get("collocates", []) if isinstance(coll.get("russian"), dict) else []
    ua_coll = coll.get("ukrainian", {}).get("collocates", []) if isinstance(coll.get("ukrainian"), dict) else []
    ru_cloud = make_collocation_cloud(ru_coll, "#dc2626", ru, strikethrough=True)
    ua_cloud = make_collocation_cloud(ua_coll, "#0057B8", ua)
    coll_html = ""
    if ru_cloud or ua_cloud:
        coll_html = f'''<div class="card">
  <div style="font-weight:700;font-size:0.95rem;margin-bottom:0.3rem;">Collocations</div>
  <p style="color:#6b7280;font-size:0.72rem;margin-bottom:0.75rem;">PMI-ranked · {coll_label}</p>
  {ru_cloud}{ua_cloud}
</div>'''

    cl_html = ""
    if ctx_html or coll_html:
        top_row = f'<div class="grid-2">\n{ctx_html}\n{coll_html}\n</div>' if ctx_html and coll_html else (ctx_html or coll_html or "")
        cl_html = f'<h2 id="cl">Computational Analysis</h2>\n{top_row}'

    # ---- LLM Audit — top performer per family, NO scroll ----
    llm_html = ""
    llm_models = llm.get("models", [])
    if tas > 0 and llm_models:
        gap = forced - recall
        # Pick best model per family
        best_per_fam = {}
        for m in llm_models:
            fam = m["family"]
            mtas = m.get("tas", 0) or 0
            if fam not in best_per_fam or mtas > (best_per_fam[fam].get("tas", 0) or 0):
                best_per_fam[fam] = m

        fam_order = ["Anthropic Claude", "OpenAI GPT", "Google Gemini", "Google Gemma",
                      "xAI Grok", "Meta Llama", "Mistral", "Alibaba Qwen"]

        fam_rows = []
        for fam in fam_order:
            if fam not in best_per_fam:
                continue
            m = best_per_fam[fam]
            mtas = m.get("tas", 0) or 0
            rec = m.get("x_open", 0) or 0
            fru = m.get("x_forced_ru_first", 0) or 0
            fua = m.get("x_forced_ua_first", 0) or 0
            favg = (fru + fua) / 2
            tc = "#059669" if mtas >= 0.9 else ("#d97706" if mtas >= 0.5 else "#dc2626")
            ri = '<span style="color:#059669">&#10003;</span>' if rec else '<span style="color:#dc2626">&#10007;</span>'
            fi = '<span style="color:#059669">&#10003;</span>' if favg >= 0.75 else ('<span style="color:#d97706">~</span>' if favg > 0 else '<span style="color:#dc2626">&#10007;</span>')
            fc = FAMILY_COLORS.get(fam, "#6b7280")
            fd = by_family.get(fam, {})
            fn = fd.get("n", 0) or 0
            fam_short = fam.split()[-1] if " " in fam else fam  # "Claude", "GPT", etc.
            fam_rows.append(
                f'<tr>'
                f'<td style="padding:0.4rem 0.6rem;"><span style="display:inline-block;width:8px;height:8px;border-radius:50%;background:{fc};margin-right:0.4rem;vertical-align:middle;"></span>{fam_short} <span style="color:#9ca3af;">({fn})</span></td>'
                f'<td style="font-size:0.78rem;font-family:monospace;padding:0.4rem 0.5rem;">{m["key"]}</td>'
                f'<td style="text-align:center;padding:0.4rem;">{ri}</td>'
                f'<td style="text-align:center;padding:0.4rem;">{fi}</td>'
                f'<td style="text-align:right;padding:0.4rem 0.6rem;font-weight:600;color:{tc};font-family:monospace;">{mtas*100:.0f}%</td></tr>')

        fam_table = "\n".join(fam_rows)

        llm_html = f'''<h2 id="ai">AI / LLM Audit</h2>
<div class="card" style="padding:1.5rem 1.75rem;">
  <div style="display:flex;justify-content:space-between;align-items:flex-start;flex-wrap:wrap;gap:1rem;margin-bottom:1rem;">
    <div>
      <div style="font-size:1.05rem;font-weight:700;">{n_models} Models · 8 Families</div>
      <div style="font-size:0.72rem;color:#6b7280;margin-top:0.15rem;">Toponymic Alignment Score = 0.4 &times; forced_choice + 0.6 &times; free_recall</div>
    </div>
    <div style="display:flex;gap:1.5rem;">
      <div style="text-align:center;"><div style="font-size:1.6rem;font-weight:800;font-family:'JetBrains Mono',monospace;color:#0057B8;">{recall:.1f}%</div><div style="font-size:0.62rem;color:#6b7280;text-transform:uppercase;">Free Recall</div></div>
      <div style="text-align:center;"><div style="font-size:1.6rem;font-weight:800;font-family:'JetBrains Mono',monospace;color:#059669;">{forced:.1f}%</div><div style="font-size:0.62rem;color:#6b7280;text-transform:uppercase;">Forced Choice</div></div>
      <div style="text-align:center;"><div style="font-size:1.6rem;font-weight:800;font-family:'JetBrains Mono',monospace;color:#7c3aed;">{tas:.1f}%</div><div style="font-size:0.62rem;color:#6b7280;text-transform:uppercase;">TAS</div></div>
    </div>
  </div>
  <div style="font-size:0.76rem;color:#6b7280;margin-bottom:0.75rem;padding:0.4rem 0.7rem;background:#f9fafb;border-radius:6px;border-left:3px solid #7c3aed;">Recognition-recall gap: {abs(gap):.0f}pp. Models <em>know</em> {ua} is correct but still <em>generate</em> {ru} {100-recall:.0f}% of the time.</div>
  <div style="font-size:0.7rem;color:#9ca3af;margin-bottom:0.4rem;font-weight:600;text-transform:uppercase;letter-spacing:0.04em;">Top performer per family</div>
  <table style="font-size:0.82rem;">
    <thead>
      <tr><th style="padding:0.4rem 0.6rem;">Family</th><th style="padding:0.4rem 0.5rem;">Best Model</th><th style="text-align:center;padding:0.4rem;">Recall</th><th style="text-align:center;padding:0.4rem;">Forced</th><th style="text-align:right;padding:0.4rem 0.6rem;">TAS</th></tr>
    </thead>
    <tbody>
{fam_table}
    </tbody>
  </table>
</div>'''

    # ---- Religious ----
    denom_urls = {
        "Moscow Patriarchate, Department for External Church Relations": ("Moscow Patriarchate", "https://mospat.ru/en/"),
        "World Council of Churches": ("WCC", "https://www.oikoumene.org/"),
        "Ecumenical Patriarchate of Constantinople": ("Constantinople", "https://ec-patr.org/"),
        "Holy See / Vatican": ("Vatican", "https://www.vatican.va/"),
        "Russian Orthodox Church (patriarchia.ru)": ("ROC (patriarchia.ru)", "https://patriarchia.ru/"),
    }
    rel_rows = []
    for denom in religious_data.get("denominations", []):
        for pp in denom.get("pairs", []):
            if pp.get("pair_id") == pid:
                rc, uc = pp.get("ru", 0), pp.get("ua", 0)
                if rc + uc == 0:
                    continue
                ua_pct = pp.get("ua_pct", 0) or 0
                dn = denom["name"]
                short, url = denom_urls.get(dn, (dn, "#"))
                color = "#dc2626" if ua_pct < 30 else "#059669"
                rel_rows.append(
                    f'<tr><td><a href="{url}" target="_blank" style="color:#3b82f6;text-decoration:none;">{short} ↗</a></td>'
                    f'<td style="text-align:right">{rc}</td><td style="text-align:right">{uc}</td>'
                    f'<td style="text-align:right;font-weight:700;color:{color}">{ua_pct:.1f}%</td></tr>')

    rel_html = ""
    if rel_rows:
        rel_html = f'''<h2 id="religious">Religious Sources</h2>
<div class="card">
<table><tr><th>Institution</th><th style="text-align:right">Russian form</th><th style="text-align:right">Ukrainian form</th><th style="text-align:right">UA %</th></tr>
{"".join(rel_rows)}
</table></div>'''

    # ---- TOC ----
    toc = ['<a href="#analysis">Summary</a>', '<a href="#sources">Sources</a>']
    if llm_html:
        toc.append('<a href="#ai">LLM Audit</a>')
    if cl_html:
        toc.append('<a href="#cl">CL Analysis</a>')
    if rel_html:
        toc.append('<a href="#religious">Religious</a>')
    if article_examples and str(pid) in (article_examples or {}):
        toc.append('<a href="#evidence">Source Evidence</a>')
    toc.append('<a href="#refs">References</a>')

    # ---- Human-friendly TAS label ----
    if tas >= 90:
        tas_label = "AI Alignment"
    elif tas >= 50:
        tas_label = "AI Alignment"
    else:
        tas_label = "AI Alignment"

    # ---- Events for references — global + pair-specific from same pair_events.json ----
    events_ref = [
        '<li><strong>2018-10</strong> — <a href="https://mfa.gov.ua/en/correctua">#KyivNotKiev campaign launched by MFA of Ukraine</a></li>',
        '<li><strong>2019-06</strong> — <a href="https://geonames.usgs.gov/">US Board on Geographic Names adopts Kyiv</a></li>',
        '<li><strong>2019-08</strong> — <a href="https://www.apstylebook.com/">AP Stylebook adopts Kyiv</a></li>',
        '<li><strong>2022-02</strong> — Full-scale Russian invasion accelerates global adoption</li>',
    ]
    for ev in p_events:
        events_ref.append(f'<li><strong>{ev["date"]}</strong> — {ev["label"]}</li>')

    stat_lines = []
    if ci_lo and ci_hi:
        stat_lines.append(f'<li>Bootstrap CI: <strong>{ci_point:.2f}%</strong> (95% CI [{ci_lo:.2f}, {ci_hi:.2f}]), n = {fmt_number(ci_n)}</li>')
    if tas > 0:
        stat_lines.append(f'<li>Toponymic Alignment Score: {tas:.1f}% across {n_models} models</li>')
    stat_lines.append('<li>CL classifier: <a href="https://huggingface.co/KyivNotKiev/toponym-context-classifier">XLM-RoBERTa-large</a>, F1=83.8%, 11 categories, 42,613 texts</li>')

    html = f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Full Audit: {ru} → {ua}</title>
<style>
*{{margin:0;padding:0;box-sizing:border-box}}
body{{font-family:'Inter',-apple-system,BlinkMacSystemFont,'Segoe UI',sans-serif;background:#f4f5f7;color:#1f2937;line-height:1.7;font-size:15px}}
.page{{max-width:1200px;margin:0 auto;padding:2.5rem 2rem}}
h2{{font-size:0.85rem;font-weight:700;color:#374151;margin:1.75rem 0 0.75rem;text-transform:uppercase;letter-spacing:0.06em}}
.card{{background:white;border:1px solid #e5e7eb;border-radius:12px;padding:1.25rem;margin-bottom:1rem;box-shadow:0 2px 8px rgba(0,0,0,0.04)}}
.grid-2{{display:grid;grid-template-columns:1fr 1fr;gap:1.25rem}}
.grid-3{{display:grid;grid-template-columns:1fr 1fr 1fr;gap:1rem}}
.stat{{text-align:center;padding:1rem}}
.stat-value{{font-size:2.2rem;font-weight:800;font-family:'JetBrains Mono',monospace}}
.stat-label{{font-size:0.7rem;color:#6b7280;text-transform:uppercase;letter-spacing:0.06em;margin-top:0.2rem}}
.toc{{display:flex;flex-wrap:wrap;gap:0.5rem 1.25rem;font-size:0.82rem;margin:1rem 0 1.5rem}}
.toc a{{color:#3b82f6;text-decoration:none;padding:0.3rem 0.7rem;border-radius:6px;background:#eff6ff;transition:all 0.15s}}
.toc a:hover{{background:#3b82f6;color:white}}
@keyframes borderRotate{{0%{{--angle:0deg}}100%{{--angle:360deg}}}}
@property --angle{{syntax:'<angle>';initial-value:0deg;inherits:false}}
.analysis-box{{background:#fff;border-radius:16px;padding:2rem;color:#374151;box-shadow:0 4px 24px rgba(99,102,241,0.10),0 1px 4px rgba(0,0,0,0.06);line-height:1.85;font-size:0.88rem;position:relative;overflow:visible;border:2px solid transparent;background-clip:padding-box}}
.analysis-box::before{{content:'';position:absolute;inset:-2px;border-radius:18px;padding:2px;background:conic-gradient(from var(--angle),#6366f1,#3b82f6,#0ea5e9,#8b5cf6,#6366f1);-webkit-mask:linear-gradient(#fff 0 0) content-box,linear-gradient(#fff 0 0);-webkit-mask-composite:xor;mask-composite:exclude;animation:borderRotate 4s linear infinite;pointer-events:none}}
.analysis-box p{{position:relative;z-index:1}}
.ctx-row{{display:grid;grid-template-columns:90px 1fr 1fr 60px;gap:0.5rem;align-items:center;font-size:0.8rem;padding:0.4rem 0;border-bottom:1px solid #f9fafb}}
table{{width:100%;border-collapse:collapse;font-size:0.85rem}}
th{{text-align:left;padding:0.6rem;border-bottom:2px solid #e5e7eb;color:#6b7280;font-size:0.7rem;text-transform:uppercase;letter-spacing:0.05em}}
td{{padding:0.6rem;border-bottom:1px solid #f3f4f6}}
.ref-section{{background:white;border:1px solid #e5e7eb;border-radius:12px;padding:2rem;box-shadow:0 2px 8px rgba(0,0,0,0.04)}}
.ref-section h3{{font-size:0.82rem;font-weight:700;color:#374151;text-transform:uppercase;letter-spacing:0.05em;margin:1.5rem 0 0.5rem;padding-top:1rem;border-top:1px solid #f3f4f6}}
.ref-section h3:first-child{{margin-top:0;padding-top:0;border-top:none}}
.ref-section ul{{margin-left:1.25rem;font-size:0.82rem;color:#374151;line-height:2}}
.ref-section a{{color:#3b82f6;text-decoration:none}}
.ref-section a:hover{{text-decoration:underline}}
.source-head{{display:flex;justify-content:space-between;align-items:center;margin-bottom:0.5rem;flex-wrap:wrap;gap:0.5rem}}
.source-meta{{display:flex;gap:1.5rem;font-size:0.8rem;flex-wrap:wrap}}
.source-meta span{{color:#6b7280}}
.source-meta strong{{color:#1f2937}}
@media (max-width:768px){{.page{{padding:1rem 0.75rem}}.grid-2{{grid-template-columns:1fr}}.grid-3{{grid-template-columns:1fr}}.stat-value{{font-size:1.6rem}}.analysis-box{{padding:1.5rem;font-size:0.82rem}}.ctx-row{{grid-template-columns:70px 1fr 1fr 45px;font-size:0.72rem}}}}
@media print{{.page{{max-width:100%}}.card,.analysis-box,.ref-section{{break-inside:avoid}}}}
</style>
</head>
<body>
<div class="page">

<div style="margin-bottom:2rem;">
  <div style="font-size:2.4rem;font-weight:800;line-height:1.2;">
    <span style="color:#D52B1E;text-decoration:line-through;text-decoration-thickness:3px;opacity:0.55;">{ru}</span>
    <span style="color:#9ca3af;font-weight:300;margin:0 0.3rem;">→</span>
    <span style="color:#0057B8;">{ua}</span>
  </div>
  <p style="color:#6b7280;font-size:0.85rem;margin-top:0.3rem;">{subtitle}</p>
</div>

<div class="grid-3" style="margin-bottom:1.5rem;">
  <div class="card stat"><div class="stat-value" style="color:#0057B8;">{adoption:.1f}%</div><div class="stat-label">Current Adoption</div></div>
  <div class="card stat"><div class="stat-value">{fmt_number(total)}</div><div class="stat-label">Total Mentions</div></div>
  <div class="card stat"><div class="stat-value" style="color:#7c3aed;">{tas:.1f}%</div><div class="stat-label">{tas_label}</div></div>
</div>

<nav class="toc">{"".join(toc)}</nav>

<h2 id="analysis">Summary</h2>
<div class="analysis-box">
  {analysis_text}
</div>

<h2 id="sources">Data by Source</h2>
{"".join(source_charts)}

{llm_html}

{cl_html}

{rel_html}

{_build_evidence_section(pid, ru, ua, article_examples)}

<h2 id="refs">References & Methodology</h2>
<div class="ref-section">
  <h3>Key Events</h3>
  <ul>{"".join(events_ref)}</ul>
  <h3>Statistical Summary</h3>
  <ul>{"".join(stat_lines)}</ul>
  <h3>Data Sources</h3>
  <ul>
    <li><a href="https://trends.google.com/trends/">Google Trends</a> — search interest, 55 countries</li>
    <li><a href="https://www.gdeltproject.org/">GDELT</a> — news from 53K+ domains</li>
    <li><a href="https://wikimedia.org/api/rest_v1/">Wikipedia Pageviews API</a></li>
    <li><a href="https://arctic-shift.photon-reddit.com/">Reddit (Arctic Shift)</a> — 12 subreddits</li>
    <li><a href="https://www.youtube.com/">YouTube</a> — via yt-dlp</li>
    <li><a href="https://books.google.com/ngrams/">Google Books Ngrams</a> — corpus 37, 1900–2022</li>
    <li><a href="https://openalex.org/">OpenAlex</a> — 250M+ academic works</li>
    <li><a href="https://openlibrary.org/">Open Library</a> — 8M+ book records</li>
  </ul>
  <h3>Links</h3>
  <ul>
    <li><a href="https://mfa.gov.ua/en/correctua">#CorrectUA — MFA of Ukraine</a></li>
    <li><a href="https://huggingface.co/KyivNotKiev">HuggingFace — datasets, corpus & model</a></li>
    <li><a href="https://github.com/IvanDobrovolsky/kyivnotkiev">GitHub — source code & pipeline</a></li>
  </ul>
</div>

</div>
</body>
</html>'''
    return html


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pair", type=int, help="Generate for single pair ID")
    parser.add_argument("--first", type=int, help="Generate for first N pairs")
    args = parser.parse_args()

    manifest = load_json("manifest.json")
    timeseries = load_json("timeseries.json")
    llm_per_pair = load_json("llm_per_pair.json").get("pairs", {})
    collocations = load_json("cl_collocations.json")
    religious = load_json("religious.json")
    pair_events = load_json("pair_events.json")
    stats = load_json("statistical_tests.json").get("pair_bootstrap_cis", {})
    cl_analysis = load_json("cl_analysis.json")
    ctx_distributions = cl_analysis.get("context_distribution", {})

    # Load confusion matrix SVG
    cm_svg_path = DATA / "confusion_matrix_svg.html"
    cm_svg = cm_svg_path.read_text() if cm_svg_path.exists() else ""

    # Load per-pair chart annotations
    try:
        chart_annotations = load_json("chart_annotations.json")
    except Exception:
        chart_annotations = {}

    try:
        article_examples = load_json("article_examples.json")
    except Exception:
        article_examples = {}

    all_pairs = manifest["pairs"]

    if args.pair:
        pairs_to_gen = [p for p in all_pairs if p["id"] == args.pair]
    elif args.first:
        pairs_to_gen = all_pairs[:args.first]
    else:
        pairs_to_gen = all_pairs

    print(f"Generating reports for {len(pairs_to_gen)} pairs...")

    reports_json = {}
    for pair in pairs_to_gen:
        pid = pair["id"]
        out_file = OUT / f"pair-{pid}.html"
        html = generate_report(
            pair, timeseries, llm_per_pair, collocations,
            religious, pair_events, stats, ctx_distributions, cm_svg, chart_annotations, all_pairs, article_examples
        )
        out_file.write_text(html)
        size_kb = len(html) / 1024
        print(f"  Pair {pid}: {pair['russian']} → {pair['ukrainian']} — {size_kb:.0f}KB")

        # Extract body content for the Astro /report page JSON
        content_start = html.find('<h2 id="analysis">')
        body_end = html.find('</body>')
        if content_start >= 0 and body_end >= 0:
            page_close = html.rfind('</div>', 0, body_end)
            if page_close >= 0:
                reports_json[str(pid)] = html[content_start:page_close].strip()

    # Write JSON index for Astro /report page
    json_path = DATA / "pair_reports.json"
    with open(json_path, "w") as f:
        json.dump(reports_json, f, ensure_ascii=False)
    print(f"JSON index: {json_path} ({len(reports_json)} pairs)")

    print(f"\nDone! Reports saved to {OUT}/")


if __name__ == "__main__":
    main()
