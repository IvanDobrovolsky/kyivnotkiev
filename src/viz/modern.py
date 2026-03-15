"""Modern publication-quality visualizations using Plotly.

Generates interactive HTML charts and static exports with a clean,
contemporary design language.

Usage:
    python -m src.viz.modern
"""

import logging
import json

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

from src.config import (
    PROCESSED_DIR,
    FIGURES_DIR,
    EVENTS_TIMELINE,
    ensure_dirs,
    get_all_pairs,
    get_categories,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

# ── Design tokens ──────────────────────────────────────────────────────────────

COLORS = {
    "ukrainian": "#0057B8",
    "russian": "#E74C3C",
    "bg": "#FAFAFA",
    "grid": "#E8E8E8",
    "text": "#2C3E50",
    "muted": "#95A5A6",
    "accent": "#FFD700",
}

CAT_COLORS = {
    "geographical": "#0057B8",
    "food": "#E67E22",
    "landmarks": "#8E44AD",
    "country": "#2ECC71",
    "institutional": "#3498DB",
    "sports": "#E74C3C",
    "historical": "#7F8C8D",
}

LAYOUT_DEFAULTS = dict(
    font=dict(family="Inter, Helvetica, Arial, sans-serif", color=COLORS["text"]),
    plot_bgcolor=COLORS["bg"],
    paper_bgcolor="white",
    margin=dict(l=60, r=30, t=80, b=60),
    hovermode="x unified",
)

EVENT_ANNOTATIONS = [
    {"date": "2018-10-02", "label": "#KyivNotKiev", "color": "#0057B8"},
    {"date": "2019-08-14", "label": "AP adopts Kyiv", "color": "#87CEEB"},
    {"date": "2022-02-24", "label": "Full-scale invasion", "color": "#DC143C"},
    {"date": "2022-09-06", "label": "Kharkiv counteroffensive", "color": "#228B22"},
]


def _add_events(fig, y_max, events=None):
    """Add event marker lines and annotations to a figure."""
    if events is None:
        events = EVENT_ANNOTATIONS
    for ev in events:
        fig.add_vline(
            x=ev["date"], line_dash="dot", line_color=ev["color"],
            line_width=1.5, opacity=0.6,
        )
        fig.add_annotation(
            x=ev["date"], y=y_max * 0.97, text=ev["label"],
            showarrow=False, textangle=-35, font=dict(size=9, color=ev["color"]),
            xanchor="left", yanchor="top",
        )


def _save(fig, name, width=1400, height=700):
    """Save as both HTML (interactive) and PNG (static)."""
    html_path = FIGURES_DIR / f"{name}.html"
    fig.write_html(str(html_path), include_plotlyjs="cdn")
    log.info(f"Saved: {html_path}")

    try:
        png_path = FIGURES_DIR / f"{name}.png"
        fig.write_image(str(png_path), width=width, height=height, scale=2)
        log.info(f"Saved: {png_path}")
    except Exception as e:
        log.warning(f"PNG export failed (install kaleido): {e}")


# ── Chart 1: Flagship Crossover ───────────────────────────────────────────────

def plot_flagship_crossover(source: str = "gdelt"):
    """The iconic Kiev vs Kyiv chart with area fill and event markers."""
    df = pd.read_parquet(PROCESSED_DIR / f"{source}_merged.parquet")
    pair = df[df["pair_id"] == 1].sort_values("week").copy()

    if pair.empty:
        log.warning("No data for flagship pair")
        return

    pair["week"] = pd.to_datetime(pair["week"])
    window = 4
    pair["r_smooth"] = pair["russian_count"].rolling(window, min_periods=1).mean()
    pair["u_smooth"] = pair["ukrainian_count"].rolling(window, min_periods=1).mean()

    fig = go.Figure()

    fig.add_trace(go.Scatter(
        x=pair["week"], y=pair["r_smooth"], name='"Kiev"',
        line=dict(color=COLORS["russian"], width=2.5),
        fill="tozeroy", fillcolor="rgba(231,76,60,0.08)",
    ))
    fig.add_trace(go.Scatter(
        x=pair["week"], y=pair["u_smooth"], name='"Kyiv"',
        line=dict(color=COLORS["ukrainian"], width=2.5),
        fill="tozeroy", fillcolor="rgba(0,87,184,0.08)",
    ))

    y_max = max(pair["r_smooth"].max(), pair["u_smooth"].max()) * 1.1
    _add_events(fig, y_max)

    fig.update_layout(
        **LAYOUT_DEFAULTS,
        title=dict(
            text='<b>"Kiev" vs "Kyiv"</b> — Global News Media (GDELT, 2015–2026)',
            font=dict(size=20),
        ),
        xaxis=dict(title="", gridcolor=COLORS["grid"], showgrid=True),
        yaxis=dict(title="Weekly article mentions", gridcolor=COLORS["grid"]),
        legend=dict(x=0.02, y=0.98, bgcolor="rgba(255,255,255,0.8)",
                    bordercolor=COLORS["grid"], borderwidth=1),
        height=550,
    )

    _save(fig, f"modern_flagship_{source}")
    return fig


# ── Chart 2: Adoption Heatmap ─────────────────────────────────────────────────

def plot_adoption_heatmap(source: str = "gdelt"):
    """Clean heatmap showing adoption ratio over time for all pairs."""
    df = pd.read_parquet(PROCESSED_DIR / f"{source}_merged.parquet")
    pairs = [p for p in get_all_pairs() if not (p["is_control"] and p["russian"] == p["ukrainian"])]

    time_col = "week"
    df_agg = df.groupby(["pair_id", time_col]).agg(adoption_ratio=("adoption_ratio", "mean")).reset_index()

    # Build matrix
    all_times = sorted(df_agg[time_col].unique())
    labels = []
    matrix = []
    crossover_order = []

    for pair in pairs:
        pair_data = df_agg[df_agg["pair_id"] == pair["id"]].set_index(time_col)
        if pair_data.empty:
            row = [np.nan] * len(all_times)
            crossover_order.append(len(all_times))
        else:
            row = [pair_data.loc[t, "adoption_ratio"] if t in pair_data.index else np.nan for t in all_times]
            first_above = next((i for i, v in enumerate(row) if v is not None and not np.isnan(v) and v >= 0.5), len(all_times))
            crossover_order.append(first_above)

        labels.append(f"{pair['russian']} → {pair['ukrainian']}")
        matrix.append(row)

    # Sort by crossover time
    order = np.argsort(crossover_order)
    matrix = [matrix[i] for i in order]
    labels = [labels[i] for i in order]

    # Subsample time axis for readability
    step = max(1, len(all_times) // 60)
    time_labels = [str(all_times[i])[:10] if i % step == 0 else "" for i in range(len(all_times))]

    fig = go.Figure(data=go.Heatmap(
        z=matrix,
        x=[str(t)[:10] for t in all_times],
        y=labels,
        colorscale=[[0, COLORS["russian"]], [0.5, "white"], [1, COLORS["ukrainian"]]],
        zmin=0, zmax=1,
        colorbar=dict(title=dict(text="Adoption<br>ratio", side="right"),
                      tickvals=[0, 0.5, 1], ticktext=["Russian", "50/50", "Ukrainian"]),
        hovertemplate="<b>%{y}</b><br>Week: %{x}<br>Ratio: %{z:.2f}<extra></extra>",
    ))

    fig.update_layout(
        **LAYOUT_DEFAULTS,
        title=dict(
            text=f"<b>Ukrainian Toponym Adoption Timeline</b> — {source.upper()} (2015–2026)",
            font=dict(size=18),
        ),
        xaxis=dict(title="", tickangle=-45, dtick=26),
        yaxis=dict(title="", autorange="reversed"),
        height=max(500, len(labels) * 22),
        width=1200,
    )

    _save(fig, f"modern_heatmap_{source}", width=1200, height=max(500, len(labels) * 22))
    return fig


# ── Chart 3: Category Comparison (Dumbbell) ───────────────────────────────────

def plot_category_dumbbell():
    """Dumbbell chart comparing GDELT vs Trends adoption per category."""
    summary = pd.read_csv(PROCESSED_DIR / "cross_source_summary.csv")

    cat_stats = summary.groupby("category").agg(
        gdelt=("gdelt_ratio", "mean"),
        trends=("trends_ratio", "mean"),
        n=("id", "count"),
    ).reset_index()

    # Sort by mean adoption
    cat_stats["mean"] = cat_stats[["gdelt", "trends"]].mean(axis=1)
    cat_stats = cat_stats.sort_values("mean", ascending=True)

    cat_names = {c["id"]: c["name"] for c in get_categories()}
    cat_stats["label"] = cat_stats["category"].map(cat_names)

    fig = go.Figure()

    # Connecting lines
    for _, row in cat_stats.iterrows():
        gdelt_val = row["gdelt"] if not np.isnan(row["gdelt"]) else None
        trends_val = row["trends"] if not np.isnan(row["trends"]) else None
        if gdelt_val is not None and trends_val is not None:
            fig.add_trace(go.Scatter(
                x=[gdelt_val, trends_val], y=[row["label"], row["label"]],
                mode="lines", line=dict(color=COLORS["muted"], width=2),
                showlegend=False, hoverinfo="skip",
            ))

    # GDELT dots
    valid_gdelt = cat_stats.dropna(subset=["gdelt"])
    fig.add_trace(go.Scatter(
        x=valid_gdelt["gdelt"], y=valid_gdelt["label"],
        mode="markers", name="GDELT (media)",
        marker=dict(size=14, color=COLORS["ukrainian"], symbol="circle",
                    line=dict(width=2, color="white")),
        hovertemplate="%{y}: %{x:.2f}<extra>GDELT</extra>",
    ))

    # Trends dots
    valid_trends = cat_stats.dropna(subset=["trends"])
    fig.add_trace(go.Scatter(
        x=valid_trends["trends"], y=valid_trends["label"],
        mode="markers", name="Google Trends (public)",
        marker=dict(size=14, color=COLORS["accent"], symbol="diamond",
                    line=dict(width=2, color="white")),
        hovertemplate="%{y}: %{x:.2f}<extra>Trends</extra>",
    ))

    fig.add_vline(x=0.5, line_dash="dot", line_color=COLORS["muted"], opacity=0.5)
    fig.add_annotation(x=0.5, y=-0.5, text="50% crossover", showarrow=False,
                       font=dict(size=10, color=COLORS["muted"]))

    fig.update_layout(
        **LAYOUT_DEFAULTS,
        title=dict(
            text="<b>Adoption by Category</b> — Media vs Public Search Interest",
            font=dict(size=18),
        ),
        xaxis=dict(title="Mean adoption ratio", range=[-0.05, 1.05],
                   gridcolor=COLORS["grid"]),
        yaxis=dict(title=""),
        legend=dict(x=0.7, y=0.05),
        height=450,
    )

    _save(fig, "modern_category_dumbbell", height=450)
    return fig


# ── Chart 4: Per-Pair Adoption Status ─────────────────────────────────────────

def plot_pair_status():
    """Horizontal bar chart showing adoption status for each pair, colored by category."""
    summary = pd.read_csv(PROCESSED_DIR / "cross_source_summary.csv")

    # Use best available ratio (prefer Trends for public-facing)
    summary["best_ratio"] = summary["trends_ratio"].fillna(summary["gdelt_ratio"])
    summary = summary.dropna(subset=["best_ratio"])
    summary = summary.sort_values("best_ratio", ascending=True)

    fig = go.Figure()

    for cat, color in CAT_COLORS.items():
        cat_data = summary[summary["category"] == cat]
        if cat_data.empty:
            continue
        cat_name = next((c["name"] for c in get_categories() if c["id"] == cat), cat)
        fig.add_trace(go.Bar(
            y=[f"{r['russian']} → {r['ukrainian']}" for _, r in cat_data.iterrows()],
            x=cat_data["best_ratio"],
            orientation="h",
            name=cat_name,
            marker=dict(color=color, opacity=0.85,
                        line=dict(width=0.5, color="white")),
            hovertemplate="%{y}<br>Ratio: %{x:.3f}<extra>%{fullData.name}</extra>",
        ))

    fig.add_vline(x=0.5, line_dash="dot", line_color=COLORS["muted"])

    fig.update_layout(
        **LAYOUT_DEFAULTS,
        title=dict(
            text="<b>Adoption Status: All Toponym Pairs</b>",
            font=dict(size=18),
        ),
        xaxis=dict(title="Adoption ratio (0 = Russian dominant, 1 = Ukrainian dominant)",
                   range=[0, 1.05], gridcolor=COLORS["grid"]),
        yaxis=dict(title=""),
        barmode="stack",
        height=max(500, len(summary) * 25),
        legend=dict(x=0.65, y=0.02),
    )

    _save(fig, "modern_pair_status", height=max(500, len(summary) * 25))
    return fig


# ── Chart 5: Event Impact Waterfall ───────────────────────────────────────────

def plot_event_impact():
    """Waterfall chart showing cumulative impact of events on Kiev→Kyiv adoption."""
    events_path = PROCESSED_DIR / "events_gdelt.parquet"
    if not events_path.exists():
        return

    df = pd.read_parquet(events_path)
    # Focus on pair 1 (Kiev/Kyiv)
    pair_events = df[(df["pair_id"] == 1) & (df["significant"] == True)].copy()
    if pair_events.empty:
        return

    pair_events = pair_events.sort_values("event_date")

    fig = go.Figure(go.Waterfall(
        x=pair_events["event_name"],
        y=pair_events["delta"],
        text=[f"+{d:.1%}" if d > 0 else f"{d:.1%}" for d in pair_events["delta"]],
        textposition="outside",
        connector=dict(line=dict(color=COLORS["muted"])),
        increasing=dict(marker=dict(color=COLORS["ukrainian"])),
        decreasing=dict(marker=dict(color=COLORS["russian"])),
    ))

    fig.update_layout(
        **LAYOUT_DEFAULTS,
        title=dict(
            text='<b>Event Impact on "Kyiv" Adoption</b> — Statistically Significant Events (p<0.05)',
            font=dict(size=17),
        ),
        xaxis=dict(title="", tickangle=-25),
        yaxis=dict(title="Change in adoption ratio", tickformat=".0%",
                   gridcolor=COLORS["grid"]),
        height=450,
        showlegend=False,
    )

    _save(fig, "modern_event_waterfall", height=450)
    return fig


# ── Chart 6: Per-Category Detail Pages ─────────────────────────────────────────

def plot_category_detail(category_id: str, source: str = "gdelt"):
    """Multi-panel chart for a single category showing all pairs."""
    df = pd.read_parquet(PROCESSED_DIR / f"{source}_merged.parquet")
    pairs = [p for p in get_all_pairs()
             if p["category"] == category_id
             and not (p["is_control"] and p["russian"] == p["ukrainian"])]

    if not pairs:
        return

    cat_name = next((c["name"] for c in get_categories() if c["id"] == category_id), category_id)

    n_pairs = len(pairs)
    cols = min(3, n_pairs)
    rows = (n_pairs + cols - 1) // cols

    subtitles = [f'{p["russian"]} → {p["ukrainian"]}' for p in pairs]
    fig = make_subplots(rows=rows, cols=cols, subplot_titles=subtitles,
                        vertical_spacing=0.08, horizontal_spacing=0.06)

    for i, pair in enumerate(pairs):
        row = i // cols + 1
        col = i % cols + 1

        pair_data = df[df["pair_id"] == pair["id"]].sort_values(
            "week" if "week" in df.columns else df.columns[0]
        )

        if pair_data.empty:
            continue

        dates = pd.to_datetime(pair_data["week"] if "week" in pair_data.columns else pair_data.iloc[:, 0])
        ratio = pair_data["adoption_ratio"].rolling(4, min_periods=1).mean()

        # Area fill for adoption ratio
        fig.add_trace(go.Scatter(
            x=dates, y=ratio, mode="lines",
            line=dict(color=COLORS["ukrainian"], width=2),
            fill="tozeroy", fillcolor="rgba(0,87,184,0.15)",
            name=pair["ukrainian"], showlegend=False,
            hovertemplate="%{x|%Y-%m}: %{y:.2f}<extra></extra>",
        ), row=row, col=col)

        # 50% line
        fig.add_hline(y=0.5, line_dash="dot", line_color=COLORS["muted"],
                      opacity=0.5, row=row, col=col)

        fig.update_yaxes(range=[0, 1.05], row=row, col=col, gridcolor=COLORS["grid"])
        fig.update_xaxes(gridcolor=COLORS["grid"], row=row, col=col)

    fig.update_layout(
        **LAYOUT_DEFAULTS,
        title=dict(
            text=f"<b>{cat_name}</b> — Adoption Ratio Over Time ({source.upper()})",
            font=dict(size=18),
        ),
        height=300 * rows,
        width=1200,
    )

    _save(fig, f"modern_category_{category_id}_{source}", width=1200, height=300 * rows)
    return fig


# ── Chart 7: Resistance Spectrum ──────────────────────────────────────────────

def plot_resistance_spectrum():
    """Dot plot showing the spectrum from full adoption to full resistance."""
    summary = pd.read_csv(PROCESSED_DIR / "cross_source_summary.csv")
    summary["best_ratio"] = summary["trends_ratio"].fillna(summary["gdelt_ratio"])
    summary = summary.dropna(subset=["best_ratio"]).sort_values("best_ratio")

    fig = go.Figure()

    for _, row in summary.iterrows():
        color = CAT_COLORS.get(row["category"], "#666")
        fig.add_trace(go.Scatter(
            x=[row["best_ratio"]],
            y=[f"{row['ukrainian']}"],
            mode="markers+text",
            marker=dict(size=12, color=color, opacity=0.85,
                        line=dict(width=1, color="white")),
            text=f" {row['best_ratio']:.0%}" if row["best_ratio"] > 0.01 else " <1%",
            textposition="middle right",
            textfont=dict(size=9),
            showlegend=False,
            hovertemplate=f"{row['russian']} → {row['ukrainian']}<br>"
                          f"Category: {row['category']}<br>"
                          f"Ratio: {row['best_ratio']:.3f}<extra></extra>",
        ))

    # Add zone backgrounds
    fig.add_vrect(x0=0, x1=0.2, fillcolor=COLORS["russian"], opacity=0.05,
                  annotation_text="Resistant", annotation_position="top left",
                  annotation_font_size=10, annotation_font_color=COLORS["muted"])
    fig.add_vrect(x0=0.2, x1=0.5, fillcolor=COLORS["accent"], opacity=0.05,
                  annotation_text="Emerging", annotation_position="top left",
                  annotation_font_size=10, annotation_font_color=COLORS["muted"])
    fig.add_vrect(x0=0.5, x1=0.8, fillcolor="lightblue", opacity=0.05,
                  annotation_text="Crossing", annotation_position="top left",
                  annotation_font_size=10, annotation_font_color=COLORS["muted"])
    fig.add_vrect(x0=0.8, x1=1.0, fillcolor=COLORS["ukrainian"], opacity=0.05,
                  annotation_text="Adopted", annotation_position="top left",
                  annotation_font_size=10, annotation_font_color=COLORS["muted"])

    fig.update_layout(
        **LAYOUT_DEFAULTS,
        title=dict(
            text="<b>Adoption Spectrum</b> — From Resistant to Fully Adopted",
            font=dict(size=18),
        ),
        xaxis=dict(title="Adoption ratio", range=[-0.05, 1.15],
                   gridcolor=COLORS["grid"], tickformat=".0%"),
        yaxis=dict(title=""),
        height=max(500, len(summary) * 22),
    )

    _save(fig, "modern_resistance_spectrum", height=max(500, len(summary) * 22))
    return fig


# ── Generate All ──────────────────────────────────────────────────────────────

def generate_all(source: str = "gdelt"):
    """Generate all modern charts."""
    ensure_dirs()

    log.info("=== Flagship crossover ===")
    plot_flagship_crossover(source)

    log.info("=== Adoption heatmap ===")
    plot_adoption_heatmap(source)

    log.info("=== Category dumbbell ===")
    plot_category_dumbbell()

    log.info("=== Pair status bars ===")
    plot_pair_status()

    log.info("=== Event waterfall ===")
    plot_event_impact()

    log.info("=== Resistance spectrum ===")
    plot_resistance_spectrum()

    log.info("=== Category details ===")
    for cat in get_categories():
        plot_category_detail(cat["id"], source)

    log.info("=== All modern charts generated ===")


if __name__ == "__main__":
    generate_all()
