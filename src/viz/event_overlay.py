"""Event-driven spike charts with geopolitical event overlay.

Usage:
    python -m src.viz.event_overlay [--source gdelt|trends] [--pair-ids 1,2,3]
"""

import argparse
import logging

import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import pandas as pd

from src.config import (
    PROCESSED_DIR,
    FIGURES_DIR,
    EVENTS_TIMELINE,
    VIZ_DPI,
    VIZ_FIGSIZE,
    VIZ_STYLE,
    COLOR_RUSSIAN,
    COLOR_UKRAINIAN,
    ensure_dirs,
    get_all_pairs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def plot_event_overlay(
    df: pd.DataFrame,
    pair: dict,
    source: str,
    save: bool = True,
) -> plt.Figure:
    """Plot search/mention volume with event markers."""
    plt.style.use(VIZ_STYLE)

    fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(VIZ_FIGSIZE[0], VIZ_FIGSIZE[1] * 1.3),
                                    gridspec_kw={"height_ratios": [3, 1]}, sharex=True)

    time_col = "week" if "week" in df.columns else "year"
    dates = pd.to_datetime(df[time_col])

    # Top panel: raw volume
    if "russian_count" in df.columns:
        r_vals = df["russian_count"]
        u_vals = df["ukrainian_count"]
    elif "russian_interest" in df.columns:
        r_vals = df["russian_interest"]
        u_vals = df["ukrainian_interest"]
    else:
        ratio = df["adoption_ratio"].fillna(0.5)
        r_vals = 1 - ratio
        u_vals = ratio

    window = 4 if len(dates) > 20 else 1
    ax1.plot(dates, r_vals.rolling(window, min_periods=1).mean(),
             color=COLOR_RUSSIAN, linewidth=1.5, label=f'"{pair["russian"]}"', alpha=0.8)
    ax1.plot(dates, u_vals.rolling(window, min_periods=1).mean(),
             color=COLOR_UKRAINIAN, linewidth=1.5, label=f'"{pair["ukrainian"]}"', alpha=0.8)

    # Event markers
    for event in EVENTS_TIMELINE:
        event_date = pd.Timestamp(event["date"])
        if dates.min() <= event_date <= dates.max():
            color = event.get("color", "#7F8C8D")
            ax1.axvline(event_date, color=color, linestyle="--", alpha=0.6, linewidth=1.2)
            ax1.annotate(
                event["name"],
                xy=(event_date, ax1.get_ylim()[1] if ax1.get_ylim()[1] > 0 else 1),
                xytext=(5, -5), textcoords="offset points",
                fontsize=7, color=color, rotation=45, ha="left", va="top",
            )
            ax2.axvline(event_date, color=color, linestyle="--", alpha=0.6, linewidth=1.2)

    ax1.set_ylabel("Volume", fontsize=11)
    ax1.legend(fontsize=10)
    ax1.set_title(
        f'Event Impact: "{pair["russian"]}" vs "{pair["ukrainian"]}" — {source.upper()}',
        fontsize=13, fontweight="bold",
    )
    ax1.grid(True, alpha=0.3)

    # Bottom panel: adoption ratio
    ratio = df["adoption_ratio"].fillna(0).rolling(window, min_periods=1).mean()
    ax2.fill_between(dates, 0, ratio, color=COLOR_UKRAINIAN, alpha=0.3)
    ax2.fill_between(dates, ratio, 1, color=COLOR_RUSSIAN, alpha=0.3)
    ax2.plot(dates, ratio, color="black", linewidth=1.5)
    ax2.axhline(0.5, color="#666", linestyle=":", alpha=0.5)
    ax2.set_ylabel("Adoption ratio", fontsize=11)
    ax2.set_ylim(-0.05, 1.05)
    ax2.set_xlabel("Date", fontsize=11)
    ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax2.xaxis.set_major_locator(mdates.YearLocator())
    ax2.grid(True, alpha=0.3)

    fig.autofmt_xdate()
    plt.tight_layout()

    if save:
        out_path = FIGURES_DIR / f"events_{pair['id']:02d}_{source}.png"
        fig.savefig(out_path, dpi=VIZ_DPI, bbox_inches="tight")
        log.info(f"Saved: {out_path}")

    return fig


def plot_all(source: str = "gdelt", pair_ids: list[int] | None = None):
    """Generate event overlay charts for selected pairs."""
    ensure_dirs()

    data_path = PROCESSED_DIR / f"{source}_merged.parquet"
    if not data_path.exists():
        log.error(f"Data not found: {data_path}")
        return

    df = pd.read_parquet(data_path)

    # Default to most interesting pairs
    if pair_ids is None:
        pair_ids = [1, 2, 3, 5, 11]  # Kyiv, Kharkiv, Odesa, Zaporizhzhia, Luhansk

    pairs = [p for p in get_all_pairs() if p["id"] in pair_ids]

    for pair in pairs:
        pair_data = df[df["pair_id"] == pair["id"]]
        if pair_data.empty:
            continue
        pair_data = pair_data.sort_values("week" if "week" in pair_data.columns else pair_data.columns[0])
        fig = plot_event_overlay(pair_data, pair, source)
        plt.close(fig)


def main():
    parser = argparse.ArgumentParser(description="Generate event overlay charts")
    parser.add_argument("--source", type=str, default="gdelt", choices=["gdelt", "trends"])
    parser.add_argument("--pair-ids", type=str, default=None)
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    plot_all(source=args.source, pair_ids=pair_ids)


if __name__ == "__main__":
    main()
