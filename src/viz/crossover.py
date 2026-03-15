"""Flagship crossover charts: Ukrainian vs Russian spelling over time.

Usage:
    python -m src.viz.crossover [--source gdelt|trends] [--pair-ids 1,2,3]
"""

import argparse
import logging

import matplotlib.dates as mdates
import matplotlib.pyplot as plt
import pandas as pd

from src.config import (
    COLOR_EVENT,
    COLOR_RUSSIAN,
    COLOR_UKRAINIAN,
    EVENTS_TIMELINE,
    FIGURES_DIR,
    PROCESSED_DIR,
    VIZ_DPI,
    VIZ_FIGSIZE,
    VIZ_STYLE,
    ensure_dirs,
    get_all_pairs,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def plot_crossover(
    df: pd.DataFrame,
    pair: dict,
    source: str,
    show_events: bool = True,
    save: bool = True,
) -> plt.Figure:
    """Plot the crossover chart for a single toponym pair."""
    plt.style.use(VIZ_STYLE)

    fig, ax = plt.subplots(figsize=VIZ_FIGSIZE)

    time_col = "week" if "week" in df.columns else "year"
    dates = pd.to_datetime(df[time_col])

    russian = pair["russian"]
    ukrainian = pair["ukrainian"]

    # Determine which columns contain the counts/interest
    if "russian_count" in df.columns:
        r_vals = df["russian_count"]
        u_vals = df["ukrainian_count"]
        ylabel = "Article mentions (GDELT)"
    elif "russian_interest" in df.columns:
        r_vals = df["russian_interest"]
        u_vals = df["ukrainian_interest"]
        ylabel = "Search interest (Google Trends)"
    else:
        r_vals = 1 - df["adoption_ratio"].fillna(0.5)
        u_vals = df["adoption_ratio"].fillna(0.5)
        ylabel = "Relative share"

    # Smooth with 4-week rolling mean
    window = 4 if len(dates) > 20 else 1
    r_smooth = r_vals.rolling(window=window, min_periods=1).mean()
    u_smooth = u_vals.rolling(window=window, min_periods=1).mean()

    ax.plot(dates, r_smooth, color=COLOR_RUSSIAN, linewidth=2, label=f'"{russian}"', alpha=0.9)
    ax.plot(dates, u_smooth, color=COLOR_UKRAINIAN, linewidth=2, label=f'"{ukrainian}"', alpha=0.9)

    # Fill between to highlight dominance
    ax.fill_between(dates, r_smooth, u_smooth,
                     where=u_smooth >= r_smooth,
                     color=COLOR_UKRAINIAN, alpha=0.1, interpolate=True)
    ax.fill_between(dates, r_smooth, u_smooth,
                     where=r_smooth > u_smooth,
                     color=COLOR_RUSSIAN, alpha=0.1, interpolate=True)

    # Event lines
    if show_events:
        for event in EVENTS_TIMELINE:
            event_date = pd.Timestamp(event["date"])
            if dates.min() <= event_date <= dates.max():
                ax.axvline(event_date, color=event.get("color", COLOR_EVENT),
                          linestyle="--", alpha=0.5, linewidth=1)
                ax.text(event_date, ax.get_ylim()[1] * 0.95, event["name"],
                       rotation=45, fontsize=7, ha="left", va="top",
                       color=event.get("color", COLOR_EVENT))

    ax.set_xlabel("Date", fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_title(f'"{russian}" vs "{ukrainian}" — {source.upper()} ({pair["significance"]})',
                fontsize=14, fontweight="bold")
    ax.legend(fontsize=11, loc="upper left")
    ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    ax.xaxis.set_major_locator(mdates.YearLocator())
    fig.autofmt_xdate()
    ax.grid(True, alpha=0.3)

    plt.tight_layout()

    if save:
        out_path = FIGURES_DIR / f"crossover_{pair['id']:02d}_{source}.png"
        fig.savefig(out_path, dpi=VIZ_DPI, bbox_inches="tight")
        log.info(f"Saved: {out_path}")

    return fig


def plot_flagship(source: str = "gdelt", save: bool = True) -> plt.Figure:
    """Plot the flagship Kiev/Kyiv crossover chart."""
    data_path = PROCESSED_DIR / f"{source}_merged.parquet"
    if not data_path.exists():
        log.error(f"Data not found: {data_path}")
        return None

    df = pd.read_parquet(data_path)
    pair1 = [p for p in get_all_pairs() if p["id"] == 1][0]  # Kiev/Kyiv
    pair_data = df[df["pair_id"] == 1].sort_values("week" if "week" in df.columns else df.columns[0])

    if pair_data.empty:
        log.error("No data for pair 1 (Kiev/Kyiv)")
        return None

    return plot_crossover(pair_data, pair1, source, save=save)


def plot_all(
    source: str = "gdelt",
    pair_ids: list[int] | None = None,
) -> list[plt.Figure]:
    """Generate crossover charts for all pairs."""
    ensure_dirs()

    data_path = PROCESSED_DIR / f"{source}_merged.parquet"
    if not data_path.exists():
        log.error(f"Data not found: {data_path}")
        return []

    df = pd.read_parquet(data_path)
    pairs = get_all_pairs()

    if pair_ids:
        pairs = [p for p in pairs if p["id"] in pair_ids]

    figs = []
    for pair in pairs:
        if pair["is_control"] and pair["russian"] == pair["ukrainian"]:
            continue

        pair_data = df[df["pair_id"] == pair["id"]]
        if pair_data.empty:
            continue

        pair_data = pair_data.sort_values("week" if "week" in pair_data.columns else pair_data.columns[0])
        fig = plot_crossover(pair_data, pair, source)
        figs.append(fig)
        plt.close(fig)

    log.info(f"Generated {len(figs)} crossover charts")
    return figs


def main():
    parser = argparse.ArgumentParser(description="Generate crossover charts")
    parser.add_argument("--source", type=str, default="gdelt", choices=["gdelt", "trends"])
    parser.add_argument("--pair-ids", type=str, default=None)
    args = parser.parse_args()

    pair_ids = None
    if args.pair_ids:
        pair_ids = [int(x) for x in args.pair_ids.split(",")]

    if pair_ids:
        plot_all(source=args.source, pair_ids=pair_ids)
    else:
        plot_all(source=args.source)


if __name__ == "__main__":
    main()
