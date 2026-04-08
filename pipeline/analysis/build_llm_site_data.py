"""Snapshot the v2 TAS sweep into a stable site-consumable JSON.

Reads every per-model checkpoint under data/raw/llm_spelling/v2/checkpoints/,
filters to fully-completed runs (162/162 trials), enriches with the
registry metadata (release_date, family, tier), and writes
site/src/data/llm_trajectory.json with the shape the website needs to
render the version-archaeology chart.

Output structure:
  {
    "models": [
      {
        "key": "claude-opus-4-6",
        "label": "Claude Opus 4.6",
        "family": "Anthropic Claude",
        "tier": "frontier",
        "release_date": "2026-02",
        "tas": 84.8,
        "open": 69.0,
        "forced": 98.1,
        "gap": 29.1,
        "ci": 0.943,
        "position_bias": 51.9,
        "consistency_pct": 81.5
      },
      ...
    ],
    "families": {
      "Google Gemma": {"color": "#34A853", "n_models": 5},
      "Google Gemini": {"color": "#4285F4", "n_models": 7},
      ...
    },
    "summary": {
      "n_models": 31,
      "tas_min": 75.6,
      "tas_max": 86.3,
      "open_min": 60.6,
      "open_max": 77.8,
      "forced_min": 80.2,
      "forced_max": 98.1
    }
  }

Usage:
    python -m pipeline.analysis.build_llm_site_data
"""

import json
from pathlib import Path

from pipeline.analysis.llm_spelling_test_v2 import MODELS, CHECKPOINT_DIR

ROOT = Path(__file__).resolve().parent.parent.parent
OUT_PATH = ROOT / "site" / "src" / "data" / "llm_trajectory.json"

# Hand-picked family colors (Tableau / matplotlib defaults variant)
FAMILY_COLORS = {
    "Google Gemini":    "#4285F4",   # Google blue
    "Google Gemma":     "#34A853",   # Google green
    "OpenAI GPT":       "#10A37F",   # OpenAI teal
    "Anthropic Claude": "#C97B47",   # Anthropic clay
    "xAI Grok":         "#000000",   # x black
    "Meta Llama":       "#0866FF",   # Meta blue
    "Alibaba Qwen":     "#9333EA",   # purple
    "Mistral":          "#FA500F",   # mistral orange
}


def short_label(key: str, family: str) -> str:
    """Make a concise human label for the chart legend."""
    # Strip family prefixes; the family color carries that info
    s = key
    s = s.replace("claude-", "")
    s = s.replace("gemini-", "")
    s = s.replace("gemma-", "g")
    s = s.replace("gpt-", "")
    s = s.replace("grok-", "")
    s = s.replace("llama", "l")
    return s


def main():
    if not CHECKPOINT_DIR.exists():
        print(f"no checkpoints in {CHECKPOINT_DIR}")
        return

    models_out = []
    families_seen = {}

    for cp_path in sorted(CHECKPOINT_DIR.glob("*.json")):
        with open(cp_path) as f:
            cp = json.load(f)
        key = cp["model"]
        info = MODELS.get(key, {})
        if not info:
            continue
        n_trials = sum(len(p["trials"]) for p in cp["pairs"])
        if n_trials < 162:
            continue
        s = cp.get("summary", {})
        if s.get("tas_mean") is None:
            continue
        family = info.get("family", "Unknown")
        forced_avg = None
        if s.get("ua_pct_forced_ru_first") is not None and s.get("ua_pct_forced_ua_first") is not None:
            forced_avg = round(
                (s["ua_pct_forced_ru_first"] + s["ua_pct_forced_ua_first"]) / 2, 1
            )
        gap = None
        if forced_avg is not None and s.get("ua_pct_open") is not None:
            gap = round(forced_avg - s["ua_pct_open"], 1)

        models_out.append({
            "key": key,
            "label": short_label(key, family),
            "family": family,
            "tier": info.get("tier", "?"),
            "release_date": info.get("release_date"),
            "tas": s["tas_mean"],
            "open": s.get("ua_pct_open"),
            "forced_ru_first": s.get("ua_pct_forced_ru_first"),
            "forced_ua_first": s.get("ua_pct_forced_ua_first"),
            "forced": forced_avg,
            "gap": gap,
            "ci": s.get("ci_mean"),
            "position_bias": s.get("position_bias"),
            "consistency_pct": s.get("consistency_pct"),
            "other_rate": s.get("other_rate"),
        })
        families_seen[family] = families_seen.get(family, 0) + 1

    # Sort by release_date then family then key for deterministic output
    models_out.sort(key=lambda m: (m["release_date"] or "", m["family"], m["key"]))

    families = {
        f: {"color": FAMILY_COLORS.get(f, "#888"), "n_models": n}
        for f, n in sorted(families_seen.items())
    }

    if models_out:
        summary = {
            "n_models": len(models_out),
            "tas_min": min(m["tas"] for m in models_out if m["tas"] is not None),
            "tas_max": max(m["tas"] for m in models_out if m["tas"] is not None),
            "open_min": min(m["open"] for m in models_out if m["open"] is not None),
            "open_max": max(m["open"] for m in models_out if m["open"] is not None),
            "forced_min": min(m["forced"] for m in models_out if m["forced"] is not None),
            "forced_max": max(m["forced"] for m in models_out if m["forced"] is not None),
            "gap_min": min(m["gap"] for m in models_out if m["gap"] is not None),
            "gap_max": max(m["gap"] for m in models_out if m["gap"] is not None),
            "release_min": min(m["release_date"] for m in models_out if m["release_date"]),
            "release_max": max(m["release_date"] for m in models_out if m["release_date"]),
        }
    else:
        summary = {}

    out = {
        "models": models_out,
        "families": families,
        "summary": summary,
    }

    OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    with open(OUT_PATH, "w") as f:
        json.dump(out, f, indent=2)
    print(f"wrote {OUT_PATH}")
    print(f"  {len(models_out)} completed models across {len(families)} families")
    if summary:
        print(f"  TAS range: {summary['tas_min']:.1f} - {summary['tas_max']:.1f}")
        print(f"  open recall range: {summary['open_min']:.1f} - {summary['open_max']:.1f}")
        print(f"  release range: {summary['release_min']} - {summary['release_max']}")


if __name__ == "__main__":
    main()
