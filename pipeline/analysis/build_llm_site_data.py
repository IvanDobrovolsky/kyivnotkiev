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
PER_PAIR_OUT = ROOT / "site" / "src" / "data" / "llm_per_pair.json"

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


def build_per_pair(checkpoints):
    """Pivot the per-model checkpoints into per-pair structure.

    Returns dict: {pair_id (str): {
        "russian": ..., "ukrainian": ..., "category": ...,
        "models": [
            {"key": "...", "family": "...", "open": 1|0|null,
             "forced_ru_first": ..., "forced_ua_first": ..., "tas": float},
            ...
        ],
        "summary": {
            "n_models": int,
            "tas_mean": float,
            "open_pct": float,    # share of models that picked UA in open
            "forced_pct": float,  # share that picked UA in forced (avg of two orders)
            "by_family": {family: {"open_pct": ..., "forced_pct": ..., "n": ...}}
        }
    }}
    """
    pairs = {}
    for cp_path, cp in checkpoints:
        model_key = cp["model"]
        info = MODELS.get(model_key, {})
        family = info.get("family", "Unknown")
        for pair in cp["pairs"]:
            pid = str(pair["pair_id"])
            entry = pairs.setdefault(pid, {
                "russian": pair["russian"],
                "ukrainian": pair["ukrainian"],
                "category": pair.get("category", ""),
                "models": [],
            })
            trials_by_test = {t["test"]: t for t in pair["trials"]}
            x_ru = trials_by_test.get("forced_ru_first", {}).get("x")
            x_ua = trials_by_test.get("forced_ua_first", {}).get("x")
            x_open = trials_by_test.get("open", {}).get("x")

            # Per-pair TAS using the same default weights as global
            xs_forced = [x for x in (x_ru, x_ua) if x is not None]
            forced_avg = sum(xs_forced) / len(xs_forced) if xs_forced else None
            if forced_avg is not None and x_open is not None:
                tas = 0.4 * forced_avg + 0.6 * x_open
            elif forced_avg is not None:
                tas = forced_avg
            elif x_open is not None:
                tas = float(x_open)
            else:
                tas = None

            entry["models"].append({
                "key": model_key,
                "family": family,
                "tier": info.get("tier", "?"),
                "release_date": info.get("release_date"),
                "x_open": x_open,
                "x_forced_ru_first": x_ru,
                "x_forced_ua_first": x_ua,
                "tas": round(tas, 3) if tas is not None else None,
            })

    # Add summary stats per pair
    for pid, entry in pairs.items():
        models = entry["models"]
        decisive_open = [m["x_open"] for m in models if m["x_open"] is not None]
        decisive_forced_ru = [m["x_forced_ru_first"] for m in models if m["x_forced_ru_first"] is not None]
        decisive_forced_ua = [m["x_forced_ua_first"] for m in models if m["x_forced_ua_first"] is not None]
        decisive_tas = [m["tas"] for m in models if m["tas"] is not None]

        # Family rollup
        by_family = {}
        for m in models:
            f = m["family"]
            slot = by_family.setdefault(f, {"open_ua": 0, "open_n": 0, "forced_ua": 0, "forced_n": 0, "tas_sum": 0, "tas_n": 0})
            if m["x_open"] is not None:
                slot["open_n"] += 1
                slot["open_ua"] += m["x_open"]
            for fv in (m["x_forced_ru_first"], m["x_forced_ua_first"]):
                if fv is not None:
                    slot["forced_n"] += 1
                    slot["forced_ua"] += fv
            if m["tas"] is not None:
                slot["tas_n"] += 1
                slot["tas_sum"] += m["tas"]
        family_summary = {}
        for f, s in by_family.items():
            family_summary[f] = {
                "n": len([m for m in models if m["family"] == f]),
                "open_pct": round(s["open_ua"] / s["open_n"] * 100, 1) if s["open_n"] else None,
                "forced_pct": round(s["forced_ua"] / s["forced_n"] * 100, 1) if s["forced_n"] else None,
                "tas_mean": round(s["tas_sum"] / s["tas_n"] * 100, 1) if s["tas_n"] else None,
            }

        entry["summary"] = {
            "n_models": len(models),
            "tas_mean": round(sum(decisive_tas) / len(decisive_tas) * 100, 1) if decisive_tas else None,
            "open_pct": round(sum(decisive_open) / len(decisive_open) * 100, 1) if decisive_open else None,
            "forced_ru_pct": round(sum(decisive_forced_ru) / len(decisive_forced_ru) * 100, 1) if decisive_forced_ru else None,
            "forced_ua_pct": round(sum(decisive_forced_ua) / len(decisive_forced_ua) * 100, 1) if decisive_forced_ua else None,
            "by_family": family_summary,
        }

    return pairs


def main():
    if not CHECKPOINT_DIR.exists():
        print(f"no checkpoints in {CHECKPOINT_DIR}")
        return

    models_out = []
    families_seen = {}
    completed_checkpoints = []  # for the per-pair pivot

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
        completed_checkpoints.append((cp_path, cp))
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

    # Per-pair pivot
    per_pair = build_per_pair(completed_checkpoints)
    per_pair_out = {
        "n_pairs": len(per_pair),
        "n_models": len(models_out),
        "families": families,
        "pairs": per_pair,
    }
    with open(PER_PAIR_OUT, "w") as f:
        json.dump(per_pair_out, f, indent=2)
    print(f"wrote {PER_PAIR_OUT}")
    print(f"  {len(per_pair)} pairs × {len(models_out)} models")


if __name__ == "__main__":
    main()
