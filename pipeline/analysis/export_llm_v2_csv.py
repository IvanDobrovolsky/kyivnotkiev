"""Flatten v2 TAS checkpoints into long-format CSVs for analysis.

Produces two files in data/raw/llm_spelling/v2/:

  trials.csv     one row per (pair × model × test)
                 columns: pair_id, russian, ukrainian, category, model,
                          family, test, prompt, response, x

  pair_scores.csv  one row per (pair × model) with TAS components
                   columns: pair_id, russian, ukrainian, category, model,
                            family, x_forced_ru_first, x_forced_ua_first,
                            x_open, tas, ci

Usage:
    python -m pipeline.analysis.export_llm_v2_csv
"""

import csv
import json
from pathlib import Path

from pipeline.analysis.llm_spelling_test_v2 import (
    CHECKPOINT_DIR, OUT_DIR, DEFAULT_W1, DEFAULT_W2,
    TEST_FORCED_RU_FIRST, TEST_FORCED_UA_FIRST, TEST_OPEN,
    _per_pair_tas,
)


def main():
    if not CHECKPOINT_DIR.exists():
        print(f"no checkpoints in {CHECKPOINT_DIR}")
        return

    trials_rows = []
    pair_rows = []

    for cp_path in sorted(CHECKPOINT_DIR.glob("*.json")):
        with open(cp_path) as f:
            cp = json.load(f)
        model = cp["model"]
        family = cp.get("family", "")

        for pair in cp["pairs"]:
            by_test = {t["test"]: t for t in pair["trials"]}
            for test_name in (TEST_FORCED_RU_FIRST, TEST_FORCED_UA_FIRST, TEST_OPEN):
                t = by_test.get(test_name)
                if not t:
                    continue
                trials_rows.append({
                    "pair_id": pair["pair_id"],
                    "russian": pair["russian"],
                    "ukrainian": pair["ukrainian"],
                    "category": pair.get("category", ""),
                    "model": model,
                    "family": family,
                    "test": test_name,
                    "prompt": t["prompt"],
                    "response": t["response"],
                    "x": t.get("x"),
                })

            tas, ci, n_other = _per_pair_tas(by_test, DEFAULT_W1, DEFAULT_W2)
            pair_rows.append({
                "pair_id": pair["pair_id"],
                "russian": pair["russian"],
                "ukrainian": pair["ukrainian"],
                "category": pair.get("category", ""),
                "model": model,
                "family": family,
                "x_forced_ru_first": by_test.get(TEST_FORCED_RU_FIRST, {}).get("x"),
                "x_forced_ua_first": by_test.get(TEST_FORCED_UA_FIRST, {}).get("x"),
                "x_open": by_test.get(TEST_OPEN, {}).get("x"),
                "tas": tas,
                "ci": ci,
                "n_other": n_other,
            })

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    trials_path = OUT_DIR / "trials.csv"
    with open(trials_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(trials_rows[0].keys()))
        w.writeheader()
        w.writerows(trials_rows)
    print(f"wrote {trials_path} ({len(trials_rows)} rows)")

    pair_path = OUT_DIR / "pair_scores.csv"
    with open(pair_path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(pair_rows[0].keys()))
        w.writeheader()
        w.writerows(pair_rows)
    print(f"wrote {pair_path} ({len(pair_rows)} rows)")


if __name__ == "__main__":
    main()
