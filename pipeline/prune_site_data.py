"""Drop disabled pairs from every site JSON.

export_site_data.py filters on `enabled` for the files it writes, but several
site JSON files are produced by other pipeline steps (CL clustering, LLM audit,
dictionary, wiki redirects, pair reports) that do not. Disabling a pair in
config/pairs.yaml therefore left its data bundled into the client even though
no page rendered for it.

This runs as the last step of the site export so that config/pairs.yaml is the
single source of truth, and re-running the export is all that is needed after
enabling or disabling a pair.

Only slugs that are known pairs AND currently disabled are removed — arbitrary
keys are never touched.
"""

import json
import logging
from pathlib import Path

import yaml

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
CONFIG_PATH = ROOT / "config" / "pairs.yaml"
SITE_DATA_DIR = ROOT / "site" / "src" / "data"


def disabled_slugs(config_path: Path = CONFIG_PATH) -> set[str]:
    with open(config_path) as f:
        cfg = yaml.safe_load(f)
    return {p["slug"] for p in cfg["pairs"] if not p.get("enabled", True)}


def _prune(node, drop: set[str], removed: list[str]):
    """Recursively strip dict entries keyed by a disabled slug."""
    if isinstance(node, dict):
        for key in [k for k in node if isinstance(k, str) and k in drop]:
            removed.append(key)
            del node[key]
        for value in node.values():
            _prune(value, drop, removed)
        # keep any sibling counter honest after pruning
        if isinstance(node.get("pairs"), dict) and "n_pairs" in node:
            node["n_pairs"] = len(node["pairs"])
    elif isinstance(node, list):
        # drop list items that describe a disabled pair
        for i in [i for i, v in enumerate(node)
                  if isinstance(v, dict)
                  and (v.get("slug") in drop or v.get("pair_slug") in drop)][::-1]:
            removed.append(node[i].get("slug") or node[i].get("pair_slug"))
            del node[i]
        for value in node:
            _prune(value, drop, removed)


def main(config_path: Path = CONFIG_PATH, data_dir: Path = SITE_DATA_DIR) -> dict:
    drop = disabled_slugs(config_path)
    if not drop:
        log.info("No disabled pairs — nothing to prune")
        return {}

    log.info(f"Pruning {len(drop)} disabled pairs from site JSON: {sorted(drop)}")
    summary, saved = {}, 0

    for path in sorted(data_dir.glob("*.json")):
        try:
            with open(path) as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as e:
            log.warning(f"  {path.name}: unreadable ({e}) — skipped")
            continue

        removed: list[str] = []
        _prune(data, drop, removed)
        if not removed:
            continue

        before = path.stat().st_size
        with open(path, "w") as f:
            json.dump(data, f, separators=(",", ":"))
        after = path.stat().st_size
        saved += before - after
        summary[path.name] = sorted(set(removed))
        log.info(f"  {path.name}: removed {len(set(removed))} pairs, "
                 f"{(before - after) / 1024:.0f} KB freed")

    if summary:
        log.info(f"Pruned {len(summary)} files, {saved / 1024 / 1024:.1f} MB freed")
    else:
        log.info("Site JSON already clean")
    return summary


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
