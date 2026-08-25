"""Regenerate the README's numbers from the site manifest.

The README hard-coded pair counts and per-source record counts, so it drifted
out of date every time a pair was enabled or disabled — it still claimed 56
pairs when the real number was 22. Everything between the AUTO markers is
generated from site/src/data/manifest.json, which is itself generated from
config/pairs.yaml, so the chain has a single source of truth.

Runs as the last step of pipeline.rebuild. Safe to run standalone:
    python -m pipeline.update_readme
"""

import json
import logging
import re
from pathlib import Path

log = logging.getLogger(__name__)

ROOT = Path(__file__).resolve().parent.parent
README = ROOT / "README.md"
SITE_DATA = ROOT / "site" / "src" / "data"


def _fmt(n) -> str:
    """Human-readable count: 20300000 -> 20.3M."""
    if isinstance(n, str):
        return n
    n = int(n)
    for div, suffix in ((1_000_000_000, "B"), (1_000_000, "M"), (1_000, "K")):
        if n >= div:
            return f"{n / div:.1f}".rstrip("0").rstrip(".") + suffix
    return str(n)


def _block(name: str, body: str, text: str) -> str:
    """Replace the content between <!-- AUTO:name --> markers."""
    pattern = re.compile(
        rf"<!-- AUTO:{name} -->.*?<!-- /AUTO:{name} -->",
        re.DOTALL,
    )
    if not pattern.search(text):
        log.warning(f"  README has no AUTO:{name} block — skipped")
        return text
    replacement = f"<!-- AUTO:{name} -->\n{body}\n<!-- /AUTO:{name} -->"
    return pattern.sub(lambda _: replacement, text)


def main() -> None:
    manifest = json.loads((SITE_DATA / "manifest.json").read_text())

    # No sampled false-positive rate is reported. Toponym matching is a
    # DETERMINISTIC phase: the requirement is zero false positives by
    # construction, not a precision estimate from a sample. A sampled "0.9% FP"
    # figure measures the wrong thing and was also stale. The old
    # regex_precision.json was removed for this reason.

    metrics = [
        ("Records scanned", f"**{manifest.get('records_scanned', '—')}**"),
        ("Toponym matches", f"**{_fmt(manifest.get('toponym_matches', 0))}**"),
        ("Toponym pairs", f"**{manifest.get('analyzable_pairs', 0)}**"),
        ("Data sources", f"**{manifest.get('num_sources', 0)}**"),
        ("Time span", f"**{manifest.get('time_span', '—')}**"),
    ]
    if manifest.get("cl_corpus"):
        metrics.append(("CL corpus", f"**{_fmt(manifest['cl_corpus'])}** verified English texts"))

    metrics_md = "\n".join(
        ["| Metric | Value |", "|--------|-------|"]
        + [f"| {k} | {v} |" for k, v in metrics]
    )

    rows = ["| Source | Records | Pairs | Description |", "|--------|---------|-------|-------------|"]
    for info in sorted(manifest.get("sources", {}).values(), key=lambda s: -int(s.get("records", 0))):
        rows.append(
            f"| {info.get('label', '?')} | {_fmt(info.get('records', 0))} | "
            f"{info.get('pairs', 0)} | {info.get('extra', '')} |"
        )
    sources_md = "\n".join(rows)

    text = README.read_text()
    text = _block("metrics", metrics_md, text)
    text = _block("sources", sources_md, text)
    README.write_text(text)

    log.info(f"  README updated: {manifest.get('analyzable_pairs')} pairs, "
             f"{len(manifest.get('sources', {}))} sources")


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    main()
