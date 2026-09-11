"""The one place source filtering happens.

Every consumer — the verified news builder, the store's pair stacking, the
site's YouTube loader, the stats corpus, the training corpus — calls
apply_source_filters() instead of owning a private copy of the rules. A
filter that exists here exists everywhere; the week of "fixed on the site,
still on HuggingFace" bugs came from scattered per-consumer copies.

Rules, all config-driven from pairs.yaml per pair:

  homonym_filters           all sources; regex over title+text+channel blob
  youtube_homonym_filters   youtube rows only, same blob
  referent_filter           all text sources:
      evidence              row needs this regex (or a whitelisted domain)
      evidence_domains      domain whitelist counted as evidence
      drop                  unconditional drop patterns
      frozen                frozen-compound: drop when matching WITHOUT evidence
      require_evidence      with frozen: evidence is mandatory regardless

Deterministic: same input frame + same config = same output, always.
"""

from __future__ import annotations

import re

import pandas as pd
import yaml


_CFG_CACHE: dict | None = None
_VDROP_CACHE: dict | None = None

# Individually verified wrong entries (data/audit/holdout_ai_verification.json,
# produced by the per-pair verification agents). Each carries a recorded reason;
# they are dropped from EVERY layer — corpus, series, exhibits — because a row
# that names a person or a band was never evidence of a spelling choice. The
# reusable half of the same audit lands in pairs.yaml as regex patterns; this
# is the residue that no pattern generalises.
def _verified_drops(slug: str) -> set:
    global _VDROP_CACHE
    if _VDROP_CACHE is None:
        import json
        import pathlib
        _VDROP_CACHE = {}
        p = pathlib.Path("data/audit/holdout_ai_verification.json")
        if p.exists():
            try:
                for d in json.loads(p.read_text()).get("drops", []):
                    u = str(d.get("url") or "").strip()
                    if u:
                        _VDROP_CACHE.setdefault(d.get("pair"), set()).add(u)
            except Exception:                          # noqa: BLE001
                pass
    return _VDROP_CACHE.get(slug, set())


def _pair_cfg(slug: str) -> dict:
    global _CFG_CACHE
    if _CFG_CACHE is None:
        doc = yaml.safe_load(open("config/pairs.yaml"))
        _CFG_CACHE = {p["slug"]: p for p in doc["pairs"]}
    return _CFG_CACHE.get(slug, {})


def _blob(df: pd.DataFrame) -> pd.Series:
    parts = []
    for col in ("title", "text", "channel", "channel_title", "description"):
        if col in df.columns:
            parts.append(df[col].fillna("").astype(str))
    # The subreddit, and nothing else from the URL. Nineteen patterns across
    # pairs are written as "r/hellletloose" and matched nothing, because the
    # blob never carried it and reddit_processed has no subreddit column: r/tf2
    # is the largest subreddit in chicken-kyiv's reddit corpus (240 rows, ahead
    # of r/food) and its Team Fortress hat-trading rows all survived.
    # Only "r/<name>" is exposed — putting the whole URL in would let text
    # patterns match link slugs, which is the defect that had West Texas radio
    # stations counted as Odesa.
    if "url" in df.columns:
        _sub = df["url"].fillna("").astype(str).str.extract(
            r"reddit\.com/r/([\w-]+)", expand=False).fillna("")
        parts.append(_sub.where(_sub.eq(""), "r/" + _sub))
    if not parts:
        return pd.Series("", index=df.index)
    out = parts[0]
    for p in parts[1:]:
        # " | " separator: a plain space created phantom cross-field matches
        # ("...Kiev" + "19.05..." read as the Kiev-19 camera at the seam)
        out = out + " | " + p
    return out


def apply_source_filters(df: pd.DataFrame, slug: str, source: str,
                         audit: dict | None = None) -> pd.DataFrame:
    """Filter one pair's rows for one source. Returns the surviving frame."""
    if not len(df):
        return df
    cfg = _pair_cfg(slug)
    blob = _blob(df)
    note = (lambda k, n: audit.__setitem__(k, audit.get(k, 0) + int(n))
            if audit is not None else None)

    vdrop = _verified_drops(slug)
    if vdrop:
        # Match on the URL and on the bare identifier: exhibit URLs are built
        # at export time (youtube.com/watch?v=ID, reddit.com/r/x/comments/ID)
        # while the frames carry video_id / post_id / openalex_id, so a
        # URL-only comparison silently missed 355 YouTube rows.
        # Only platform-style identifiers, never news slugs: a bare
        # rsplit('/') on an article URL yields its headline slug, which can
        # collide with an unrelated row and delete real evidence.
        ids = set()
        for u in vdrop:
            if "youtube.com" in u or "youtu.be" in u:
                ids.add(u.rsplit("v=", 1)[-1].split("&")[0].rsplit("/", 1)[-1])
            elif "reddit.com" in u:
                ids.add(u.rstrip("/").rsplit("/", 1)[-1])
            elif "openalex.org" in u:
                ids.add(u.rstrip("/").rsplit("/", 1)[-1])
        for col in ("url", "doc_id", "video_id", "post_id", "openalex_id"):
            if col in df.columns:
                vals = df[col].astype(str)
                hit = vals.isin(vdrop) | vals.isin(ids)
                if hit.any():
                    note("dropped_verified_wrong", hit.sum())
                    df, blob = df[~hit], blob[~hit]

    pats = list(cfg.get("homonym_filters", []))
    if source == "youtube":
        pats += cfg.get("youtube_homonym_filters", [])
    if pats:
        rx = re.compile("|".join(pats), re.I)
        hit = blob.str.contains(rx)
        note("dropped_homonym", hit.sum())
        df, blob = df[~hit], blob[~hit]

    rf = cfg.get("referent_filter")
    if rf and len(df):
        text = blob
        # Evidence-class rules (evidence / frozen / require_evidence) are
        # validated on text-rich sources; short video metadata cannot carry
        # gazetteer evidence, so applying them to youtube would drop tens of
        # thousands of genuine videos (measured: odesa 43K). Explicit `drop`
        # patterns apply to every source regardless.
        _ev_sources = set(rf.get("evidence_sources", ["gdelt", "openalex", "reddit"]))
        _ev_applies = source in _ev_sources
        if rf.get("evidence"):
            ev = text.str.contains(rf["evidence"], case=False, regex=True)
        else:
            ev = pd.Series(True, index=df.index)
        if rf.get("evidence_domains") and "url" in df.columns:
            dom = df["url"].astype(str).str.extract(
                r"https?://(?:www\.)?([^/]+)")[0].fillna("")
            ev = ev | dom.str.contains(rf["evidence_domains"], case=False,
                                       regex=True)
        if rf.get("drop"):
            dr = text.str.contains("|".join(rf["drop"]), case=False, regex=True)
            note("dropped_referent_drop", dr.sum())
            df, text, ev = df[~dr], text[~dr], ev[~dr]
        if not _ev_applies:
            return df
        if rf.get("frozen"):
            drop = text.str.contains(rf["frozen"], case=False, regex=True) & ~ev
            note("dropped_frozen_compound", drop.sum())
            if rf.get("require_evidence"):
                drop = drop | ~ev
                note("dropped_no_referent", (~ev).sum())
        else:
            drop = ~ev
            note("dropped_no_referent", drop.sum())
        df = df[~drop]

    return df
