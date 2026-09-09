"""Build the training corpus. Deterministic: same inputs, same bytes.

Implements docs/CORPUS_SPEC.md. One parquet per pair under
data/cl/corpus/training/, a _manifest.json with input SHA1s and counts,
and _imbalance.json with the pair x variant x source matrix and
inverse-frequency class weights.

    python -m pipeline.cl.corpus.build_training_corpus
"""

import hashlib
import json
import pathlib
import re

import pandas as pd
import yaml

ROOT = pathlib.Path(__file__).resolve().parent.parent.parent.parent
OUT = ROOT / "data" / "cl" / "corpus" / "training"

FW = {"de", "la", "el", "los", "las", "una", "del", "en", "que", "com", "por",
      "para", "contra", "tras", "durante", "les", "des", "dans", "sur", "der",
      "die", "das", "und", "di", "il", "della", "dei", "dopo", "gli", "um",
      "uma", "dos", "per", "non", "nessun", "alla", "dalla", "como", "hacia",
      "ve", "bir", "için", "ile", "daha", "sonra", "önce", "con"}
WRONG_REF = re.compile(r"\ba/kiev/\d|\bkiev(an)?\s+rus", re.I)


def sha1(p: pathlib.Path) -> str:
    h = hashlib.sha1()
    with open(p, "rb") as f:
        for c in iter(lambda: f.read(1 << 20), b""):
            h.update(c)
    return h.hexdigest()


def non_english(t: str) -> bool:
    toks = re.findall(r"[^\W\d_]+", str(t).lower(), re.UNICODE)
    fw = sum(1 for w in toks if w in FW)
    na = sum(1 for w in toks if any(ord(c) > 127 for c in w))
    return fw >= 2 or na >= 3 or (fw and na >= 2)


def main() -> int:
    cfg = yaml.safe_load(open(ROOT / "config" / "pairs.yaml"))
    pairs = [p for p in cfg["pairs"] if p.get("enabled", True)]
    mk = lambda t: re.compile(
        r"\b" + r"[\s\-]+".join(re.escape(w) for w in str(t).split()) + r"\b", re.I)
    OUT.mkdir(parents=True, exist_ok=True)

    inputs, matrix = {}, []
    reddit = pd.read_parquet(ROOT / "data" / "store" / "reddit_processed.parquet")
    inputs["reddit_processed.parquet"] = sha1(ROOT / "data" / "store" / "reddit_processed.parquet")
    oa = pd.read_parquet(ROOT / "data" / "cl" / "raw" / "openalex" / "all_pairs.parquet")
    inputs["openalex_all_pairs.parquet"] = sha1(ROOT / "data" / "cl" / "raw" / "openalex" / "all_pairs.parquet")

    for p in pairs:
        slug = p["slug"]
        rx_ua, rx_ru = mk(p["ukrainian"]), mk(p["russian"])
        homos = [re.compile(f, re.I) for f in
                 p.get("homonym_filters", []) + p.get("youtube_homonym_filters", [])]
        frames = []

        # news — already referent-filtered, deduped, verified
        nf = ROOT / "data" / "cl" / "corpus" / "gdelt_verified" / f"{slug}.parquet"
        if nf.exists():
            d = pd.read_parquet(nf, columns=["url", "date", "text", "variant"])
            frames.append(pd.DataFrame({
                "source": "gdelt", "doc_id": d.url.astype(str),
                "date": d.date.astype(str).str[:10],
                "text": d.text.astype(str), "variant": d.variant}))
            inputs[f"gdelt_verified/{slug}.parquet"] = sha1(nf)

        # youtube — verified rows, homonym-screened incl. channel
        yf = ROOT / "data" / "cl" / "raw" / "youtube_census" / f"{slug}_enriched.parquet"
        if yf.exists():
            y = pd.read_parquet(yf)
            if "verified" in y.columns:
                y = y[y["verified"]]
            if "span_artifact" in y.columns:
                y = y[~y["span_artifact"].fillna(False)]
            blob = (y.get("title", pd.Series("", index=y.index)).fillna("").astype(str) + " " +
                    y.get("description", pd.Series("", index=y.index)).fillna("").astype(str))
            full = blob + " " + y.get("channel", pd.Series("", index=y.index)).fillna("").astype(str)
            if homos:
                keep = ~full.map(lambda t: any(r.search(t) for r in homos))
                y, blob = y[keep], blob[keep]
            frames.append(pd.DataFrame({
                "source": "youtube", "doc_id": y.video_id.astype(str),
                "date": y.published_at.astype(str).str[:10],
                "text": blob,
                "variant": y["form"] if "form" in y.columns else y["variant"]}))
            inputs[f"youtube_census/{slug}_enriched.parquet"] = sha1(yf)

        # reddit — store is bot-filtered and backfilled
        r = reddit[reddit.pair_slug == slug]
        if len(r):
            frames.append(pd.DataFrame({
                "source": "reddit", "doc_id": r.doc_id.astype(str),
                "date": r.date.astype(str).str[:10],
                "text": r.text.astype(str), "variant": r.variant}))

        # openalex — wrong-referent guards apply
        o = oa[oa.matched_term.astype(str).str.strip().str.lower().isin(
            {str(p["ukrainian"]).lower(), str(p["russian"]).lower()})]
        if len(o):
            ot = (o.title.fillna("").astype(str) + ". " +
                  o.abstract.fillna("").astype(str))
            keep = ~o.title.astype(str).str.contains(WRONG_REF)
            frames.append(pd.DataFrame({
                "source": "openalex", "doc_id": o[keep].openalex_id.astype(str),
                "date": o[keep].year.fillna(0).astype(int).astype(str) + "-01-01",
                "text": ot[keep], "variant": o[keep].variant}))

        if not frames:
            continue
        df = pd.concat(frames, ignore_index=True)
        # labels from a standalone recount, never trusted metadata
        has_ua = df.text.map(lambda t: bool(rx_ua.search(t)))
        has_ru = df.text.map(lambda t: bool(rx_ru.search(t)))
        df["variant"] = "neither"
        df.loc[has_ua & ~has_ru, "variant"] = "ukrainian"
        df.loc[has_ru & ~has_ua, "variant"] = "russian"
        df.loc[has_ua & has_ru, "variant"] = "both"
        df = df[df.variant != "neither"]
        # English screen
        n0 = len(df)
        df = df[~df.text.map(non_english)]
        dropped_lang = n0 - len(df)
        df["pair_slug"] = slug
        df = df.sort_values(["source", "doc_id"]).reset_index(drop=True)
        df.to_parquet(OUT / f"{slug}.parquet", compression="zstd", index=False)
        for (src, var), n in df.groupby(["source", "variant"]).size().items():
            matrix.append({"pair": slug, "source": src, "variant": var, "n": int(n)})
        print(f"{slug:22s} {len(df):7,} rows  (dropped non-English: {dropped_lang:,})")

    m = pd.DataFrame(matrix)
    per_pair = m.groupby("pair")["n"].sum()
    per_var = m.groupby("variant")["n"].sum()
    weights = {v: round(float(per_var.sum() / (len(per_var) * n)), 4)
               for v, n in per_var.items()}
    json.dump({"matrix": matrix,
               "per_pair": {k: int(v) for k, v in per_pair.items()},
               "per_variant": {k: int(v) for k, v in per_var.items()},
               "inverse_freq_variant_weights": weights},
              open(OUT / "_imbalance.json", "w"), indent=1)
    json.dump({"inputs_sha1": inputs,
               "total_rows": int(per_pair.sum()),
               "pairs": len(per_pair)},
              open(OUT / "_manifest.json", "w"), indent=1)
    print(f"\nTOTAL {per_pair.sum():,} rows across {len(per_pair)} pairs")
    print("variant totals:", {k: int(v) for k, v in per_var.items()})
    print("inverse-frequency weights:", weights)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
