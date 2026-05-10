"""
YouTube Transcript Fetcher — run in Google Colab or any machine with a clean IP.

Fetches English transcripts for all YouTube videos in the KyivNotKiev dataset.
Downloads video IDs from HuggingFace, fetches transcripts, saves to parquet.

Usage in Colab:
    !pip install youtube-transcript-api huggingface_hub pandas pyarrow
    !python fetch_youtube_transcripts.py

Downloads result: youtube_transcripts.parquet
Upload to HF or download locally when done.
"""

import json
import os
import time
import logging
from pathlib import Path

import pandas as pd

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)


def load_video_ids():
    """Load YouTube video IDs from HuggingFace dataset."""
    from huggingface_hub import hf_hub_download

    path = hf_hub_download(
        repo_id="KyivNotKiev/toponym-adoption-data",
        filename="data/raw_youtube.parquet",
        repo_type="dataset",
    )
    df = pd.read_parquet(path)
    # Deduplicate and keep pair mapping
    vids = df[["video_id", "pair_id"]].drop_duplicates(subset=["video_id"])
    log.info(f"Loaded {len(vids):,} unique video IDs from HuggingFace")
    return vids


def fetch_transcript(vid, languages=("en",)):
    """Fetch English transcript for a single video."""
    from youtube_transcript_api import YouTubeTranscriptApi

    ytt = YouTubeTranscriptApi()
    try:
        t = ytt.fetch(vid, languages=list(languages))
        text = " ".join(s.text for s in t)
        if len(text) > 20:
            return text[:10000], len(text)
    except Exception:
        pass
    return None, 0


def main():
    vids = load_video_ids()
    video_ids = vids["video_id"].tolist()
    vid_to_pair = dict(zip(vids["video_id"], vids["pair_id"]))

    checkpoint_path = "yt_transcript_checkpoint.json"
    out_path = "youtube_transcripts.parquet"

    # Resume from checkpoint
    done = set()
    if os.path.exists(checkpoint_path):
        with open(checkpoint_path) as f:
            done = set(json.load(f))
        log.info(f"Resuming: {len(done):,} already done")

    remaining = [v for v in video_ids if v not in done]
    log.info(f"Remaining: {len(remaining):,}")

    results = []
    # Load existing results
    if os.path.exists(out_path):
        prev = pd.read_parquet(out_path)
        results = prev.to_dict("records")
        log.info(f"Loaded {len(results):,} existing transcripts")

    no_subs = 0
    errors = 0

    for i, vid in enumerate(remaining):
        text, text_len = fetch_transcript(vid)

        done.add(vid)
        if text:
            results.append({
                "video_id": vid,
                "pair_id": vid_to_pair.get(vid, -1),
                "transcript": text,
                "transcript_len": text_len,
            })
        else:
            no_subs += 1

        # Progress + checkpoint every 200
        if (i + 1) % 200 == 0:
            n_transcripts = len(results)
            rate = n_transcripts / (i + 1) * 100 if i > 0 else 0
            log.info(
                f"  {i + 1:,}/{len(remaining):,} done, "
                f"{n_transcripts:,} transcripts ({rate:.0f}%), "
                f"{no_subs:,} no subs, {errors:,} errors"
            )
            with open(checkpoint_path, "w") as f:
                json.dump(list(done), f)
            pd.DataFrame(results).to_parquet(out_path, index=False)

        # Throttle: 1 request per second to avoid IP ban
        time.sleep(1)

    # Final save
    log.info(f"\nDone: {len(results):,} transcripts, {no_subs:,} no subs")
    if results:
        df = pd.DataFrame(results)
        df.to_parquet(out_path, index=False)
        log.info(f"Saved: {out_path} ({len(df):,} rows)")
        log.info(f"Median length: {df['transcript_len'].median():.0f} chars")

    with open(checkpoint_path, "w") as f:
        json.dump(list(done), f)


if __name__ == "__main__":
    main()
