"""Scrape public Telegram channels for toponym adoption analysis.

Uses Telethon to access full channel history of public English-language
channels that discuss Ukraine. Matches messages against pair terms.

Usage:
    python -m pipeline.ingestion.telegram_channels
"""

import asyncio
import json
import logging
import re
import yaml
from pathlib import Path
from datetime import datetime

import pandas as pd

from pipeline.config import ROOT_DIR

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger(__name__)

OUT_DIR = ROOT_DIR / "data" / "cl" / "raw" / "telegram"

# Public channels — all languages, pair matching is Latin-script only
CHANNELS = [
    # English-language Ukrainian news
    "KyivIndependent",
    "UkraineNow",
    # International news
    "BBCWorld",
    "CNN",
    "guardian",
    # War reporting / OSINT (English)
    "operativnoZSU",
    "DeepStateUA",
    "GeneralStaffZSU",
    "militaryland",
    "ukraineweapons",
    "DefMon3",
    "wartranslated",
    "NOELreports",
    "war_monitor",
    "nexta_live",
    "nexta_tv",
    # Official Ukraine government
    "ZelenskiyOfficial",
    "USEmbassyKyiv",
    "UkrainianLandForces",
    "Ukrainian_Navy",
    # Ukrainian media (mixed language)
    "Ukrinform_News",
    "suspilnenews",
    "TCH_channel",
    "ukrainaonlajn",
    # Analytics / think tanks
    "ISWresearch",
    "UnderstandingWar",
    "UkraineWorld",
    # Regional
    "KharkivLifeEng",
    "OdessaJournal",
    # Ukrainian government/parliament
    "V_Zelenskyy",
    "ukrainenowenglish",
    "EuromaidanPR",
]

# Load pair patterns
with open(ROOT_DIR / "config" / "pairs.yaml") as f:
    _cfg = yaml.safe_load(f)

PAIRS = []
for p in _cfg["pairs"]:
    if not p.get("enabled") or p.get("is_control"):
        continue
    PAIRS.append({
        "id": p["id"],
        "russian": p["russian"],
        "ukrainian": p["ukrainian"],
        "ru_re": re.compile(r"\b" + re.escape(p["russian"]) + r"\b", re.IGNORECASE),
        "ua_re": re.compile(r"\b" + re.escape(p["ukrainian"]) + r"\b", re.IGNORECASE),
    })


def match_text(text):
    """Match text against all pair patterns."""
    matches = []
    for p in PAIRS:
        if p["ru_re"].search(text):
            matches.append((p["id"], "russian", p["russian"]))
        if p["ua_re"].search(text):
            matches.append((p["id"], "ukrainian", p["ukrainian"]))
    return matches


async def scrape_channels():
    from telethon import TelegramClient

    creds = Path.home() / ".telegram_creds"
    api_id = int((creds / "api_id").read_text().strip())
    api_hash = (creds / "api_hash").read_text().strip()

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    session_path = str(OUT_DIR / "session")

    client = TelegramClient(session_path, api_id, api_hash)
    await client.start()

    log.info(f"Connected to Telegram. Scraping {len(CHANNELS)} channels...")

    results = []
    for channel_name in CHANNELS:
        try:
            entity = await client.get_entity(channel_name)
            log.info(f"  @{channel_name}: {getattr(entity, 'title', channel_name)}")

            count = 0
            async for message in client.iter_messages(entity, limit=10000):
                if not message.text:
                    continue

                text = message.text
                if len(text) < 10:
                    continue

                # Match Latin-script pair terms in any language text
                matches = match_text(text)
                for pair_id, variant, term in matches:
                    results.append({
                        "pair_id": pair_id,
                        "text": text[:3000],
                        "source": "telegram",
                        "variant": variant,
                        "matched_term": term,
                        "channel": channel_name,
                        "date": message.date.isoformat() if message.date else "",
                        "views": message.views or 0,
                    })
                    count += 1

            log.info(f"    → {count} matched messages")

        except Exception as e:
            log.warning(f"  @{channel_name}: ERROR — {e}")

    await client.disconnect()

    if results:
        df = pd.DataFrame(results)
        df = df.drop_duplicates(subset=["pair_id", "text"])
        out = OUT_DIR / "all_channels.parquet"
        df.to_parquet(out, index=False)
        log.info(f"\nSaved: {out} ({len(df):,} messages, {df['pair_id'].nunique()} pairs)")
        log.info(f"Channels: {df['channel'].nunique()}")
        log.info(f"Variants: {df['variant'].value_counts().to_dict()}")
        log.info(f"Top pairs: {df.groupby('pair_id').size().sort_values(ascending=False).head(5).to_dict()}")
    else:
        log.info("No matches found")

    return results


def main():
    asyncio.run(scrape_channels())


if __name__ == "__main__":
    main()
