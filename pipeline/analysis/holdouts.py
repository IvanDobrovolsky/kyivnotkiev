"""Holdout analysis: identify media outlets still using Russian-based spellings.

Produces a ranked list of domains/sources that continue to use outdated
spellings, broken down by pair and time period. This is the actionable
output for the MFA policy brief — a concrete list of who to contact.

Usage:
    python -m pipeline.analysis.holdouts [--pair-ids 1,3,10] [--min-mentions 10]
"""

import argparse
import logging

from google.cloud import bigquery

from pipeline.config import get_gcp_config, get_enabled_pairs

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-8s %(message)s")
log = logging.getLogger(__name__)


def get_holdout_domains(client: bigquery.Client, dataset: str, project: str,
                        pair_id: int = None, min_mentions: int = 10,
                        since: str = "2024-01-01") -> list[dict]:
    """Find domains that still predominantly use Russian-based spellings.

    Returns list of dicts with: domain, pair_id, russian_count, ukrainian_count,
    total, russian_pct, last_seen.
    """
    pair_filter = f"AND pair_id = {pair_id}" if pair_id else ""

    query = f"""
        WITH domain_counts AS (
            SELECT
                source_domain,
                pair_id,
                COUNTIF(variant = 'russian') AS russian_count,
                COUNTIF(variant = 'ukrainian') AS ukrainian_count,
                COUNT(*) AS total,
                MAX(date) AS last_seen
            FROM `{project}.{dataset}.raw_gdelt`
            WHERE date >= '{since}'
              {pair_filter}
            GROUP BY source_domain, pair_id
            HAVING total >= {min_mentions}
        )
        SELECT
            source_domain AS domain,
            pair_id,
            russian_count,
            ukrainian_count,
            total,
            ROUND(russian_count / total * 100, 1) AS russian_pct,
            last_seen
        FROM domain_counts
        WHERE russian_count > ukrainian_count  -- still majority Russian
        ORDER BY total DESC, russian_pct DESC
    """

    rows = list(client.query(query).result())
    return [dict(row) for row in rows]


def get_holdout_by_category(client: bigquery.Client, dataset: str, project: str,
                            since: str = "2024-01-01") -> list[dict]:
    """Aggregate holdout analysis by category."""
    query = f"""
        WITH pair_info AS (
            SELECT DISTINCT pair_id, matched_term
            FROM `{project}.{dataset}.raw_gdelt`
        ),
        domain_scores AS (
            SELECT
                g.source_domain,
                g.pair_id,
                COUNTIF(g.variant = 'russian') AS russian_count,
                COUNTIF(g.variant = 'ukrainian') AS ukrainian_count,
                COUNT(*) AS total
            FROM `{project}.{dataset}.raw_gdelt` g
            WHERE g.date >= '{since}'
            GROUP BY g.source_domain, g.pair_id
            HAVING total >= 5
        )
        SELECT
            source_domain AS domain,
            COUNT(DISTINCT pair_id) AS pairs_tracked,
            SUM(russian_count) AS total_russian,
            SUM(ukrainian_count) AS total_ukrainian,
            SUM(total) AS total_mentions,
            ROUND(SUM(russian_count) / SUM(total) * 100, 1) AS overall_russian_pct
        FROM domain_scores
        WHERE russian_count > ukrainian_count
        GROUP BY source_domain
        HAVING total_mentions >= 20
        ORDER BY total_mentions DESC
    """

    rows = list(client.query(query).result())
    return [dict(row) for row in rows]


def get_switched_domains(client: bigquery.Client, dataset: str, project: str,
                         pair_id: int = None, min_mentions: int = 10) -> list[dict]:
    """Find domains that successfully switched to Ukrainian spellings.
    Useful as positive examples for the policy brief.
    """
    pair_filter = f"AND pair_id = {pair_id}" if pair_id else ""

    query = f"""
        WITH before AS (
            SELECT
                source_domain,
                pair_id,
                COUNTIF(variant = 'russian') AS russian_count,
                COUNTIF(variant = 'ukrainian') AS ukrainian_count
            FROM `{project}.{dataset}.raw_gdelt`
            WHERE date BETWEEN '2018-01-01' AND '2019-06-01'
              {pair_filter}
            GROUP BY source_domain, pair_id
        ),
        after AS (
            SELECT
                source_domain,
                pair_id,
                COUNTIF(variant = 'russian') AS russian_count,
                COUNTIF(variant = 'ukrainian') AS ukrainian_count
            FROM `{project}.{dataset}.raw_gdelt`
            WHERE date >= '2023-01-01'
              {pair_filter}
            GROUP BY source_domain, pair_id
        )
        SELECT
            a.source_domain AS domain,
            a.pair_id,
            b.russian_count AS before_russian,
            b.ukrainian_count AS before_ukrainian,
            a.russian_count AS after_russian,
            a.ukrainian_count AS after_ukrainian,
            ROUND(b.russian_count / GREATEST(b.russian_count + b.ukrainian_count, 1) * 100, 1) AS before_russian_pct,
            ROUND(a.russian_count / GREATEST(a.russian_count + a.ukrainian_count, 1) * 100, 1) AS after_russian_pct
        FROM after a
        JOIN before b ON a.source_domain = b.source_domain AND a.pair_id = b.pair_id
        WHERE b.russian_count > b.ukrainian_count  -- was using Russian
          AND a.ukrainian_count > a.russian_count   -- now using Ukrainian
          AND (b.russian_count + b.ukrainian_count) >= {min_mentions}
          AND (a.russian_count + a.ukrainian_count) >= {min_mentions}
        ORDER BY (b.russian_count + b.ukrainian_count) DESC
    """

    rows = list(client.query(query).result())
    return [dict(row) for row in rows]


def generate_report(pair_ids: list[int] = None, min_mentions: int = 10,
                    since: str = "2024-01-01"):
    """Generate full holdout report and print to stdout."""
    cfg = get_gcp_config()
    client = bigquery.Client(project=cfg["project_id"])
    dataset = cfg["bigquery"]["dataset"]
    project = cfg["project_id"]

    pairs = get_enabled_pairs()
    pair_lookup = {p["id"]: p for p in pairs}

    # Overall holdouts
    log.info("Analyzing holdout domains...")
    holdouts = get_holdout_by_category(client, dataset, project, since)

    print("\n" + "=" * 80)
    print("HOLDOUT REPORT: Media still using Russian-based spellings")
    print(f"Period: {since} to present | Min mentions: {min_mentions}")
    print("=" * 80)

    if holdouts:
        print(f"\n{'Domain':<40} {'Pairs':<6} {'Russian%':<10} {'Total':<8}")
        print("-" * 70)
        for h in holdouts[:50]:
            print(f"{h['domain']:<40} {h['pairs_tracked']:<6} "
                  f"{h['overall_russian_pct']:<10} {h['total_mentions']:<8}")
    else:
        print("\nNo holdout data found. Run ingestion first.")

    # Per-pair holdouts
    if pair_ids:
        for pid in pair_ids:
            pair = pair_lookup.get(pid)
            if not pair:
                continue
            print(f"\n--- Pair {pid}: {pair['russian']} → {pair['ukrainian']} ---")
            pair_holdouts = get_holdout_domains(client, dataset, project, pid,
                                                min_mentions, since)
            for h in pair_holdouts[:20]:
                print(f"  {h['domain']:<35} {h['russian_pct']:>5}% Russian "
                      f"({h['total']} mentions, last: {h['last_seen']})")

    # Success stories
    log.info("Finding domains that successfully switched...")
    switched = get_switched_domains(client, dataset, project)
    if switched:
        print(f"\n{'=' * 80}")
        print("SUCCESS STORIES: Domains that switched to Ukrainian spellings")
        print("=" * 80)
        print(f"\n{'Domain':<35} {'Before':<12} {'After':<12}")
        print("-" * 60)
        for s in switched[:30]:
            print(f"{s['domain']:<35} {s['before_russian_pct']:>5}% → "
                  f"{s['after_russian_pct']:>5}%")


def main():
    parser = argparse.ArgumentParser(description="Holdout analysis — who still uses old spellings")
    parser.add_argument("--pair-ids", type=str, help="Comma-separated pair IDs")
    parser.add_argument("--min-mentions", type=int, default=10)
    parser.add_argument("--since", type=str, default="2024-01-01")
    args = parser.parse_args()

    pair_ids = [int(x) for x in args.pair_ids.split(",")] if args.pair_ids else None
    generate_report(pair_ids, args.min_mentions, args.since)


if __name__ == "__main__":
    main()
