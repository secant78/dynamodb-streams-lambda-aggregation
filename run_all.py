"""
Post-deployment utility: seed orders and query aggregated stats.

Prerequisites: run `npx cdk deploy` from the cdk/ directory first.

Usage:
  python run_all.py          # seed + query
  python run_all.py --query  # query only (skip seeding)
"""

import sys
import time

import seed_orders
import query_stats


def banner(msg):
    print("\n" + "="*60)
    print(f"  {msg}")
    print("="*60)


def main():
    query_only = "--query" in sys.argv

    banner("DynamoDB Streams & Lambda Aggregation – Post-Deploy")

    if not query_only:
        banner("Step 1/2 – Seed 50 Random Orders")
        seed_orders.main()

        print("\n  Waiting 15s for stream processor to aggregate...")
        time.sleep(15)
    else:
        print("\n  [skipped] Seeding (--query flag)")

    banner("Step 2/2 – Query CustomerStats & GSI")
    query_stats.main()

    banner("Done")


if __name__ == "__main__":
    main()
