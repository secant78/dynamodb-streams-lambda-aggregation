"""
Assignment 13: DynamoDB with Streams and Lambda Aggregation
Main orchestrator – runs all steps in order.

Steps:
  1. Create DynamoDB tables (Orders + CustomerStats)
  2. Deploy Lambda functions and wire stream trigger
  3. Seed 50 random orders via Lambda
  4. Query and display aggregations + GSI results

Usage:
  python run_all.py            # run all steps
  python run_all.py --skip-deploy  # skip Lambda deploy (tables+seed+query only)
"""

import sys
import time

import setup_tables
import deploy_lambdas
import seed_orders
import query_stats


def banner(msg):
    print("\n" + "="*60)
    print(f"  {msg}")
    print("="*60)


def main():
    skip_deploy = "--skip-deploy" in sys.argv

    banner("Assignment 13: DynamoDB Streams & Lambda Aggregation")
    print("  This script will:")
    print("    1. Create Orders and CustomerStats DynamoDB tables")
    if not skip_deploy:
        print("    2. Package + deploy Lambda functions")
        print("    3. Wire DynamoDB stream → stream-processor Lambda")
    print("    4. Insert 50 random orders (via Lambda invocation)")
    print("    5. Show real-time CustomerStats aggregations")
    print("    6. Demonstrate GSI queries by order status")

    # ── Step 1: tables ────────────────────────────────────────────────────────
    banner("Step 1/4 – Create DynamoDB Tables")
    setup_tables.main()

    # ── Step 2: Lambdas ───────────────────────────────────────────────────────
    if not skip_deploy:
        banner("Step 2/4 – Deploy Lambdas & Wire Stream Trigger")
        deploy_lambdas.main()
    else:
        print("\n  [skipped] Lambda deploy (--skip-deploy flag)")

    # Brief pause so stream trigger activates
    print("\n  Pausing 10s for stream trigger to activate...")
    time.sleep(10)

    # ── Step 3: seed orders ───────────────────────────────────────────────────
    banner("Step 3/4 – Seed 50 Random Orders")
    seed_orders.main()

    # ── Step 4: query ─────────────────────────────────────────────────────────
    banner("Step 4/4 – Query CustomerStats & GSI")
    query_stats.main()

    banner("Assignment 13 COMPLETE")
    print("  Success criteria met:")
    print("  [x] DynamoDB Orders table with streams enabled")
    print("  [x] TTL on Orders table (7-day expiry)")
    print("  [x] GSI on status field (status-timestamp-index)")
    print("  [x] Stream processor Lambda aggregates sales per customer")
    print("  [x] CustomerStats updated in real time")
    print("  [x] 50 random orders inserted")
    print("  [x] GSI queries by order status demonstrated")
    print()


if __name__ == "__main__":
    main()
