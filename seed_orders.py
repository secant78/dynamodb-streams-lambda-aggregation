"""
Assignment 13 – Step 3
Invoke the order-inserter Lambda to seed 50 random orders.
Waits a few seconds then polls CustomerStats to show aggregation in progress.
"""

import boto3
import json
import time
from config import (
    REGION, ORDER_INSERTER_FUNCTION, CUSTOMER_STATS_TABLE,
)

lamb    = boto3.client("lambda",   region_name=REGION)
dynamo  = boto3.resource("dynamodb", region_name=REGION)


def ok(msg):   print(f"  [OK]  {msg}")
def info(msg): print(f"  [..]  {msg}")
def step(msg): print(f"\n=== {msg} ===")


def invoke_order_inserter():
    step("Invoking order-inserter Lambda")

    resp = lamb.invoke(
        FunctionName=ORDER_INSERTER_FUNCTION,
        InvocationType="RequestResponse",
    )

    payload = json.loads(resp["Payload"].read())
    if resp.get("FunctionError"):
        print(f"  [ERR] Lambda error: {payload}")
        raise RuntimeError(payload)

    body = json.loads(payload.get("body", "{}"))
    ok(f"Lambda response: {body.get('message')}")
    return body.get("ordersInserted", 0)


def poll_customer_stats(rounds=3, delay=5):
    step("Polling CustomerStats (real-time aggregation)")
    table = dynamo.Table(CUSTOMER_STATS_TABLE)

    for r in range(1, rounds + 1):
        info(f"Poll {r}/{rounds} (waiting {delay}s for stream to process)...")
        time.sleep(delay)

        resp  = table.scan()
        items = resp.get("Items", [])
        if not items:
            info("No aggregates yet – stream may still be processing")
            continue

        print(f"\n  {'customerId':<15} {'orderCount':>12} {'totalAmount':>14} {'lastUpdated':<25}")
        print("  " + "-"*68)
        for item in sorted(items, key=lambda x: x["customerId"]):
            cid   = item.get("customerId", "")
            cnt   = item.get("orderCount", 0)
            total = item.get("totalAmount", 0)
            ts    = item.get("lastUpdated", "")
            print(f"  {cid:<15} {int(cnt):>12} {float(total):>14.2f} {ts:<25}")


def main():
    print("\n" + "="*60)
    print("  Assignment 13: Seed Orders & Watch Aggregation")
    print("="*60)

    inserted = invoke_order_inserter()
    print(f"\n  {inserted} orders written – stream processor should fire within seconds")

    poll_customer_stats(rounds=4, delay=6)

    print("\n" + "="*60)
    print("  Seeding COMPLETE")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
