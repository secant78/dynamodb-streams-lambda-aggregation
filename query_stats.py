"""
Assignment 13 – Step 4
Query demonstrations:
  1. Scan CustomerStats – show all real-time aggregations
  2. Query Orders by status using the GSI (status-timestamp-index)
  3. Show TTL values on sample orders
"""

import boto3
import time
from boto3.dynamodb.conditions import Key
from config import (
    REGION, ORDERS_TABLE, CUSTOMER_STATS_TABLE,
    STATUS_GSI_NAME, ORDER_STATUSES,
)

dynamo = boto3.resource("dynamodb", region_name=REGION)


def ok(msg):   print(f"  [OK]  {msg}")
def info(msg): print(f"  [..]  {msg}")
def step(msg): print(f"\n=== {msg} ===")


# ─── 1. CustomerStats scan ────────────────────────────────────────────────────

def show_customer_stats():
    step("CustomerStats – Aggregated totals per customer")
    table = dynamo.Table(CUSTOMER_STATS_TABLE)
    resp  = table.scan()
    items = resp.get("Items", [])

    if not items:
        info("No aggregates found – run seed_orders.py first")
        return

    grand_total  = 0
    grand_orders = 0

    print(f"\n  {'customerId':<15} {'orderCount':>12} {'totalAmount':>14} {'lastUpdated':<25}")
    print("  " + "-"*68)
    for item in sorted(items, key=lambda x: x["customerId"]):
        cid   = item["customerId"]
        cnt   = int(item.get("orderCount", 0))
        total = float(item.get("totalAmount", 0))
        ts    = item.get("lastUpdated", "")
        print(f"  {cid:<15} {cnt:>12} {total:>14.2f} {ts:<25}")
        grand_total  += total
        grand_orders += cnt

    print("  " + "-"*68)
    print(f"  {'GRAND TOTAL':<15} {grand_orders:>12} {grand_total:>14.2f}")


# ─── 2. GSI query by status ───────────────────────────────────────────────────

def query_by_status(status, limit=10):
    step(f"Orders GSI query – status = '{status}' (limit {limit})")
    table = dynamo.Table(ORDERS_TABLE)

    resp  = table.query(
        IndexName=STATUS_GSI_NAME,
        KeyConditionExpression=Key("status").eq(status),
        Limit=limit,
        ScanIndexForward=False,   # most-recent first
    )
    items = resp.get("Items", [])

    if not items:
        info(f"No orders found with status '{status}'")
        return

    print(f"\n  Found {resp['Count']} item(s) (showing up to {limit}):")
    print(f"  {'orderId':<38} {'customerId':<12} {'amount':>10} {'timestamp':<25}")
    print("  " + "-"*90)
    for item in items:
        oid   = item["orderId"]
        cid   = item["customerId"]
        amt   = float(item.get("amount", 0))
        ts    = item.get("timestamp", "")
        print(f"  {oid:<38} {cid:<12} {amt:>10.2f} {ts:<25}")


# ─── 3. TTL sample ────────────────────────────────────────────────────────────

def show_ttl_sample(count=5):
    step(f"Orders TTL sample – first {count} items")
    table = dynamo.Table(ORDERS_TABLE)
    resp  = table.scan(Limit=count, ProjectionExpression="orderId, #ts, #ttl",
                       ExpressionAttributeNames={"#ts": "timestamp", "#ttl": "ttl"})
    items = resp.get("Items", [])

    now = int(time.time())
    print(f"\n  {'orderId':<38} {'timestamp':<25} {'ttl (epoch)':>14} {'expires_in_days':>16}")
    print("  " + "-"*96)
    for item in items:
        oid = item["orderId"]
        ts  = item.get("timestamp", "")
        ttl = int(item.get("ttl", 0))
        days_left = (ttl - now) / 86400
        print(f"  {oid:<38} {ts:<25} {ttl:>14} {days_left:>15.1f}d")


# ─── main ─────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "="*60)
    print("  Assignment 13: Query Demonstrations")
    print("="*60)

    show_customer_stats()

    for status in ORDER_STATUSES:
        query_by_status(status, limit=5)

    show_ttl_sample(count=5)

    print("\n" + "="*60)
    print("  Query demonstrations COMPLETE")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
