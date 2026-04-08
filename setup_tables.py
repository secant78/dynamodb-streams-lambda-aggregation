"""
Assignment 13 – Step 1
Create DynamoDB tables:
  Orders        – PK orderId, GSI on status+timestamp, Streams enabled, TTL
  CustomerStats – PK customerId
"""

import boto3
import time
from config import (
    REGION, ORDERS_TABLE, CUSTOMER_STATS_TABLE,
    STATUS_GSI_NAME, TTL_ATTRIBUTE,
)

dynamodb = boto3.client("dynamodb", region_name=REGION)


# ─── helpers ──────────────────────────────────────────────────────────────────

def ok(msg):   print(f"  [OK]  {msg}")
def info(msg): print(f"  [..]  {msg}")
def step(msg): print(f"\n=== {msg} ===")


def table_exists(table_name):
    try:
        dynamodb.describe_table(TableName=table_name)
        return True
    except dynamodb.exceptions.ResourceNotFoundException:
        return False


def wait_for_table(table_name):
    info(f"Waiting for {table_name} to become ACTIVE...")
    waiter = dynamodb.get_waiter("table_exists")
    waiter.wait(TableName=table_name, WaiterConfig={"Delay": 5, "MaxAttempts": 20})
    ok(f"{table_name} is ACTIVE")


# ─── Orders table ─────────────────────────────────────────────────────────────

def create_orders_table():
    step(f"Creating table: {ORDERS_TABLE}")

    if table_exists(ORDERS_TABLE):
        ok(f"{ORDERS_TABLE} already exists – skipping creation")
        return get_stream_arn()

    resp = dynamodb.create_table(
        TableName=ORDERS_TABLE,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "orderId",   "AttributeType": "S"},
            {"AttributeName": "status",    "AttributeType": "S"},
            {"AttributeName": "timestamp", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "orderId", "KeyType": "HASH"},
        ],
        GlobalSecondaryIndexes=[
            {
                "IndexName": STATUS_GSI_NAME,
                "KeySchema": [
                    {"AttributeName": "status",    "KeyType": "HASH"},
                    {"AttributeName": "timestamp", "KeyType": "RANGE"},
                ],
                "Projection": {"ProjectionType": "ALL"},
            }
        ],
        StreamSpecification={
            "StreamEnabled": True,
            "StreamViewType": "NEW_AND_OLD_IMAGES",
        },
    )

    ok(f"Table {ORDERS_TABLE} created")
    wait_for_table(ORDERS_TABLE)

    # Enable TTL
    dynamodb.update_time_to_live(
        TableName=ORDERS_TABLE,
        TimeToLiveSpecification={
            "Enabled": True,
            "AttributeName": TTL_ATTRIBUTE,
        },
    )
    ok(f"TTL enabled on attribute '{TTL_ATTRIBUTE}' ({7}-day expiry)")

    return get_stream_arn()


def get_stream_arn():
    desc = dynamodb.describe_table(TableName=ORDERS_TABLE)
    stream_arn = desc["Table"].get("LatestStreamArn")
    ok(f"Stream ARN: {stream_arn}")
    return stream_arn


# ─── CustomerStats table ──────────────────────────────────────────────────────

def create_customer_stats_table():
    step(f"Creating table: {CUSTOMER_STATS_TABLE}")

    if table_exists(CUSTOMER_STATS_TABLE):
        ok(f"{CUSTOMER_STATS_TABLE} already exists – skipping creation")
        return

    dynamodb.create_table(
        TableName=CUSTOMER_STATS_TABLE,
        BillingMode="PAY_PER_REQUEST",
        AttributeDefinitions=[
            {"AttributeName": "customerId", "AttributeType": "S"},
        ],
        KeySchema=[
            {"AttributeName": "customerId", "KeyType": "HASH"},
        ],
    )

    ok(f"Table {CUSTOMER_STATS_TABLE} created")
    wait_for_table(CUSTOMER_STATS_TABLE)


# ─── main ─────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "="*60)
    print("  Assignment 13: DynamoDB Table Setup")
    print("="*60)

    stream_arn = create_orders_table()
    create_customer_stats_table()

    print("\n" + "="*60)
    print("  Table setup COMPLETE")
    print(f"  Orders table     : {ORDERS_TABLE}")
    print(f"  CustomerStats    : {CUSTOMER_STATS_TABLE}")
    print(f"  Stream ARN       : {stream_arn}")
    print(f"  GSI              : {STATUS_GSI_NAME}")
    print(f"  TTL attribute    : {TTL_ATTRIBUTE}")
    print("="*60 + "\n")

    return stream_arn


if __name__ == "__main__":
    main()
