"""
Lambda handler: Order Inserter
Inserts 50 random orders into the Orders table.
Each order includes a TTL attribute set 7 days from now.

Environment variables (injected at deploy time):
  ORDERS_TABLE – name of the Orders DynamoDB table
"""

import boto3
import json
import os
import random
import time
import uuid
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")

TTL_SECONDS = 7 * 24 * 60 * 60   # 7 days
NUM_ORDERS  = 50
CUSTOMER_IDS = [f"CUST-{i:04d}" for i in range(1, 11)]   # 10 customers
STATUSES     = ["pending", "processing", "completed", "cancelled"]


def lambda_handler(event, context):
    table_name = os.environ["ORDERS_TABLE"]
    table      = dynamodb.Table(table_name)
    now        = int(time.time())

    inserted = 0
    # batch_writer handles chunking into groups of ≤25 (DynamoDB limit)
    with table.batch_writer() as batch:
        for _ in range(NUM_ORDERS):
            order_time = now - random.randint(0, 86400)   # within last 24 h
            ts_str     = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(order_time))
            amount     = round(random.uniform(10.00, 500.00), 2)

            batch.put_item(Item={
                "orderId":    str(uuid.uuid4()),
                "customerId": random.choice(CUSTOMER_IDS),
                "amount":     Decimal(str(amount)),
                "status":     random.choice(STATUSES),
                "timestamp":  ts_str,
                "ttl":        now + TTL_SECONDS,
            })
            inserted += 1

    print(f"[INFO] Inserted {inserted} orders into {table_name}")
    return {
        "statusCode": 200,
        "body": json.dumps({
            "message":        f"Inserted {inserted} orders",
            "ordersInserted": inserted,
        }),
    }
