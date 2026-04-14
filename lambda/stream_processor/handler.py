"""
Lambda handler: DynamoDB Stream Processor
Triggered by Orders table stream.
Aggregates total sales and order count per customer in CustomerStats.

Environment variables (injected at deploy time):
  CUSTOMER_STATS_TABLE – name of the CustomerStats DynamoDB table
"""

import boto3
import os
from decimal import Decimal

dynamodb = boto3.resource("dynamodb")


def lambda_handler(event, context):
    table_name = os.environ["CUSTOMER_STATS_TABLE"]
    stats_table = dynamodb.Table(table_name)

    processed = 0
    errors = []

    for record in event.get("Records", []):
        try:
            _process_record(record, stats_table)
            processed += 1
        except Exception as exc:
            errors.append(str(exc))
            print(f"[ERROR] record processing failed: {exc}")

    print(f"[INFO] Processed {processed}/{len(event['Records'])} records, errors={len(errors)}")
    return {"processed": processed, "errors": errors}


def _process_record(record, stats_table):
    event_name = record["eventName"]  # INSERT | MODIFY | REMOVE

    if event_name == "INSERT":
        new_img = record["dynamodb"]["NewImage"]
        customer_id = new_img["customerId"]["S"]
        amount      = Decimal(new_img["amount"]["N"])
        timestamp   = new_img.get("timestamp", {}).get("S", "")

        stats_table.update_item(
            Key={"customerId": customer_id},
            UpdateExpression=(
                "ADD totalAmount :amt, orderCount :one "
                "SET lastUpdated = :ts"
            ),
            ExpressionAttributeValues={
                ":amt": amount,
                ":one": 1,
                ":ts":  timestamp,
            },
        )
        print(f"[INSERT] customer={customer_id} amount={amount}")

    elif event_name == "MODIFY":
        old_img = record["dynamodb"]["OldImage"]
        new_img = record["dynamodb"]["NewImage"]
        customer_id = new_img["customerId"]["S"]
        old_amount  = Decimal(old_img["amount"]["N"])
        new_amount  = Decimal(new_img["amount"]["N"])
        delta       = new_amount - old_amount
        timestamp   = new_img.get("timestamp", {}).get("S", "")

        if delta != 0:
            stats_table.update_item(
                Key={"customerId": customer_id},
                UpdateExpression=(
                    "ADD totalAmount :delta "
                    "SET lastUpdated = :ts"
                ),
                ExpressionAttributeValues={
                    ":delta": delta,
                    ":ts":    timestamp,
                },
            )
            print(f"[MODIFY] customer={customer_id} delta={delta:+}")

    elif event_name == "REMOVE":
        old_img     = record["dynamodb"]["OldImage"]
        customer_id = old_img["customerId"]["S"]
        amount      = Decimal(old_img["amount"]["N"])

        stats_table.update_item(
            Key={"customerId": customer_id},
            UpdateExpression="ADD totalAmount :neg_amt, orderCount :neg_one",
            ExpressionAttributeValues={
                ":neg_amt":  -amount,
                ":neg_one": -1,
            },
        )
        print(f"[REMOVE] customer={customer_id} amount=-{amount}")
