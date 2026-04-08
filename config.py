"""
Assignment 13: DynamoDB with Streams and Lambda Aggregation
Configuration constants shared across all scripts.
"""

ACCOUNT_ID = "866934333672"
REGION = "us-east-1"

SUFFIX = "sean-0303"

# DynamoDB tables
ORDERS_TABLE        = f"Orders-{SUFFIX}"
CUSTOMER_STATS_TABLE = f"CustomerStats-{SUFFIX}"

# Lambda functions
STREAM_PROCESSOR_FUNCTION = f"dynamodb-stream-processor-{SUFFIX}"
ORDER_INSERTER_FUNCTION   = f"order-inserter-{SUFFIX}"

# IAM
LAMBDA_ROLE_NAME   = f"dynamodb-lambda-role-{SUFFIX}"
LAMBDA_POLICY_NAME = f"dynamodb-lambda-policy-{SUFFIX}"

# DynamoDB settings
TTL_ATTRIBUTE = "ttl"
TTL_DAYS      = 7

# GSI
STATUS_GSI_NAME = "status-timestamp-index"

# Seed data
NUM_ORDERS     = 50
NUM_CUSTOMERS  = 10
ORDER_STATUSES = ["pending", "processing", "completed", "cancelled"]
