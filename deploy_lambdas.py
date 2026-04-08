"""
Assignment 13 – Step 2
Package and deploy both Lambda functions, create the IAM execution role,
and wire the DynamoDB stream trigger to the stream-processor function.
"""

import boto3
import json
import io
import os
import time
import zipfile
from config import (
    ACCOUNT_ID, REGION,
    ORDERS_TABLE, CUSTOMER_STATS_TABLE,
    STREAM_PROCESSOR_FUNCTION, ORDER_INSERTER_FUNCTION,
    LAMBDA_ROLE_NAME, LAMBDA_POLICY_NAME,
)

iam    = boto3.client("iam",    region_name=REGION)
lamb   = boto3.client("lambda", region_name=REGION)
dynamo = boto3.client("dynamodb", region_name=REGION)

HANDLER_DIR = os.path.dirname(os.path.abspath(__file__))


# ─── helpers ──────────────────────────────────────────────────────────────────

def ok(msg):   print(f"  [OK]  {msg}")
def info(msg): print(f"  [..]  {msg}")
def step(msg): print(f"\n=== {msg} ===")


def zip_handler(filename):
    """Return the in-memory zip bytes for a single handler .py file."""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        src_path = os.path.join(HANDLER_DIR, filename)
        zf.write(src_path, arcname="handler.py")
    return buf.getvalue()


# ─── IAM role ─────────────────────────────────────────────────────────────────

def create_lambda_role():
    step("Creating Lambda IAM Role")

    trust = {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"Service": "lambda.amazonaws.com"},
            "Action": "sts:AssumeRole",
        }],
    }

    try:
        resp     = iam.create_role(
            RoleName=LAMBDA_ROLE_NAME,
            AssumeRolePolicyDocument=json.dumps(trust),
            Description="Lambda execution role for Assignment 13",
        )
        role_arn = resp["Role"]["Arn"]
        ok(f"Role created: {role_arn}")
    except iam.exceptions.EntityAlreadyExistsException:
        role_arn = iam.get_role(RoleName=LAMBDA_ROLE_NAME)["Role"]["Arn"]
        ok(f"Role already exists: {role_arn}")

    policy = {
        "Version": "2012-10-17",
        "Statement": [
            # CloudWatch Logs
            {
                "Effect": "Allow",
                "Action": [
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                ],
                "Resource": "arn:aws:logs:*:*:*",
            },
            # Read DynamoDB stream
            {
                "Effect": "Allow",
                "Action": [
                    "dynamodb:GetRecords",
                    "dynamodb:GetShardIterator",
                    "dynamodb:DescribeStream",
                    "dynamodb:ListStreams",
                ],
                "Resource": f"arn:aws:dynamodb:{REGION}:{ACCOUNT_ID}:table/{ORDERS_TABLE}/stream/*",
            },
            # Write CustomerStats
            {
                "Effect": "Allow",
                "Action": [
                    "dynamodb:PutItem",
                    "dynamodb:UpdateItem",
                    "dynamodb:GetItem",
                ],
                "Resource": f"arn:aws:dynamodb:{REGION}:{ACCOUNT_ID}:table/{CUSTOMER_STATS_TABLE}",
            },
            # Write Orders (inserter)
            {
                "Effect": "Allow",
                "Action": [
                    "dynamodb:PutItem",
                    "dynamodb:BatchWriteItem",
                ],
                "Resource": f"arn:aws:dynamodb:{REGION}:{ACCOUNT_ID}:table/{ORDERS_TABLE}",
            },
        ],
    }

    try:
        iam.put_role_policy(
            RoleName=LAMBDA_ROLE_NAME,
            PolicyName=LAMBDA_POLICY_NAME,
            PolicyDocument=json.dumps(policy),
        )
        ok("Inline policy attached")
    except Exception as exc:
        print(f"  [WARN] Policy attachment: {exc}")

    info("Waiting 15s for IAM propagation...")
    time.sleep(15)
    return role_arn


# ─── Lambda deploy helpers ─────────────────────────────────────────────────────

def _deploy_function(name, handler_file, handler_entry, env_vars, role_arn):
    info(f"Packaging {handler_file}...")
    zip_bytes = zip_handler(handler_file)

    try:
        lamb.get_function(FunctionName=name)
        exists = True
    except lamb.exceptions.ResourceNotFoundException:
        exists = False

    if exists:
        lamb.update_function_code(FunctionName=name, ZipFile=zip_bytes)
        lamb.update_function_configuration(
            FunctionName=name,
            Environment={"Variables": env_vars},
        )
        ok(f"Updated Lambda: {name}")
    else:
        lamb.create_function(
            FunctionName=name,
            Runtime="python3.12",
            Role=role_arn,
            Handler=handler_entry,
            Code={"ZipFile": zip_bytes},
            Timeout=60,
            MemorySize=128,
            Environment={"Variables": env_vars},
        )
        ok(f"Created Lambda: {name}")

    # Wait until the function is active
    waiter = lamb.get_waiter("function_active_v2")
    waiter.wait(FunctionName=name)


# ─── Deploy both functions ────────────────────────────────────────────────────

def deploy_stream_processor(role_arn):
    step(f"Deploying Lambda: {STREAM_PROCESSOR_FUNCTION}")
    _deploy_function(
        name=STREAM_PROCESSOR_FUNCTION,
        handler_file="stream_processor_handler.py",
        handler_entry="handler.lambda_handler",
        env_vars={"CUSTOMER_STATS_TABLE": CUSTOMER_STATS_TABLE},
        role_arn=role_arn,
    )


def deploy_order_inserter(role_arn):
    step(f"Deploying Lambda: {ORDER_INSERTER_FUNCTION}")
    _deploy_function(
        name=ORDER_INSERTER_FUNCTION,
        handler_file="order_inserter_handler.py",
        handler_entry="handler.lambda_handler",
        env_vars={"ORDERS_TABLE": ORDERS_TABLE},
        role_arn=role_arn,
    )


# ─── Stream trigger ───────────────────────────────────────────────────────────

def wire_stream_trigger():
    step("Wiring DynamoDB Stream → stream-processor Lambda")

    desc       = dynamo.describe_table(TableName=ORDERS_TABLE)
    stream_arn = desc["Table"].get("LatestStreamArn")
    if not stream_arn:
        raise RuntimeError(f"No stream ARN found for {ORDERS_TABLE}. Enable streams first.")

    # Check if mapping already exists
    mappings = lamb.list_event_source_mappings(
        EventSourceArn=stream_arn,
        FunctionName=STREAM_PROCESSOR_FUNCTION,
    ).get("EventSourceMappings", [])

    if mappings:
        ok(f"Event source mapping already exists (UUID={mappings[0]['UUID']})")
        return mappings[0]["UUID"]

    resp = lamb.create_event_source_mapping(
        EventSourceArn=stream_arn,
        FunctionName=STREAM_PROCESSOR_FUNCTION,
        StartingPosition="LATEST",
        BatchSize=10,
        BisectBatchOnFunctionError=True,
    )
    ok(f"Event source mapping created (UUID={resp['UUID']})")
    return resp["UUID"]


# ─── main ─────────────────────────────────────────────────────────────────────

def main():
    print("\n" + "="*60)
    print("  Assignment 13: Lambda Deployment")
    print("="*60)

    role_arn = create_lambda_role()
    deploy_stream_processor(role_arn)
    deploy_order_inserter(role_arn)
    mapping_uuid = wire_stream_trigger()

    print("\n" + "="*60)
    print("  Lambda deployment COMPLETE")
    print(f"  Stream processor : {STREAM_PROCESSOR_FUNCTION}")
    print(f"  Order inserter   : {ORDER_INSERTER_FUNCTION}")
    print(f"  Stream mapping   : {mapping_uuid}")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()
