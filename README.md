# DynamoDB Streams & Lambda Aggregation

Real-time order aggregation pipeline using DynamoDB Streams and AWS Lambda. Orders written to a DynamoDB table trigger a stream processor that continuously aggregates per-customer totals into a `CustomerStats` table — no polling required.

All AWS resources are provisioned with **AWS CDK** (Infrastructure as Code).

---

## Architecture

```
┌─────────────────────┐        ┌──────────────────────────────────┐
│  order-inserter     │        │  Orders Table                    │
│  Lambda             │──────▶ │  PK: orderId                     │
│                     │ write  │  GSI: status-timestamp-index     │
└─────────────────────┘        │  Stream: NEW_AND_OLD_IMAGES      │
                                │  TTL: 7 days                     │
                                └──────────────┬───────────────────┘
                                               │ DynamoDB Stream
                                               ▼
                                ┌──────────────────────────────────┐
                                │  dynamodb-stream-processor       │
                                │  Lambda                          │
                                │                                  │
                                │  INSERT  → ADD amount + count    │
                                │  MODIFY  → ADD delta             │
                                │  REMOVE  → SUBTRACT amount       │
                                └──────────────┬───────────────────┘
                                               │ write
                                               ▼
                                ┌──────────────────────────────────┐
                                │  CustomerStats Table             │
                                │  PK: customerId                  │
                                │  totalAmount, orderCount,        │
                                │  lastUpdated                     │
                                └──────────────────────────────────┘
```

### Data Flow

1. The **order-inserter** Lambda inserts 50 random orders into the `Orders` table.
2. Each write to `Orders` emits a stream record (`NEW_AND_OLD_IMAGES`).
3. The **stream-processor** Lambda is triggered in batches of up to 10 records.
4. For each record it atomically increments/decrements `totalAmount` and `orderCount` in `CustomerStats`.
5. `CustomerStats` reflects real-time aggregated sales per customer with no additional queries against the main table.

---

## AWS Resources

| Resource | Name | Notes |
|---|---|---|
| DynamoDB Table | `Orders-sean-0303` | PAY_PER_REQUEST, streams enabled, 7-day TTL |
| DynamoDB GSI | `status-timestamp-index` | Query orders by status, sorted by time |
| DynamoDB Table | `CustomerStats-sean-0303` | PAY_PER_REQUEST |
| Lambda | `dynamodb-stream-processor-sean-0303` | Python 3.12, triggered by Orders stream |
| Lambda | `order-inserter-sean-0303` | Python 3.12, inserts 50 random orders |
| IAM Role | `dynamodb-lambda-role-sean-0303` | Least-privilege execution role |
| EventSourceMapping | — | BatchSize=10, BisectOnError=true, LATEST |

---

## Screenshots

### DynamoDB Tables
![DynamoDB Tables](screenshots/dynamodb-tables.png)

### Lambda Functions
![Lambda Functions](screenshots/lambda-functions.png)

### Stream Trigger (EventSourceMapping)
![Stream Trigger](screenshots/stream-trigger.png)

### CustomerStats — Real-time Aggregation
![CustomerStats](screenshots/customer-stats.png)

### Orders GSI Query by Status
![GSI Query](screenshots/gsi-query.png)

---

## Folder Structure

```
dynamodb-streams-lambda-aggregation/
├── cdk/
│   ├── app.py              # CDK app entry point
│   ├── stack.py            # DynamoDbStreamsStack definition
│   ├── cdk.json            # CDK project config
│   └── requirements.txt    # CDK Python dependencies
├── lambda/
│   ├── stream_processor/
│   │   └── handler.py      # Processes stream events → aggregates CustomerStats
│   └── order_inserter/
│       └── handler.py      # Inserts 50 random orders into Orders table
├── screenshots/            # AWS Console screenshots
├── config.py               # Shared constants (table names, region, etc.)
├── seed_orders.py          # Invokes order-inserter Lambda post-deploy
├── query_stats.py          # Queries CustomerStats + GSI post-deploy
└── run_all.py              # Runs seed + query in sequence
```

---

## Prerequisites

- Python 3.10+
- Node.js 18+ (required by CDK CLI)
- AWS CLI configured with credentials for account `866934333672`
- AWS CDK CLI: `npm install -g aws-cdk`

Install CDK Python dependencies:

```bash
cd cdk
pip install -r requirements.txt
```

---

## Deploy

### 1. Bootstrap (one-time per account/region)

```bash
cd cdk
npx cdk bootstrap aws://866934333672/us-east-1
```

### 2. Deploy the stack

```bash
npx cdk deploy
```

CDK will create all resources and print their ARNs as CloudFormation outputs.

### 3. Destroy (cleanup)

```bash
npx cdk destroy
```

Both DynamoDB tables use `RemovalPolicy.DESTROY` so they are deleted on teardown.

---

## Post-Deploy Usage

All scripts read table and function names from `config.py` and use your default AWS credentials.

### Seed 50 random orders and watch aggregation

```bash
python seed_orders.py
```

### Query CustomerStats and GSI

```bash
python query_stats.py
```

### Run both in sequence

```bash
python run_all.py          # seed then query
python run_all.py --query  # query only
```

### Example output

```
  customerId      orderCount    totalAmount lastUpdated
  ────────────────────────────────────────────────────
  CUST-0001               5        1243.87 2025-04-14T18:22:01Z
  CUST-0002               6         987.44 2025-04-14T18:22:03Z
  CUST-0003               4        2104.12 2025-04-14T18:22:02Z
  ...
```
