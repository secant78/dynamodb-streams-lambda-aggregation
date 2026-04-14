import os
import aws_cdk as cdk
from aws_cdk import (
    Stack,
    Duration,
    RemovalPolicy,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_event_sources,
    aws_iam as iam,
)
from constructs import Construct

SUFFIX = "sean-0303"
LAMBDA_DIR = os.path.join(os.path.dirname(__file__), "..", "lambda")


class DynamoDbStreamsStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs):
        super().__init__(scope, construct_id, **kwargs)

        # ── DynamoDB: Orders ──────────────────────────────────────────────────

        orders_table = dynamodb.Table(
            self,
            "OrdersTable",
            table_name=f"Orders-{SUFFIX}",
            partition_key=dynamodb.Attribute(
                name="orderId",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            time_to_live_attribute="ttl",
            removal_policy=RemovalPolicy.DESTROY,
        )

        orders_table.add_global_secondary_index(
            index_name="status-timestamp-index",
            partition_key=dynamodb.Attribute(
                name="status",
                type=dynamodb.AttributeType.STRING,
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp",
                type=dynamodb.AttributeType.STRING,
            ),
            projection_type=dynamodb.ProjectionType.ALL,
        )

        # ── DynamoDB: CustomerStats ───────────────────────────────────────────

        customer_stats_table = dynamodb.Table(
            self,
            "CustomerStatsTable",
            table_name=f"CustomerStats-{SUFFIX}",
            partition_key=dynamodb.Attribute(
                name="customerId",
                type=dynamodb.AttributeType.STRING,
            ),
            billing_mode=dynamodb.BillingMode.PAY_PER_REQUEST,
            removal_policy=RemovalPolicy.DESTROY,
        )

        # ── IAM Role ──────────────────────────────────────────────────────────

        lambda_role = iam.Role(
            self,
            "LambdaRole",
            role_name=f"dynamodb-lambda-role-{SUFFIX}",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Lambda execution role for DynamoDB Streams aggregation",
        )

        # Basic execution (CloudWatch Logs)
        lambda_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        )

        # ── Lambda: Stream Processor ──────────────────────────────────────────

        stream_processor = lambda_.Function(
            self,
            "StreamProcessor",
            function_name=f"dynamodb-stream-processor-{SUFFIX}",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(
                os.path.join(LAMBDA_DIR, "stream_processor")
            ),
            timeout=Duration.seconds(60),
            memory_size=128,
            role=lambda_role,
            environment={
                "CUSTOMER_STATS_TABLE": customer_stats_table.table_name,
            },
        )

        # ── Lambda: Order Inserter ────────────────────────────────────────────

        order_inserter = lambda_.Function(
            self,
            "OrderInserter",
            function_name=f"order-inserter-{SUFFIX}",
            runtime=lambda_.Runtime.PYTHON_3_12,
            handler="handler.lambda_handler",
            code=lambda_.Code.from_asset(
                os.path.join(LAMBDA_DIR, "order_inserter")
            ),
            timeout=Duration.seconds(60),
            memory_size=128,
            role=lambda_role,
            environment={
                "ORDERS_TABLE": orders_table.table_name,
            },
        )

        # ── IAM Grants ────────────────────────────────────────────────────────

        # Stream processor: read/write CustomerStats
        customer_stats_table.grant_read_write_data(stream_processor)

        # Order inserter: write Orders
        orders_table.grant_write_data(order_inserter)

        # ── DynamoDB Stream → Stream Processor ───────────────────────────────
        # DynamoEventSource also grants stream-read permissions to the role.

        stream_processor.add_event_source(
            lambda_event_sources.DynamoEventSource(
                orders_table,
                starting_position=lambda_.StartingPosition.LATEST,
                batch_size=10,
                bisect_batch_on_error=True,
            )
        )

        # ── CloudFormation Outputs ────────────────────────────────────────────

        cdk.CfnOutput(self, "OrdersTableName", value=orders_table.table_name)
        cdk.CfnOutput(self, "CustomerStatsTableName", value=customer_stats_table.table_name)
        cdk.CfnOutput(self, "StreamProcessorArn", value=stream_processor.function_arn)
        cdk.CfnOutput(self, "OrderInserterArn", value=order_inserter.function_arn)
        cdk.CfnOutput(self, "LambdaRoleArn", value=lambda_role.role_arn)
