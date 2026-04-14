import aws_cdk as cdk
from stack import DynamoDbStreamsStack

app = cdk.App()

DynamoDbStreamsStack(
    app,
    "DynamoDbStreamsStack",
    env=cdk.Environment(account="866934333672", region="us-east-1"),
)

app.synth()
