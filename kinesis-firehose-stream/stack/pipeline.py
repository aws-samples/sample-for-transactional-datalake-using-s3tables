import aws_cdk as cdk
import json
from aws_cdk import (
    Stack,
    aws_dynamodb as dynamodb,
    aws_kinesis as kinesis,
    aws_lambda as lambda_,
    aws_lambda_event_sources as lambda_events,
    aws_iam as iam,
    custom_resources as cr,
    RemovalPolicy,
    CfnOutput,
)
from constructs import Construct


class PipelineStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Read context values
        table_bucket_name = self.node.try_get_context("table_bucket_name")
        table_name = self.node.try_get_context("table_name")
        namespace = self.node.try_get_context("namespace")
        bucket_name = f"{self.node.try_get_context("bucket_name")}-{self.account}"

        # Step 1 : Create DynamoDB table with stream enabled
        dynamodb_table = dynamodb.Table(
            self,
            "financial-transactions",
            table_name="financial-transactions",
            partition_key=dynamodb.Attribute(
                name="transaction_id", type=dynamodb.AttributeType.STRING
            ),
            sort_key=dynamodb.Attribute(
                name="timestamp", type=dynamodb.AttributeType.NUMBER
            ),
            billing_mode=dynamodb.BillingMode.PROVISIONED,
            read_capacity=10,
            write_capacity=10,
            stream=dynamodb.StreamViewType.NEW_AND_OLD_IMAGES,
            removal_policy=cdk.RemovalPolicy.DESTROY,  # Use with caution in production
            point_in_time_recovery=True,
        )
        ## update removal policy in kinesis data streams

        # Step 2: Create Kinesis Data Stream
        kinesis_stream = kinesis.Stream(
            self,
            "MyKinesisStream",
            stream_mode=kinesis.StreamMode.ON_DEMAND,
            encryption=kinesis.StreamEncryption.MANAGED,  # AwsSolutions-KDS3
        )
        kinesis_stream.apply_removal_policy(RemovalPolicy.DESTROY)

        # Create Lambda execution role
        lambda_role = iam.Role(
            self,
            "DynamoToKinesisLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Execution role for DynamoDB to Kinesis Lambda function",
        )

        # Create customer managed policy for DynamoDB stream access
        dynamo_stream_policy = iam.ManagedPolicy(
            self,
            "DynamoStreamReadPolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "dynamodb:GetRecords",
                        "dynamodb:GetShardIterator",
                        "dynamodb:DescribeStream",
                    ],
                    resources=[dynamodb_table.table_stream_arn],
                ),
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["dynamodb:ListStreams"],
                    resources=[dynamodb_table.table_arn],
                ),
            ],
        )

        # Create customer managed policy for Kinesis write access
        kinesis_write_policy = iam.ManagedPolicy(
            self,
            "KinesisWritePolicy",
            statements=[
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["kinesis:PutRecord", "kinesis:PutRecords"],
                    resources=[kinesis_stream.stream_arn],
                )
            ],
        )

        lambda_role.add_managed_policy(dynamo_stream_policy)
        lambda_role.add_managed_policy(kinesis_write_policy)

        # Step 3: Create Lambda function to process DynamoDB stream and send to Kinesis
        dynamo_to_kinesis_lambda = lambda_.Function(
            self,
            "DynamoToKinesisLambda",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="index.handler",
            code=lambda_.Code.from_asset("lambda/kinesis"),
            environment={"KINESIS_STREAM_NAME": kinesis_stream.stream_name},
            role=lambda_role,
        )

        event_source = lambda_.EventSourceMapping(
            self,
            "DynamoStreamEventSource",
            event_source_arn=dynamodb_table.table_stream_arn,
            target=dynamo_to_kinesis_lambda,
            starting_position=lambda_.StartingPosition.TRIM_HORIZON,
            batch_size=100,
            enabled=True,
        )

        # Step 4: Create Lambda Layer, Lambda function to interact with S3 tables APIs
        # boto3 layer to override default version in Lambda to enable support for s3tables APIs
        boto3_layer = lambda_.LayerVersion(
            self,
            "Boto3Layer",
            code=lambda_.Code.from_asset("boto3-layer/"),
            compatible_runtimes=[lambda_.Runtime.PYTHON_3_13],
            description="Boto3 library",
        )

        # Create the IAM role for the Lambda function
        manage_s3_table_role = iam.Role(
            self,
            "ManageS3TableLambdaRole",
            assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
            description="Role for Managing S3 Table Lambda function",
        )

        # Add AWS Lambda basic execution policy
        manage_s3_table_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AWSLambdaBasicExecutionRole"
            )
        )

        # Add policies for S3 Table access
        manage_s3_table_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3tables:CreateTable",
                    "s3tables:DeleteTable",
                    "s3tables:GetTable",
                    "s3tables:ListTables",
                    "s3tables:CreateTableBucket",
                    "s3tables:CreateNamespace",
                    "s3tables:UpdateTableMetadataLocation",
                    "s3tables:ListTableBuckets",
                    "s3tables:DeleteNamespace",
                    "s3tables:DeleteTableBucket",
                ],
                resources=[
                    f"arn:aws:s3tables:{Stack.of(self).region}:{Stack.of(self).account}:bucket/{table_bucket_name}",
                    f"arn:aws:s3tables:{Stack.of(self).region}:{Stack.of(self).account}:bucket/{table_bucket_name}/*",
                    f"arn:aws:s3tables:{Stack.of(self).region}:{Stack.of(self).account}:bucket/*",  # Added for ListTableBuckets
                ],
            )
        )

        # Create Lambda function to manage S3 Table
        manage_s3_table_lambda = lambda_.Function(
            self,
            "ManageS3TableLambda",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="index.handler",
            code=lambda_.Code.from_asset("lambda/custom_resource"),
            timeout=cdk.Duration.minutes(5),
            layers=[boto3_layer],
            role=manage_s3_table_role,  # Assign the role to the Lambda function
        )

        # Step 5: Create Custom Resource to invoke Lambda
        s3_table_custom_resource = cr.AwsCustomResource(
            self,
            "S3TableCustomResource",
            on_create=cr.AwsSdkCall(
                service="Lambda",
                action="invoke",
                parameters={
                    "FunctionName": manage_s3_table_lambda.function_name,
                    "InvocationType": "RequestResponse",
                    "Payload": json.dumps(
                        {
                            "RequestType": "Create",
                            "StackId": self.stack_id,
                            "ResourceProperties": {
                                "table_bucket_name": table_bucket_name,
                                "table_name": table_name,
                                "namespace": namespace,
                            },
                        }
                    ),
                },
                physical_resource_id=cr.PhysicalResourceId.of("S3TableCreation"),
            ),
            on_delete=cr.AwsSdkCall(
                service="Lambda",
                action="invoke",
                parameters={
                    "FunctionName": manage_s3_table_lambda.function_name,
                    "InvocationType": "RequestResponse",
                    "Payload": json.dumps(
                        {
                            "RequestType": "Delete",
                            "StackId": self.stack_id,
                            "ResourceProperties": {
                                "table_bucket_name": table_bucket_name,
                                "table_name": table_name,
                                "namespace": namespace,
                            },
                        }
                    ),
                },
                physical_resource_id=cr.PhysicalResourceId.of("S3TableDeletion"),
            ),
            policy=cr.AwsCustomResourcePolicy.from_statements(
                [
                    iam.PolicyStatement(
                        actions=["lambda:InvokeFunction"],
                        resources=[manage_s3_table_lambda.function_arn],
                    )
                ]
            ),
        )
        # Ensure Custom Resource is created after the Lambda and Bucket
        s3_table_custom_resource.node.add_dependency(manage_s3_table_lambda)

        # Add CfnOutput
        CfnOutput(
            self,
            "KinesisStreamARN",
            value=kinesis_stream.stream_arn,
            export_name="KinesisStreamARN",
        )
