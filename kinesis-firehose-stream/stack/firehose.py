import aws_cdk as cdk
import json
from aws_cdk import (
    Stack,
    aws_kinesisfirehose as firehose,
    aws_lambda as lambda_,
    aws_iam as iam,
    custom_resources as cr,
    CfnOutput,
)
from constructs import Construct


class FirehoseStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Read firehose role arn from LakeFormation Stack export value
        firehose_role_arn = cdk.Fn.import_value("FirehoseRoleARN")
        resource_link_arn = cdk.Fn.import_value("ResourceLinkARN")
        failed_delivery_bucket_arn = cdk.Fn.import_value("FailedDeliveryBucketARN")
        dynamodb_table_arn = cdk.Fn.import_value("DynamoDBTableARN")

        # Read context values
        table_bucket_name = self.node.try_get_context("table_bucket_name")
        table_name = self.node.try_get_context("table_name")
        namespace = self.node.try_get_context("namespace")
        stream_type = self.node.try_get_context("stream_type")

        # Create the Firehose delivery stream based on stream_type
        if stream_type == "kinesis":
            # Import Kinesis stream ARN from Pipeline stack
            kinesis_stream_arn = cdk.Fn.import_value("KinesisStreamARN")

            # Create Lambda function for transforming Kinesis data
            transform_lambda_role = iam.Role(
                self,
                "KinesisTransformLambdaRole",
                assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                description="Execution role for Kinesis data transformation Lambda function",
            )

            transform_lambda_policy = iam.ManagedPolicy(
                self,
                "KinesisTransformPolicy",
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents",
                        ],
                        resources=["*"],
                    )
                ],
            )

            transform_lambda_role.add_managed_policy(transform_lambda_policy)

            transform_lambda = lambda_.Function(
                self,
                "KinesisTransformLambda",
                runtime=lambda_.Runtime.PYTHON_3_13,
                handler="index.handler",
                code=lambda_.Code.from_asset("lambda/transform"),
                role=transform_lambda_role,
                timeout=cdk.Duration.seconds(60),
            )

            # Create Firehose with Kinesis as source and data transformation
            delivery_stream = firehose.CfnDeliveryStream(
                self,
                "MyDeliveryStream",
                delivery_stream_type="KinesisStreamAsSource",
                kinesis_stream_source_configuration=firehose.CfnDeliveryStream.KinesisStreamSourceConfigurationProperty(
                    kinesis_stream_arn=kinesis_stream_arn,
                    role_arn=firehose_role_arn,
                ),
                iceberg_destination_configuration=firehose.CfnDeliveryStream.IcebergDestinationConfigurationProperty(
                    role_arn=firehose_role_arn,
                    catalog_configuration=firehose.CfnDeliveryStream.CatalogConfigurationProperty(
                        catalog_arn=f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:catalog"
                    ),
                    s3_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                        bucket_arn=failed_delivery_bucket_arn,
                        compression_format="UNCOMPRESSED",  # Iceberg handles compression internally
                        error_output_prefix="errors/",
                        role_arn=firehose_role_arn,
                    ),
                    buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                        interval_in_seconds=60, size_in_m_bs=1
                    ),
                    destination_table_configuration_list=[
                        firehose.CfnDeliveryStream.DestinationTableConfigurationProperty(
                            destination_table_name=table_name,
                            destination_database_name=resource_link_arn,
                            unique_keys=["transaction_id"],
                        )
                    ],
                    cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                        enabled=True,
                        log_group_name=f"/aws/firehose/{table_name}",
                        log_stream_name="IcebergDelivery",
                    ),
                    retry_options=firehose.CfnDeliveryStream.RetryOptionsProperty(
                        duration_in_seconds=300
                    ),
                    processing_configuration=firehose.CfnDeliveryStream.ProcessingConfigurationProperty(
                        enabled=True,
                        processors=[
                            firehose.CfnDeliveryStream.ProcessorProperty(
                                type="Lambda",
                                parameters=[
                                    firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                        parameter_name="LambdaArn",
                                        parameter_value=transform_lambda.function_arn,
                                    ),
                                    firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                        parameter_name="BufferSizeInMBs",
                                        parameter_value="1",
                                    ),
                                    firehose.CfnDeliveryStream.ProcessorParameterProperty(
                                        parameter_name="BufferIntervalInSeconds",
                                        parameter_value="60",
                                    ),
                                ],
                            )
                        ],
                    ),
                ),
            )
        else:  # stream_type == "dynamodb" - use Direct PUT
            # Create Firehose with Direct PUT as source
            dynamodb_table_stream_arn = cdk.Fn.import_value("DynamoDBTableStreamARN")
            delivery_stream = firehose.CfnDeliveryStream(
                self,
                "MyDeliveryStream",
                delivery_stream_type="DirectPut",
                iceberg_destination_configuration=firehose.CfnDeliveryStream.IcebergDestinationConfigurationProperty(
                    role_arn=firehose_role_arn,
                    catalog_configuration=firehose.CfnDeliveryStream.CatalogConfigurationProperty(
                        catalog_arn=f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:catalog"
                    ),
                    s3_configuration=firehose.CfnDeliveryStream.S3DestinationConfigurationProperty(
                        bucket_arn=failed_delivery_bucket_arn,
                        compression_format="UNCOMPRESSED",  # Iceberg handles compression internally
                        error_output_prefix="errors/",
                        role_arn=firehose_role_arn,
                    ),
                    buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                        interval_in_seconds=60, size_in_m_bs=1
                    ),
                    destination_table_configuration_list=[
                        firehose.CfnDeliveryStream.DestinationTableConfigurationProperty(
                            destination_table_name=table_name,
                            destination_database_name=resource_link_arn,
                            unique_keys=["transaction_id"],
                        )
                    ],
                    cloud_watch_logging_options=firehose.CfnDeliveryStream.CloudWatchLoggingOptionsProperty(
                        enabled=True,
                        log_group_name=f"/aws/firehose/{table_name}",
                        log_stream_name="IcebergDelivery",
                    ),
                    retry_options=firehose.CfnDeliveryStream.RetryOptionsProperty(
                        duration_in_seconds=300
                    ),
                ),
            )

            # Create IAM role for Lambda function
            lambda_role = iam.Role(
                self,
                "DynamoStreamProcessorRole",
                assumed_by=iam.ServicePrincipal("lambda.amazonaws.com"),
                description="Execution role for DynamoDB Stream processor Lambda function",
            )

            # Add policy to allow Lambda to read from DynamoDB stream
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
                        resources=[dynamodb_table_stream_arn],
                    ),
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["dynamodb:ListStreams"],
                        resources=[dynamodb_table_arn],
                    ),
                ],
            )

            firehose_write_policy = iam.ManagedPolicy(
                self,
                "FirehoseWritePolicy",
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=["firehose:PutRecord", "firehose:PutRecordBatch"],
                        resources=["*"],
                    )
                ],
            )

            cloudwatch_write_policy = iam.ManagedPolicy(
                self,
                "CloudWatchWritePolicy",
                statements=[
                    iam.PolicyStatement(
                        effect=iam.Effect.ALLOW,
                        actions=[
                            "logs:CreateLogGroup",
                            "logs:CreateLogStream",
                            "logs:PutLogEvents",
                        ],
                        resources=["*"],
                    )
                ],
            )

            lambda_role.add_managed_policy(dynamo_stream_policy)
            lambda_role.add_managed_policy(firehose_write_policy)
            lambda_role.add_managed_policy(cloudwatch_write_policy)

            # Create Lambda function to process DynamoDB stream and write to Firehose
            dynamo_to_firehose_lambda = lambda_.Function(
                self,
                "DynamoToFirehoseLambda",
                runtime=lambda_.Runtime.PYTHON_3_13,
                handler="index.handler",
                code=lambda_.Code.from_asset("lambda/firehose"),
                environment={
                    "FIREHOSE_DELIVERY_STREAM": delivery_stream.ref,
                },
                role=lambda_role,
                timeout=cdk.Duration.seconds(180),
            )

            event_source = lambda_.EventSourceMapping(
                self,
                "DynamoStreamEventSource",
                event_source_arn=dynamodb_table_stream_arn,
                target=dynamo_to_firehose_lambda,
                starting_position=lambda_.StartingPosition.TRIM_HORIZON,
                batch_size=100,
                enabled=True,
            )

        # Export the delivery stream name and ARN
        CfnOutput(
            self,
            "FirehoseDeliveryStreamName",
            value=delivery_stream.ref,
            export_name="FirehoseDeliveryStreamName",
        )

        CfnOutput(
            self,
            "FirehoseDeliveryStreamARN",
            value=cdk.Fn.sub(
                "arn:aws:firehose:${AWS::Region}:${AWS::AccountId}:deliverystream/${DeliveryStreamName}",
                {"DeliveryStreamName": delivery_stream.ref},
            ),
            export_name="FirehoseDeliveryStreamARN",
        )
