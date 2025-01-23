import aws_cdk as cdk
import json
from aws_cdk import (
    Stack,
    aws_kinesisfirehose as firehose,
)
from constructs import Construct


class FirehoseStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Read firehose role arn from LakeFormation Stack export value
        firehose_role_arn = cdk.Fn.import_value("FirehoseRoleARN")
        resource_link_arn = cdk.Fn.import_value("ResourceLinkARN")
        kinesis_stream_arn = cdk.Fn.import_value("KinesisStreamARN")
        failed_delivery_bucket_arn = cdk.Fn.import_value("FailedDeliveryBucketARN")

        # Read context values
        table_bucket_name = self.node.try_get_context("table_bucket_name")
        table_name = self.node.try_get_context("table_name")
        namespace = self.node.try_get_context("namespace")

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
                    # buffering_hints=firehose.CfnDeliveryStream.BufferingHintsProperty(
                    #     interval_in_seconds=60,
                    #     size_in_m_bs=1,  # Increased for better performance with Iceberg
                    # ),
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
                        unique_keys=["id"],
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
