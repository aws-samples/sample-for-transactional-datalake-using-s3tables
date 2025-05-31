import aws_cdk as cdk
import json
from aws_cdk import (
    Stack,
    aws_iam as iam,
    aws_s3 as s3,
    RemovalPolicy,
    aws_glue as glue,
    aws_lakeformation as lakeformation,
    CfnOutput,
)
from constructs import Construct


class LakeFormationStack(Stack):

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # Read context values
        table_bucket_name = self.node.try_get_context("table_bucket_name")
        table_name = self.node.try_get_context("table_name")
        namespace = self.node.try_get_context("namespace")
        stream_type = self.node.try_get_context("stream_type")

        # Get the CDK deployment role, add permissions and register as Lakeformation Administrator
        cdk_role = iam.Role.from_role_arn(
            self,
            "CDKRole",
            role_arn=f"arn:aws:iam::{self.account}:role/cdk-hnb659fds-cfn-exec-role-{self.account}-{self.region}",
        )

        # Grant necessary permissions to the deployment role
        cdk_lf_permissions = cdk_role.add_to_principal_policy(
            iam.PolicyStatement(
                actions=[
                    "glue:CreateDatabase",
                    "glue:DeleteDatabase",
                    "glue:GetDatabase",
                    "glue:GetDatabases",
                    "glue:UpdateDatabase",
                    "glue:CreateTable",
                    "glue:DeleteTable",
                    "glue:UpdateTable",
                    "glue:GetTable",
                    "glue:GetTables",
                    "lakeformation:GetDataLakeSettings",
                    "lakeformation:PutDataLakeSettings",
                    "lakeformation:GrantPermissions",
                    "lakeformation:RevokePermissions",
                ],
                resources=["*"],
            )
        )

        # Register the CDK role as a Lake Formation administrator
        admin_settings = lakeformation.CfnDataLakeSettings(
            self,
            "DataLakeSettings",
            admins=[
                lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=cdk_role.role_arn
                )
            ],
        )
        admin_settings.node.add_dependency(cdk_role)

        # Create IAM role & permissions for Firehose
        firehose_role = iam.Role(
            self,
            "FirehoseRole",
            assumed_by=iam.ServicePrincipal("firehose.amazonaws.com"),
        )

        if stream_type == "kinesis":
            # Import Kinesis stream ARN from Pipeline stack
            kinesis_stream_arn = cdk.Fn.import_value("KinesisStreamARN")

            # Add explicit permissions for Kinesis operations
            firehose_role.add_to_policy(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=[
                        "kinesis:DescribeStream",
                        "kinesis:GetShardIterator",
                        "kinesis:GetRecords",
                        "kinesis:ListShards",
                    ],
                    resources=[kinesis_stream_arn],
                )
            )

        # Add CloudWatch Logs permissions
        firehose_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "logs:PutLogEvents",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                ],
                resources=["arn:aws:logs:*:*:*"],
            )
        )

        # Add Lambda invoke permissions for Firehose
        firehose_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["lambda:InvokeFunction", "lambda:GetFunctionConfiguration"],
                resources=["*"],
            )
        )

        # Add S3 Tables Glue Federation Permissions as per documentation -
        # https://docs.aws.amazon.com/AmazonS3/latest/userguide/s3-tables-integrating-firehose.html
        firehose_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["glue:GetTable", "glue:GetDatabase", "glue:UpdateTable"],
                resources=[
                    f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:catalog/s3tablescatalog/*",
                    f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:catalog/s3tablescatalog",
                    f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:catalog",
                    f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:database/*",
                    f"arn:aws:glue:{Stack.of(self).region}:{Stack.of(self).account}:table/*/*",
                ],
            )
        )

        # Add lakeformation getDataAccess permissions as per documentation
        # https://docs.aws.amazon.com/firehose/latest/dev/controlling-access.html#using-s3-tables
        firehose_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["lakeformation:GetDataAccess"],
                resources=["*"],
            )
        )

        # Create S3 bucket for Failed delivery
        bucket = s3.Bucket(
            self,
            "stream-failed-delivery",
            removal_policy=cdk.RemovalPolicy.DESTROY,  # Use with caution in production
            # auto_delete_objects=True,  # Use with caution in production
            server_access_logs_prefix="logs",
            enforce_ssl=True,
        )

        # Add S3 specific permissions
        firehose_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=[
                    "s3:AbortMultipartUpload",
                    "s3:GetBucketLocation",
                    "s3:GetObject",
                    "s3:ListBucket",
                    "s3:ListBucketMultipartUploads",
                    "s3:PutObject",
                    "s3:DeleteObject",
                ],
                resources=[
                    bucket.bucket_arn,
                    f"{bucket.bucket_arn}/*",
                ],
            )
        )

        # Create the resource link
        resource_link = glue.CfnDatabase(
            self,
            "ResourceLink",
            catalog_id=self.account,
            database_input=glue.CfnDatabase.DatabaseInputProperty(
                name="firehoses3tableresourcelink",
                target_database=glue.CfnDatabase.DatabaseIdentifierProperty(
                    catalog_id=f"{cdk.Aws.ACCOUNT_ID}:s3tablescatalog/{table_bucket_name}",
                    database_name=namespace,
                ),
            ),
        )

        # Grant to Lake Formation service
        lakeformation.CfnPermissions(
            self,
            "LakeFormationServicePermissions",
            data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=cdk_role.role_arn
            ),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                database_resource=lakeformation.CfnPermissions.DatabaseResourceProperty(
                    name=resource_link.ref
                )
            ),
            permissions=["ALL"],
        )

        #  Grant permission to resource link
        lf_permissions_resource_link = lakeformation.CfnPermissions(
            self,
            "GrantPermissionsToResourceLink",
            data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=firehose_role.role_arn
            ),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                database_resource=lakeformation.CfnPermissions.DatabaseResourceProperty(
                    catalog_id=cdk.Aws.ACCOUNT_ID, name=resource_link.ref
                ),
            ),
            permissions=["DESCRIBE"],
            # permissionsWithGrantOption = ['DESCRIBE']
        )
        lf_permissions_resource_link.node.add_dependency(resource_link)

        # Register the Firehose role as a Lake Formation admin
        lakeformation.CfnDataLakeSettings(
            self,
            "DataLakeAdmin",
            admins=[
                lakeformation.CfnDataLakeSettings.DataLakePrincipalProperty(
                    data_lake_principal_identifier=firehose_role.role_arn
                )
            ],
        )

        # Grant to Catalog / tablebucket / namespace / table
        lf_permissions_table = lakeformation.CfnPermissions(
            self,
            "GrantPermissionsToTable",
            data_lake_principal=lakeformation.CfnPermissions.DataLakePrincipalProperty(
                data_lake_principal_identifier=firehose_role.role_arn
            ),
            resource=lakeformation.CfnPermissions.ResourceProperty(
                table_resource=lakeformation.CfnPermissions.TableResourceProperty(
                    catalog_id=f"{cdk.Aws.ACCOUNT_ID}:s3tablescatalog/{table_bucket_name}",
                    database_name=namespace,
                    name=table_name,
                ),
            ),
            permissions=["ALL"],
        )

        CfnOutput(
            self,
            "FirehoseRoleARN",
            value=firehose_role.role_arn,
            export_name="FirehoseRoleARN",
        )
        CfnOutput(
            self,
            "ResourceLinkARN",
            value=resource_link.ref,
            export_name="ResourceLinkARN",
        )
        CfnOutput(
            self,
            "FailedDeliveryBucketARN",
            value=bucket.bucket_arn,
            export_name="FailedDeliveryBucketARN",
        )
