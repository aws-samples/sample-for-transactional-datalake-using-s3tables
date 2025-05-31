# lambda/index.py
import boto3
import json
from botocore.exceptions import ClientError


# Add cfnresponse
def send_cfn_response(event, context, response_status, response_data, reason=None):
    response_body = {
        "Status": response_status,
        "Reason": reason or "See the details in CloudWatch Log Stream",
        "PhysicalResourceId": None,
        "StackId": event["StackId"],
        "RequestId": None,
        "LogicalResourceId": None,
        "NoEcho": False,
        "Data": response_data,
    }


def handler(event, context):
    print(event)
    print(context)
    s3tables = boto3.client("s3tables")

    # Extract properties from the event
    props = event["ResourceProperties"]
    table_bucket_name = props["table_bucket_name"]
    table_name = props["table_name"]
    namespace = props["namespace"]

    # Ensure namespace is a list
    if isinstance(namespace, str):
        namespace = [namespace]

    request_type = event["RequestType"]

    try:
        if request_type == "Create":
            response_data = create_table(
                s3tables,
                table_bucket_name,
                namespace,
                table_name,
            )
            send_cfn_response(event, context, "SUCCESS", response_data)
        elif request_type == "Update":
            send_cfn_response(event, context, "SUCCESS", response_data)
        elif request_type == "Delete":
            response_data = delete_table(
                s3tables, table_bucket_name, namespace, table_name
            )
            send_cfn_response(event, context, "SUCCESS", response_data)
        else:
            send_cfn_response(
                event, context, "FAILED", {"Error": "Invalid request type"}
            )
    except Exception as e:
        print(f"Error: {str(e)}")
        send_cfn_response(event, context, "FAILED", {"Error": str(e)})


def create_table(
    s3tables,
    table_bucket_name,
    namespace,
    table_name,
):
    try:
        # Step 1: Create the table bucket
        table_bucket = s3tables.create_table_bucket(name=table_bucket_name)
        print(table_bucket)
        print(f"S3 Table Bucket '{table_bucket}' created successfully")

        # Step 2: Create the namespace
        ns = s3tables.create_namespace(
            tableBucketARN=table_bucket["arn"], namespace=namespace
        )
        print(
            f"Namespace '{namespace}' created successfully in bucket '{table_bucket_name}'"
        )

        print(namespace[0])

        # Step 3: Create the table
        response = s3tables.create_table(
            tableBucketARN=table_bucket["arn"],
            namespace=namespace[0],
            name=table_name,
            format="ICEBERG",
            metadata={
                "iceberg": {
                    "schema": {
                        "fields": [
                            {
                                "name": "transaction_id",
                                "type": "string",
                                "required": True,
                            },
                            {"name": "timestamp", "type": "long"},
                            {"name": "customer_id", "type": "string"},
                            {"name": "date", "type": "date"},
                            {"name": "hour", "type": "int"},
                            {"name": "minute", "type": "int"},
                            {"name": "transaction_type", "type": "string"},
                            {"name": "amount", "type": "decimal(12,2)"},
                            {"name": "currency", "type": "string"},
                            {"name": "merchant_category", "type": "string"},
                            {"name": "payment_method", "type": "string"},
                            {"name": "region", "type": "string"},
                            {"name": "risk_score", "type": "string"},
                            {"name": "status", "type": "string"},
                            {"name": "processing_timestamp", "type": "long"},
                            {"name": "device_type", "type": "string"},
                            {"name": "authentication_method", "type": "string"},
                            {"name": "merchant_id", "type": "string"},
                            {"name": "velocity_check", "type": "string"},
                            {"name": "amount_threshold", "type": "string"},
                            {"name": "location_risk", "type": "string"},
                            {"name": "pattern_match", "type": "string"},
                        ]
                    }
                }
            },
        )

        print(f"S3 Table '{table_name}' created successfully")
        return {
            "Message": "S3 Table created successfully",
            "TableArn": response.get("tableArn"),
        }
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceAlreadyExistsException":
            print(f"Table {table_name} already exists. Skipping creation.")
            return {"Message": f"S3 Table {table_name} already exists"}
        else:
            raise


def delete_table(s3tables, table_bucket_name, namespace, table_name):
    try:

        # List all table buckets
        response = s3tables.list_table_buckets()
        print(response)
        table_bucket_arn = None
        # Loop through the table buckets to find the matching one
        for table_bucket in response.get("tableBuckets", []):
            if table_bucket["name"] == table_bucket_name:
                table_bucket_arn = table_bucket["arn"]
                break
        print(namespace)

        # Step 1: Delete the table
        response = s3tables.delete_table(
            tableBucketARN=table_bucket_arn,
            namespace=namespace[0],
            name=table_name,
        )
        print(f"Deleted table {table_name} in namespace {namespace}")

        # Step 2: Delete the namespace
        s3tables.delete_namespace(
            tableBucketARN=table_bucket_arn, namespace=namespace[0]
        )
        print(f"Deleted namespace {namespace}")

        # Step 3: Delete the table bucket
        s3tables.delete_table_bucket(tableBucketARN=table_bucket_arn)
        return {"Message": "S3 Table deleted successfully"}
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceNotFoundException":
            print(f"Table {table_name} not found. Skipping deletion.")
            return {"Message": f"S3 Table {table_name} does not exist"}
        else:
            raise
