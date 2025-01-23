import boto3
import time
import json
from botocore.exceptions import ClientError

# Ensure that the principal (User or Role) that you use to run this script has
# permissions to access S3 Tables, Glue and Athena. Also grant lakeformation
# permissions to the principal (User or Role) that to query the table.
# aws lakeformation grant-permissions \
#   --principal '{"DataLakePrincipalIdentifier": "arn:aws:iam::123456789012:user/johndoe"}' \
#   --resource '{"Table": {"CatalogId": "123456789012:s3tablescatalog/streamtablebucket", "DatabaseName": "streamnamespace", "Name": "streamtable"}}' \
#   --permissions SELECT DESCRIBE


class UpdateMetadata:
    def __init__(self):
        self.s3tables = boto3.client("s3tables")
        self.sts = boto3.client("sts")
        self.glue = boto3.client("glue")
        self.athena = boto3.client("athena")

        self.account_id = self.sts.get_caller_identity()["Account"]
        self.region = boto3.Session().region_name

        self.table_bucket_name = "streamtablebucket"  # HARD CODED - Update if required
        self.temp_table_name = "temptable"  # HARD CODED - Update if requird
        self.athena_output_location = f"s3://update-bucket-name/"  # HARD CODED - Update before running this script

    def get_table_bucket_arn(self):
        try:
            response = self.s3tables.list_table_buckets()
            table_bucket_arn = None
            # Loop through the table buckets to find the matching one
            for table_bucket in response.get("tableBuckets", []):
                if table_bucket["name"] == self.table_bucket_name:
                    table_bucket_arn = table_bucket["arn"]
                    break
            return table_bucket_arn
        except Exception as e:
            print(f"Error getting table bucket ARN: {e}")
            raise

    def _wait_for_athena_query(self, query_execution_id, timeout=300, interval=10):
        start_time = time.time()
        while (time.time() - start_time) < timeout:
            status = self.athena.get_query_execution(
                QueryExecutionId=query_execution_id
            )["QueryExecution"]["Status"]["State"]

            if status in ["SUCCEEDED", "FAILED", "CANCELLED"]:
                return status
            time.sleep(interval)

        raise TimeoutError("Athena query timed out")

    # The table created in the S3 tables does not have any schema/metadata associated with it
    # Currently DDLs for S3 tables are supported only from Apache Spark Clients (Glue/EMR)
    # We are using this workaround to avoid using Glue or EMR. The hack is to create a temporary ICEBERG table with desired schema.
    # Extract the warehouse_location and metadata JSON from the temporary table and update it to the S3 Table.
    def update_metadata(self, namespace, table_name):

        response = self.s3tables.get_table(
            tableBucketARN=self.get_table_bucket_arn(),
            namespace=namespace,
            name=table_name,
        )
        warehouse_location = response["warehouseLocation"]
        version_token = response["versionToken"]

        query = f"""
        CREATE TABLE IF NOT EXISTS default.{self.temp_table_name}(id string, name string)
        LOCATION '{warehouse_location}'
        TBLPROPERTIES ( 'table_type'= 'ICEBERG' )
        """

        response = self.athena.start_query_execution(
            QueryString=query,
            ResultConfiguration={"OutputLocation": self.athena_output_location},
        )
        status = self._wait_for_athena_query(response["QueryExecutionId"])
        if status == "SUCCEEDED":
            print(f"Created table '{self.temp_table_name}' in database 'default'")

        response = self.glue.get_table(
            CatalogId=self.account_id,
            DatabaseName="default",
            Name=self.temp_table_name,
        )

        metadata_location = response["Table"]["Parameters"]["metadata_location"]

        response = self.s3tables.update_table_metadata_location(
            tableBucketARN=self.get_table_bucket_arn(),
            namespace=namespace,
            name=table_name,
            versionToken=version_token,
            metadataLocation=metadata_location,
        )
        print(f"Updated table metadata location.")

        # drop the temp table
        response = self.glue.delete_table(
            DatabaseName="default", Name=self.temp_table_name
        )
        print(f"Deleted table '{self.temp_table_name}' from database 'default'")

        print(f"Update complete. ")


def main():

    update = UpdateMetadata()

    try:
        print(f"Updating metadata....")
        table_name = "streamtable"
        namespace = "streamnamespace"

        update.update_metadata(namespace, table_name)

        print(f"Successfully updated metadata for streamtable {u'\u2713'}")

    except Exception as e:
        print(f"Failed to Update metadata: {e}")
        raise


if __name__ == "__main__":
    main()
