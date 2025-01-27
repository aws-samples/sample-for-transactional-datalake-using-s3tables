import json
import boto3
import os

kinesis_client = boto3.client("kinesis")
KINESIS_STREAM_NAME = os.environ["KINESIS_STREAM_NAME"]


def handler(event, context):
    print(f"Received {len(event['Records'])} records")
    for record in event["Records"]:
        if record["eventName"] == "INSERT" or record["eventName"] == "MODIFY":
            # Get the new image of the item
            new_image = record["dynamodb"]["NewImage"]

            # Convert DynamoDB JSON to regular JSON
            item = {k: list(v.values())[0] for k, v in new_image.items()}

            # Send the item to Kinesis
            kinesis_client.put_record(
                StreamName=KINESIS_STREAM_NAME,
                Data=json.dumps(item),
                PartitionKey=item["transaction_id"],
            )

    print(f"Successfully Sent {len(event['Records'])} records to Kinesis")
    return {
        "statusCode": 200,
        "body": json.dumps("Successfully processed DynamoDB stream events"),
    }
