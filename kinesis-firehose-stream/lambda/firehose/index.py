import json
import boto3
import os

firehose_client = boto3.client("firehose")
FIREHOSE_DELIVERY_STREAM = os.environ["FIREHOSE_DELIVERY_STREAM"]


def handler(event, context):
    print(f"Received {len(event['Records'])} records")
    for record in event["Records"]:
        if record["eventName"] == "INSERT" or record["eventName"] == "MODIFY":
            # Get the new image of the item
            new_image = record["dynamodb"]["NewImage"]

            # Convert DynamoDB JSON to regular JSON
            item = {k: list(v.values())[0] for k, v in new_image.items()}

            # Send the item to Kinesis Firehose
            firehose_client.put_record(
                DeliveryStreamName=FIREHOSE_DELIVERY_STREAM,
                Record={"Data": json.dumps(item)}
            )

    print(f"Successfully Sent {len(event['Records'])} records to Firehose")
    return {
        "statusCode": 200,
        "body": json.dumps("Successfully processed DynamoDB stream events"),
    }