import json
import base64
import datetime
from decimal import Decimal

print("Loading function")


# Custom JSON encoder to handle Decimal types
class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)


def handler(event, context):
    output = []

    for record in event["records"]:
        print(record["recordId"])
        payload = base64.b64decode(record["data"]).decode("utf-8")
        payload = json.loads(payload)

        if payload.get("eventName") == "INSERT" or payload.get("eventName") == "MODIFY":
            # Get the new image of the item
            new_image = payload["dynamodb"]["NewImage"]

            # Convert DynamoDB JSON to regular JSON
            item = {k: list(v.values())[0] for k, v in new_image.items()}

            # Add processing timestamp and format date fields
            if "timestamp" in item:
                dt = datetime.datetime.fromtimestamp(int(item["timestamp"]) / 1000.0)
                item["date"] = dt.strftime("%Y-%m-%d")
                item["hour"] = dt.hour
                item["minute"] = dt.minute

            # Encode the transformed data
            output_record = {
                "recordId": record["recordId"],
                "result": "Ok",
                "data": base64.b64encode(
                    json.dumps(item, cls=DecimalEncoder).encode("utf-8")
                ).decode("utf-8"),
            }
            output.append(output_record)

    print("Successfully processed {} records.".format(len(event["records"])))

    return {"records": output}
