import os
import json
import boto3
import logging
from decimal import Decimal
from datetime import datetime

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DESTINATION_BUCKET = os.environ.get("DESTINATION_BUCKET")
s3_client = boto3.client("s3")


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


def lambda_handler(event, context):
    for record in event["Records"]:
        if record["eventName"] == "MODIFY":
            new_image = record["dynamodb"].get("NewImage", {})
            old_image = record["dynamodb"].get("OldImage", {})
            new_status = new_image.get("trip_status", {}).get("S")
            old_status = old_image.get("trip_status", {}).get("S")

            if new_status == "COMPLETED" and old_status != "COMPLETED":
                trip_id = new_image.get("trip_id", {}).get("S")
                if not trip_id:
                    continue

                logger.info(
                    f"Trip {trip_id} completed. Storing in S3 for daily aggregation."
                )
                payload = {
                    key: val[list(val.keys())[0]] for key, val in new_image.items()
                }

                completion_time = datetime.fromisoformat(payload["dropoff_datetime"])
                s3_key = (
                    f"completed-trips/year={completion_time.year}"
                    f"/month={completion_time.month:02d}/day={completion_time.day:02d}/{trip_id}.json"
                )

                try:
                    s3_client.put_object(
                        Bucket=DESTINATION_BUCKET,
                        Key=s3_key,
                        Body=json.dumps(payload, cls=DecimalEncoder),
                    )
                except Exception as e:
                    logger.error(
                        f"Error storing record for trip_id {trip_id} in S3: {e}"
                    )
                    raise e
