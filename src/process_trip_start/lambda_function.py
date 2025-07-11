import json
import base64
import boto3
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger()
logger.setLevel(logging.INFO)
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table("trips")


def lambda_handler(event, context):
    for record in event["Records"]:
        try:
            payload = json.loads(base64.b64decode(record["kinesis"]["data"]))
            trip_id = payload.get("trip_id")

            # Set initial status
            payload["trip_status"] = "IN_PROGRESS"

            # Only create the record if the trip_id does not already exist.
            # This prevents a duplicate start event from overwriting a completed trip.
            table.put_item(
                Item=payload, ConditionExpression="attribute_not_exists(trip_id)"
            )
            logger.info(f"Successfully created record for trip_id: {trip_id}")

        except ClientError as e:
            # Gracefully handle the expected error when a duplicate arrives
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                logger.warning(
                    f"Idempotency check failed. Record for trip_id {trip_id} already exists."
                )
            else:
                logger.error(f"AWS ClientError processing trip_id {trip_id}: {e}")
                raise  # Re-raise other AWS errors to trigger DLQ
        except Exception as e:
            logger.error(f"Unhandled exception on trip_id {trip_id}: {e}")
            raise
