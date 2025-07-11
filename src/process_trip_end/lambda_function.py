import json
import base64
import boto3
import logging
import os
from botocore.exceptions import ClientError

# --- Standard Configuration ---
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# --- Boto3 Clients and Environment Variables ---
TABLE_NAME = "trips"
RETRY_QUEUE_URL = os.environ.get("RETRY_QUEUE_URL")

dynamodb = boto3.resource("dynamodb")
sqs_client = boto3.client("sqs")
table = dynamodb.Table(TABLE_NAME)


def lambda_handler(event, context):
    for record in event["Records"]:
        trip_id = None  # Initialize trip_id to ensure it's available for logging
        try:
            payload = json.loads(base64.b64decode(record["kinesis"]["data"]))
            trip_id = payload.get("trip_id")

            update_expression = (
                "SET trip_status = :status, dropoff_datetime = :ddt, rate_code = :rc, "
                "passenger_count = :pc, trip_distance = :td, fare_amount = :fa, "
                "tip_amount = :ta, payment_type = :pt, trip_type = :tt"
            )

            expression_values = {
                ":status": "COMPLETED",
                ":ddt": payload.get("dropoff_datetime"),
                ":rc": payload.get("rate_code"),
                ":pc": payload.get("passenger_count"),
                ":td": payload.get("trip_distance"),
                ":fa": payload.get("fare_amount"),
                ":ta": payload.get("tip_amount"),
                ":pt": payload.get("payment_type"),
                ":tt": payload.get("trip_type"),
            }
            condition_expression = "attribute_exists(trip_id)"

            table.update_item(
                Key={"trip_id": trip_id},
                UpdateExpression=update_expression,
                ExpressionAttributeValues=expression_values,
                ConditionExpression=condition_expression,
            )
            logger.info(f"Successfully completed trip for trip_id: {trip_id}")

        except ClientError as e:
            if e.response["Error"]["Code"] == "ConditionalCheckFailedException":
                # --- NEW LOGIC FOR OUT-OF-ORDER EVENTS ---
                logger.warning(
                    f"Out-of-order event for trip_id: {trip_id}. Sending to retry queue."
                )
                try:
                    # Send the original data to the SQS queue for a delayed retry.
                    sqs_client.send_message(
                        QueueUrl=RETRY_QUEUE_URL, MessageBody=json.dumps(payload)
                    )
                except Exception as sqs_error:
                    logger.error(
                        f"FATAL: Could not send message to retry queue for trip_id {trip_id}. Error: {sqs_error}"
                    )
                    raise sqs_error
            else:
                logger.error(f"AWS ClientError on trip_id {trip_id}: {e}")
                raise e
        except Exception as e:
            logger.error(f"Error processing record on trip_id {trip_id}: {e}")
            raise e
