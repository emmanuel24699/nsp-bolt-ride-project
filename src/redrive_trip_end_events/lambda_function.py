import os
import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

DESTINATION_STREAM_NAME = os.environ.get("DESTINATION_STREAM_NAME")
kinesis_client = boto3.client("kinesis")


def lambda_handler(event, context):
    for record in event["Records"]:
        try:
            payload = json.loads(record["body"])
            trip_id = payload.get("trip_id")
            if not trip_id:
                logger.error(f"Message missing trip_id, cannot redrive: {payload}")
                continue

            # Put the record back into the main Kinesis stream to be re-processed
            kinesis_client.put_record(
                StreamName=DESTINATION_STREAM_NAME,
                Data=json.dumps(payload),
                PartitionKey=trip_id,
            )
            logger.info(f"Successfully re-drove event for trip_id: {trip_id}")

        except Exception as e:
            logger.error(
                f"Failed to redrive event. Error: {e}. Payload: {record.get('body')}"
            )
            raise e
