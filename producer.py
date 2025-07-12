import boto3
import csv
import json
import logging
import random
from pydantic import (
    BaseModel,
    ValidationError,
    constr,
    confloat,
    conint,
    field_validator,
)
from datetime import datetime, timezone
import io
from typing import Optional

# --- Configuration ---
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
REGION_NAME = "us-east-1"
S3_BUCKET_NAME = "bolt-ride-kpis"
SOURCE_DATA_PREFIX = "data/"
FAILED_RECORDS_PREFIX = "failed-records/"
TRIP_START_STREAM_NAME = "trip-start-events"
TRIP_END_STREAM_NAME = "trip-end-events"
BATCH_SIZE = 1

# --- Boto3 Clients ---
s3_client = boto3.client("s3")
kinesis_client = boto3.client("kinesis", region_name=REGION_NAME)


# --- Pydantic Schemas ---
class TripStart(BaseModel):
    trip_id: constr(min_length=1)
    pickup_location_id: constr(min_length=1)
    dropoff_location_id: constr(min_length=1)
    vendor_id: conint(ge=1)
    pickup_datetime: datetime
    estimated_dropoff_datetime: datetime
    estimated_fare_amount: confloat(ge=0)


class TripEnd(BaseModel):
    trip_id: constr(min_length=1)
    dropoff_datetime: datetime
    rate_code: Optional[conint(ge=0)] = None
    passenger_count: Optional[conint(ge=0)] = None
    trip_distance: confloat(ge=0)
    fare_amount: confloat(ge=0)
    tip_amount: confloat(ge=0)
    payment_type: Optional[conint(ge=0)] = None
    trip_type: Optional[conint(ge=0)] = None

    @field_validator(
        "rate_code", "passenger_count", "payment_type", "trip_type", mode="before"
    )
    @classmethod
    def empty_str_to_none(cls, v):
        if v == "":
            return None
        return v


# --- Helper Functions (unchanged) ---
def clean_row(row: dict) -> dict:
    for key, value in row.items():
        if value == "":
            row[key] = None
    return row


def capture_failed_records_to_s3(records, original_filename):
    if not records:
        return
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d-%H%M%S")
    s3_key = f"{FAILED_RECORDS_PREFIX}{original_filename}-failures-{timestamp}.json"
    content = "\n".join([json.dumps(rec, default=str) for rec in records])
    try:
        s3_client.put_object(Bucket=S3_BUCKET_NAME, Key=s3_key, Body=content)
        logging.warning(
            f"Captured {len(records)} failed records to s3://{S3_BUCKET_NAME}/{s3_key}"
        )
    except Exception as e:
        logging.error(f"FATAL: Could not write failed records to S3. Error: {e}")


def send_batch(stream_name, batch, original_filename):
    if not batch:
        return
    try:
        response = kinesis_client.put_records(StreamName=stream_name, Records=batch)
        if response.get("FailedRecordCount", 0) > 0:
            failed_records = []
            for i, record in enumerate(response["Records"]):
                if "ErrorCode" in record:
                    failed_records.append(
                        {
                            "error": f"{record['ErrorCode']}: {record['ErrorMessage']}",
                            "data": batch[i],
                        }
                    )
            if failed_records:
                capture_failed_records_to_s3(failed_records, original_filename)
        logging.info(
            f"Successfully sent batch of {len(batch)} records to {stream_name}."
        )
    except Exception as e:
        logging.error(f"Failed to send entire batch to {stream_name}: {e}")
        capture_failed_records_to_s3(
            [{"error": str(e), "data": record} for record in batch], original_filename
        )


# --- Main Execution Block with Randomized Streaming ---
if __name__ == "__main__":
    logging.info("Starting randomized cloud-native data stream simulation...")

    trip_start_key = f"{SOURCE_DATA_PREFIX}trip_start.csv"
    trip_end_key = f"{SOURCE_DATA_PREFIX}trip_end.csv"

    try:
        # Get and prepare both CSV readers from S3
        start_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=trip_start_key)
        end_obj = s3_client.get_object(Bucket=S3_BUCKET_NAME, Key=trip_end_key)

        start_reader = csv.DictReader(
            start_obj["Body"].read().decode("utf-8").splitlines()
        )
        end_reader = csv.DictReader(end_obj["Body"].read().decode("utf-8").splitlines())

        start_batch, end_batch = [], []
        processed_trip_ids = set()

        # Flags to track if streams are still active
        start_stream_active = True
        end_stream_active = True

        while start_stream_active or end_stream_active:
            # Determine which stream to pull from
            choices = []
            if start_stream_active:
                choices.append("start")
            if end_stream_active:
                choices.append("end")

            if not choices:
                break  # Both streams are exhausted

            source_choice = random.choice(choices)

            if source_choice == "start":
                row = next(start_reader, None)
                if row:
                    trip_id = row.get("trip_id")
                    if trip_id not in processed_trip_ids:
                        cleaned_row = clean_row(row)
                        try:
                            TripStart.model_validate(cleaned_row)
                            start_batch.append(
                                {
                                    "Data": json.dumps(cleaned_row),
                                    "PartitionKey": trip_id,
                                }
                            )
                            processed_trip_ids.add(trip_id)
                        except ValidationError as e:
                            logging.error(f"[START] Validation failed: {e}")
                            capture_failed_records_to_s3(
                                [{"error": str(e), "data": cleaned_row}],
                                "trip_start.csv",
                            )
                else:
                    start_stream_active = False  # Mark stream as finished

            elif source_choice == "end":
                row = next(end_reader, None)
                if row:
                    cleaned_row = clean_row(row)
                    try:
                        TripEnd.model_validate(cleaned_row)
                        end_batch.append(
                            {
                                "Data": json.dumps(cleaned_row),
                                "PartitionKey": row["trip_id"],
                            }
                        )
                    except ValidationError as e:
                        logging.error(f"[END] Validation failed: {e}")
                        capture_failed_records_to_s3(
                            [{"error": str(e), "data": cleaned_row}], "trip_end.csv"
                        )
                else:
                    end_stream_active = False  # Mark stream as finished

            # Send batches when they are full
            if len(start_batch) >= BATCH_SIZE:
                send_batch(TRIP_START_STREAM_NAME, start_batch, "trip_start.csv")
                start_batch = []

            if len(end_batch) >= BATCH_SIZE:
                send_batch(TRIP_END_STREAM_NAME, end_batch, "trip_end.csv")
                end_batch = []

        # Send any remaining records
        if start_batch:
            send_batch(TRIP_START_STREAM_NAME, start_batch, "trip_start.csv")
        if end_batch:
            send_batch(TRIP_END_STREAM_NAME, end_batch, "trip_end.csv")

    except Exception as e:
        logging.error(f"A fatal error occurred during initialization: {e}")

    logging.info("Finished data stream simulation.")
