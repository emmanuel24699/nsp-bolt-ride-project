import os
import boto3
import logging
from datetime import datetime
import uuid

logger = logging.getLogger()
logger.setLevel(logging.INFO)

emr_client = boto3.client("emr-serverless")

# Get configuration from environment variables
APPLICATION_ID = os.environ.get("APPLICATION_ID")
EXECUTION_ROLE_ARN = os.environ.get("EXECUTION_ROLE_ARN")
S3_BUCKET_NAME = os.environ.get("S3_BUCKET_NAME")


def lambda_handler(event, context):
    # Generate unique job name with timestamp and UUIDs
    unique_id = (
        f"nsp_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{str(uuid.uuid4())[:8]}"
    )

    # Construct the job driver object with all necessary parameterss
    job_driver = {
        "sparkSubmit": {
            "entryPoint": f"s3://{S3_BUCKET_NAME}/scripts/emr_kpi_aggregator.py",
            "entryPointArguments": [
                f"s3://{S3_BUCKET_NAME}/completed-trips/",
                f"s3://{S3_BUCKET_NAME}/daily-kpis/",
            ],
        }
    }

    try:
        response = emr_client.start_job_run(
            applicationId=APPLICATION_ID,
            executionRoleArn=EXECUTION_ROLE_ARN,
            jobDriver=job_driver,
            name=unique_id,
        )
        job_run_id = response["jobRunId"]
        logger.info(
            f"Successfully started EMR Serverless job run with ID: {job_run_id}"
        )
        return {"status": "SUCCESS", "jobRunId": job_run_id}

    except Exception as e:
        logger.error(f"Error starting EMR job run: {e}")
        raise e
