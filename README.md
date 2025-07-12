nsp-bolt-ride-project

# NSP Bolt Ride - Real-Time Trip Processing Project

## Table of Contents

- [NSP Bolt Ride - Real-Time Trip Processing Project](#nsp-bolt-ride---real-time-trip-processing-project)
  - [Table of Contents](#table-of-contents)
  - [Introduction](#introduction)
  - [Architecture Overview](#architecture-overview)
    - [High-Level Data Flow](#high-level-data-flow)
    - [Architectural Pattern](#architectural-pattern)
  - [Project Components](#project-components)
    - [AWS Services](#aws-services)
    - [Custom Scripts](#custom-scripts)
  - [Setup and Prerequisites](#setup-and-prerequisites)
  - [Deployment Steps](#deployment-steps)
    - [Part 1: Manual Infrastructure Setup](#part-1-manual-infrastructure-setup)
    - [Part 2: CI/CD for Application Code](#part-2-cicd-for-application-code)
  - [Simulations (Usage and Testing)](#simulations-usage-and-testing)
    - [Step 1: Data Preparation](#step-1-data-preparation)
    - [Step 2: Run the Producer](#step-2-run-the-producer)
    - [Step 3: Test the Aggregation Job](#step-3-test-the-aggregation-job)
    - [Step 4: Verify the Final Output](#step-4-verify-the-final-output)
  - [Monitoring and Operations](#monitoring-and-operations)
  - [Error Handling and Robustness](#error-handling-and-robustness)
  - [Folder Structure](#folder-structure)
  - [Conclusion](#conclusion)

## Introduction

The NSP Bolt Ride Real-Time Trip Processing Project is designed to create a scalable, event-driven data pipeline on Amazon Web Services (AWS) for processing and analyzing ride-hailing data in near real-time. This project simulates a data engineering solution for NSP Bolt Ride, to ingest, process, enrich, and analyze trip data, enabling operational and analytical insights.

The system meets the following core requirements:

- **Real-time ingestion**: Ingest trip start and trip end events through a streaming infrastructure.
- **State management**: Store and update trip data in a NoSQL database using `trip_id` as the primary key.
- **Event-driven processing**: Trigger transformation processes when a trip is marked as complete.
- **Analytics output**: Aggregate daily metrics and write them as structured JSON files to Amazon S3 for downstream analytics.

The architecture is serverless, decoupled, and production-grade, with automated deployment via CI/CD pipelines, comprehensive monitoring, and robust error handling.

## Architecture Overview

The pipeline leverages a serverless, event-driven architecture to ensure scalability, resilience, and cost efficiency. It processes data through multiple stages, with each stage triggering the next in a decoupled manner.

### High-Level Data Flow

```
[S3: Source CSVs] -> [Producer Script] -> [Kinesis Streams] -> [Processing Lambdas] -> [DynamoDB Table]
                                                                                  |
                                                                                  v
[S3: Final KPI Report] <- [EMR Serverless Spark Job] <- [S3: Staged JSON Files] <- [Trigger Lambda] <- [DynamoDB Stream]
```

### Architectural Pattern

1. **Ingestion & Validation**:

   - A Python script (`producer.py`) reads CSV files (trip start and trip end data) from an S3 bucket.
   - It validates and cleans the data, streaming valid records to two Amazon Kinesis streams (`trip-start-events` and `trip-end-events`).
   - Invalid records are quarantined in a separate S3 prefix (`data/failed/`) for manual inspection.

2. **Processing & State Management**:

   - Two AWS Lambda functions process events from the Kinesis streams independently.
   - These functions update a central Amazon DynamoDB table (`trips`) that serves as the single source of truth for trip states (e.g., `IN_PROGRESS`, `COMPLETED`).
   - The DynamoDB table uses `trip_id` as the partition key to ensure uniqueness and efficient querying.

3. **Triggering & Staging**:

   - When a trip is marked as `COMPLETED` in the DynamoDB table, a DynamoDB Stream event triggers a third Lambda function.
   - This Lambda writes the completed trip record as a JSON file to an S3 staging area (`completed-trips/`), partitioned by date (e.g., `trip_date=2024-05-25/`).

4. **Daily Batch Aggregation**:
   - An Amazon EventBridge schedule triggers a lightweight Lambda function daily at a specified time (e.g., midnight UTC).
   - This Lambda initiates an Amazon EMR Serverless job running a PySpark script (`emr_kpi_aggregator.py`).
   - The Spark job reads all JSON files from the previous day's staging area, computes key performance indicators (KPIs), and writes a consolidated JSON report to a final S3 prefix (`daily-kpis/`).

## Project Components

### AWS Services

- **Amazon S3**: Central data lake for storing source CSV files, failed records, staged JSON files, and final KPI reports.
- **Amazon Kinesis**: High-throughput streaming service for ingesting trip start and trip end events.
- **AWS Lambda**: Serverless compute service for processing Kinesis events, handling DynamoDB stream triggers, and orchestrating the EMR job.
- **Amazon DynamoDB**: NoSQL database for real-time trip state management with streaming enabled for event triggers.
- **Amazon EMR Serverless**: Serverless Spark environment for running daily aggregation jobs without cluster management.
- **Amazon EventBridge**: Schedules the daily aggregation job and monitors EMR job status for notifications.
- **Amazon SQS**: Manages a Dead-Letter Queue (DLQ) for failed records and a retry queue for out-of-order events.
- **Amazon SNS**: Sends email notifications about the status of the daily EMR job.
- **Amazon CloudWatch**: Provides logging, monitoring, dashboards, and alerting for operational visibility.
- **AWS IAM**: Ensures secure, least-privilege access for all services and CI/CD workflows.

### Custom Scripts

- **`producer.py`**: Reads source CSV files from S3, validates and cleans data, batches records, and streams them to Kinesis. Includes robust error handling and logging.
- **Lambda Functions**:
  - `process_trip_start`: Processes trip start events from the `trip-start-events` Kinesis stream and updates the DynamoDB table.
  - `process_trip_end`: Processes trip end events from the `trip-end-events` Kinesis stream, marks trips as `COMPLETED`, and updates the DynamoDB table.
  - `trigger_aggregation`: Triggered by DynamoDB Streams, writes completed trip records to S3 as JSON files.
  - `redrive_trip_end_events`: Manages out-of-order trip end events via the SQS retry queue.
  - `emr_job_trigger`: Initiates the daily EMR Serverless job via EventBridge scheduling.
- **`emr_kpi_aggregator.py`**: PySpark script that aggregates completed trip data, computes KPIs, and writes the final JSON report to S3.

## Setup and Prerequisites

To set up the project, ensure the following prerequisites are met:

- An AWS account with an IAM user or role with administrative privileges.
- AWS CLI installed and configured locally with appropriate credentials.
- Python 3.9+ installed locally with the `boto3` library (`pip install boto3`).
- A GitHub account and a repository for hosting the project code.
- Basic familiarity with AWS services (S3, Kinesis, Lambda, DynamoDB, EMR Serverless, etc.).
- Git installed for version control and CI/CD integration.

## Deployment Steps

The project uses a hybrid deployment model: foundational infrastructure is set up manually via the AWS Management Console, and application code is deployed automatically via a CI/CD pipeline using GitHub Actions.

### Part 1: Manual Infrastructure Setup

Create the following AWS resources once using the AWS Management Console:

1. **S3 Bucket**:

   - Create a bucket named `bolt-ride-kpis` (or a unique name of your choice).
   - Configure prefixes: `data/` for source CSVs, `data/failed/` for invalid records, `completed-trips/` for staged JSONs, and `daily-kpis/` for final reports.

2. **Kinesis Streams**:

   - Create two streams: `trip-start-events` and `trip-end-events`.
   - Configure each with an appropriate shard count based on expected throughput (e.g., 1 shard for testing).

3. **DynamoDB Table**:

   - Create a table named `trips` with `trip_id` as the partition key (string type).
   - Enable DynamoDB Streams with the "New and Old Images" stream view type.

4. **SQS Queues**:

   - Create a Dead-Letter Queue named `nsp-bolt-ride-dlq` for failed records.
   - Create a retry queue named `nsp-bolt-ride-retry-queue` with a 60-second delivery delay to handle out-of-order events.

5. **Lambda Functions**:

   - Create the five Lambda functions listed in [Custom Scripts](#custom-scripts).
   - Assign appropriate IAM roles with permissions for Kinesis, DynamoDB, S3, SQS, and EMR Serverless.
   - Configure triggers:
     - `process_trip_start`: Triggered by `trip-start-events` Kinesis stream.
     - `process_trip_end`: Triggered by `trip-end-events` Kinesis stream.
     - `trigger_aggregation`: Triggered by the `trips` DynamoDB Stream.
     - `redrive_trip_end_events`: Triggered by the `nsp-bolt-ride-retry-queue` SQS queue.
     - `emr_job_trigger`: Triggered by an EventBridge schedule.

6. **EMR Serverless Application**:

   - Create an application named `nsp-bolt-ride-aggregator`.
   - Assign an IAM role (`NSP-Bolt-Ride-EMR-Role`) with permissions for S3 read/write and CloudWatch logging.

7. **SNS Topic**:

   - Create a topic named `emr-job-status-notifications`.
   - Subscribe an email address for receiving job status alerts.

8. **EventBridge Rules**:
   - Create a rule to trigger the `emr_job_trigger` Lambda daily at midnight UTC.
   - Create a rule to monitor EMR Serverless job status and send notifications via the SNS topic.

### Part 2: CI/CD for Application Code

1. **GitHub Setup**:

   - Create a GitHub repository and push the project code following the [Folder Structure](#folder-structure).
   - Configure a secure OpenID Connect (OIDC) connection between your AWS account and the GitHub repository.
   - Create an IAM role (`github-actions-deployer-role`) with permissions to:
     - Update Lambda function code (`lambda:UpdateFunctionCode`).
     - Copy files to S3 (`s3:PutObject` for the `bolt-ride-kpis` bucket).
     - Pass the role to Lambda and EMR Serverless.

2. **GitHub Actions Workflow**:
   - Create a workflow file at `.github/workflows/deploy.yml.
   - The workflow should:
     - Trigger on pushes to the `main` branch.
     - Assume the `github-actions-deployer-role` via OIDC.
     - Package and deploy Lambda function code and the `emr_kpi_aggregator.py` script to S3.

## Simulations (Usage and Testing)

Follow these steps to simulate and test the pipeline.

### Step 1: Data Preparation

- Upload `trip_start.csv` and `trip_end.csv` files to the `data/` prefix in your S3 bucket (`bolt-ride-kpis/data/`).
- For testing historical aggregation, ensure the trip dates in the CSV files are set to the previous day (e.g., `2024-05-24` for a test on `2024-05-25`).
- CSV files should include fields such as `trip_id`, `start_time`, `end_time`, `rider_id`, `driver_id`, `origin`, `destination`, and other relevant attributes.

### Step 2: Run the Producer

- From your local machine, run the producer script to start the data ingestion process:
  ```bash
  python producer.py
  ```
- The script reads CSVs from S3, validates records, and streams valid data to the `trip-start-events` and `trip-end-events` Kinesis streams.
- Invalid records are written to `bolt-ride-kpis/data/failed/`.

### Step 3: Test the Aggregation Job

- To test the daily aggregation job without waiting for the EventBridge schedule:
  1. Navigate to the EMR Serverless application (`nsp-bolt-ride-aggregator`) in the AWS Console.
  2. Click **Submit job** and provide:
     - **Execution role**: The ARN of `NSP-Bolt-Ride-EMR-Role`.
     - **Script location**: The S3 path to `emr_kpi_aggregator.py` (e.g., `s3://bolt-ride-kpis/spark_scripts/emr_kpi_aggregator.py`).
     - **Script arguments**: Input path (`s3://bolt-ride-kpis/completed-trips/`) and output path (`s3://bolt-ride-kpis/daily-kpis/`).
  3. Monitor the job status in the EMR Serverless console until it completes.

### Step 4: Verify the Final Output

- Navigate to the `daily-kpis/` prefix in your S3 bucket (e.g., `s3://bolt-ride-kpis/daily-kpis/`).
- Verify the presence of a partitioned folder (e.g., `trip_date=2024-05-24/`).
- Inside the folder, confirm the existence of a JSON file containing the aggregated KPIs (e.g., total trips, average trip duration, revenue, etc.).
- Download and inspect the JSON file to ensure correctness.

## Monitoring and Operations

- **CloudWatch Dashboard**:

  - A dashboard named `NSP-Bolt-Ride-Pipeline-Health` provides a centralized view of key metrics, including:
    - Kinesis stream throughput and latency.
    - Lambda function invocation counts, errors, and durations.
    - SQS queue message counts (DLQ and retry queue).
    - DynamoDB read/write capacity and throttling.
  - Use this dashboard to monitor pipeline health and performance.

- **Alerting**:

  - An EventBridge rule monitors the EMR Serverless job status (`SUCCESS` or `FAILED`).
  - Notifications are sent to the `emr-job-status-notifications` SNS topic, which can be subscribed to via email for real-time alerts.
  - Configure additional CloudWatch alarms for critical metrics (e.g., DLQ message count > 0).

- **Logging**:
  - All Lambda functions and the producer script log to CloudWatch Logs.
  - The EMR Serverless job logs are available in the EMR Serverless console and CloudWatch Logs.

## Error Handling and Robustness

The pipeline is designed to be fault-tolerant and resilient:

- **Data Validation**:

  - The `producer.py` script validates incoming CSV records for required fields, data types, and logical consistency (e.g., `end_time` after `start_time`).
  - Invalid records are written to `s3://bolt-ride-kpis/data/failed/` with metadata for debugging.

- **Idempotency**:

  - Lambda functions use conditional writes (`ConditionExpression` in DynamoDB) to prevent duplicate event processing.
  - For example, a trip start event is only processed if the `trip_id` does not already exist in the `trips` table.

- **Out-of-Order Events**:

  - Trip end events arriving before their corresponding trip start events are sent to the `nsp-bolt-ride-retry-queue` SQS queue with a 60-second delay.
  - The `redrive_trip_end_events` Lambda function periodically processes the retry queue, attempting to reprocess events until they succeed or are sent to the DLQ.

- **Dead-Letter Queue**:
  - The `nsp-bolt-ride-dlq` SQS queue captures records that fail processing after retries (e.g., due to invalid data or service errors).
  - Failed records can be inspected and reprocessed manually after correcting issues.

## Folder Structure

```
nsp-bolt-ride-project/
│
├── .github/
│   └── workflows/
│       └── deploy.yml
│
├── data/
│   ├── trip_start.csv
│   └── trip_end.csv
│
├── src/
│   ├── process_trip_start/
│   │   └── index.py
│   ├── process_trip_end/
│   │   └── index.py
│   ├── trigger_aggregation/
│   │   └── index.py
│   ├── redrive_trip_end_events/
│   │   └── index.py
│   └── emr_job_trigger/
│       └── index.py
│
├── spark_scripts/
│   └── emr_kpi_aggregator.py
│
├── producer.py
└── README.md
```

- **`.github/workflows/`**: Contains the GitHub Actions workflow for CI/CD.
- **`data/`**: Sample CSV files for testing (`trip_start.csv`, `trip_end.csv`).
- **`src/`**: Lambda function code organized by function name.
- **`spark_scripts/`**: PySpark script for the EMR Serverless job.
- **`producer.py`**: Script for ingesting and streaming CSV data.
- **`README.md`**: This documentation file.

## Conclusion

The NSP Bolt Ride Real-Time Trip Processing Project demonstrates a robust, scalable, and production-ready data pipeline for processing ride-hailing data. By leveraging AWS serverless services, the system achieves low operational overhead, high availability, and fault tolerance. The CI/CD pipeline ensures seamless code deployment, while comprehensive monitoring and error handling maintain pipeline reliability. This project serves as a blueprint for building real-time data processing systems in a cloud-native environment.
