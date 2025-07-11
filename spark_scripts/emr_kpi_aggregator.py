import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_date, sum, count, avg, max, min
from pyspark.sql.types import IntegerType

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: emr_kpi_aggregator.py <s3_input_path> <s3_output_path>")
        sys.exit(-1)

    s3_input_path = sys.argv[1]
    s3_output_path = sys.argv[2]

    spark = SparkSession.builder.appName("Daily-KPI-Aggregation").getOrCreate()

    # --- Read all partitions from the base S3 path ---
    print(f"Processing data from S3 path: {s3_input_path}")
    try:
        source_df = spark.read.option("basePath", s3_input_path).json(s3_input_path)
    except Exception as e:
        print(
            f"Could not read from path {s3_input_path}. No data to process. Error: {e}"
        )
        spark.stop()
        sys.exit(0)

    # --- Data Transformation Step ---
    transformed_df = (
        source_df.withColumn(
            "passenger_count", col("passenger_count").cast(IntegerType())
        )
        .withColumn("payment_type", col("payment_type").cast(IntegerType()))
        .withColumn("rate_code", col("rate_code").cast(IntegerType()))
        .withColumn("trip_type", col("trip_type").cast(IntegerType()))
    )

    # --- Perform Batch Aggregation ---
    # Group by the actual date within the data
    daily_kpis = (
        transformed_df.withColumn("trip_date", to_date(col("dropoff_datetime")))
        .groupBy("trip_date")
        .agg(
            sum("fare_amount").alias("total_fare"),
            count("trip_id").alias("count_trips"),
            avg("fare_amount").alias("average_fare"),
            max("fare_amount").alias("max_fare"),
            min("fare_amount").alias("min_fare"),
        )
        .select(
            date_format(col("trip_date"), "yyyy-MM-dd").alias("trip_date"),
            "total_fare",
            "count_trips",
            "average_fare",
            "max_fare",
            "min_fare",
        )
    )

    # --- Write final report to S3, partitioned by the data's trip_date. ---
    (
        daily_kpis.repartition(1)
        .write.mode("overwrite")
        .format("json")
        .partitionBy("trip_date")
        .save(s3_output_path)
    )

    spark.stop()
