import argparse
import datetime

import pyspark.sql.functions as f
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    BooleanType,
    StructField,
)

"""
Run this job using 
```bash
gcloud dataproc batches submit pyspark gs://<PROJECT_ID>>-ridership-lakehouse/notebooks_and_code/pyspark-job.py 
--region=<REGION>> --subnet=projects/<PROJECT_ID>/regions/<REGION>/subnetworks/<REGION>-open-lakehouse-subnet 
--version=2.3 --files="gs://<PROJECT_ID>-ridership-lakehouse/notebooks_and_code/ivySettings.xml" 
--properties=^$^'spark.jars.ivySettings=./ivySettings.xml$spark.jars.packages=org.apache.spark:spark-streaming-kafka
-0-10_2.13:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,
com.google.cloud.hosted.kafka:managed-kafka-auth-login-handler:1.0.5' -- 
--kafka-brokers=bootstrap.kafka-cluster.<REGION>.managedkafka.<PROJECT_ID>.cloud.goog:9092 
--kafka-input-topic=bus-updates --kafka-alert-topic=capacity-alerts --spark-tmp-bucket=<PROJECT_ID>-dataproc-serverless 
--spark-checkpoint-location=gs://<PROJECT_ID>-dataproc-serverless/checkpoint 
--bigquery-table=ridership_lakehouse.bus_state
```
"""


# Create or update the in-memory table
def update_state(new_values, prev_state):
    bus_line = None
    total_passengers = 0
    total_capacity = 0

    # The `prev_state` argument is the grouping key `bus_line_id`, an integer.
    # The original code tried to access it as a list, causing a TypeError.
    # This function now correctly processes the `new_values` to determine the state.
    update_timestamp = datetime.datetime.now(datetime.UTC)
    for value in new_values:
        if value.last_stop:
            # Remove the bus_ride_id by returning None
            return None
        else:
            # Update the values with the latest data
            bus_line = value.bus_line
            total_passengers = value.total_passengers
            total_capacity = value.total_capacity
    return bus_line, total_passengers, total_capacity, update_timestamp

# Function to write a micro-batch to BigQuery
def write_to_bigquery(df: DataFrame, epoch_id, table, gcs_bucket):
    """
    Writes a DataFrame to a BigQuery table, overwriting it completely.
    This function is designed to be used with `forEachBatch`.
    """
    print(f"Writing to BigQuery at epoch {epoch_id}; {table}")
    print(df.collect())
    df.write \
      .format("bigquery") \
      .option("table", table) \
      .option("temporaryGcsBucket", gcs_bucket) \
      .mode("overwrite") \
      .save()

def run_pyspark(
        kafka_brokers: str, 
        kafka_input_topic: str, 
        kafka_alert_topic: str, 
        spark_tmp_bucket: str, 
        spark_checkpoint_location: str,
        bigquery_table: str):
    spark = (
        SparkSession.builder
        .config("spark.streaming.stopGracefullyOnShutdown", "true")
        .config("spark.sql.streaming.schemaInference", "true")
        
        # The packages will be provided via the gcloud command instead.
        .appName("streaming-bus-updates-consumer")
        .getOrCreate()
    )
    
    spark.sparkContext.setLogLevel("INFO")
    
    # --- The rest of your script remains exactly the same ---
    print("defining schema")
    bus_data_schema = (
        StructType()
        .add("bus_ride_id", StringType())
        .add("bus_line_id", IntegerType())
        .add("bus_line", StringType())
        .add("bus_size", StringType())
        .add("seating_capacity", IntegerType())
        .add("standing_capacity", IntegerType())
        .add("total_capacity", IntegerType())
        .add("bus_stop_id", IntegerType())
        .add("bus_stop_index", IntegerType())
        .add("num_of_bus_stops", IntegerType())
        .add("last_stop", BooleanType())
        .add("timestamp_at_stop", TimestampType())
        .add("passengers_in_stop", IntegerType())
        .add("passengers_alighting", IntegerType())
        .add("passengers_boarding", IntegerType())
        .add("remaining_capacity", IntegerType())
        .add("remaining_at_stop", IntegerType())
        .add("total_passengers", IntegerType())
    )
    
    # Define the schema for the incoming Kafka messages
    schema = (StructType()
        .add("id", LongType())
        .add("timestamp", TimestampType())
        .add("data", bus_data_schema))
    
    print("starting stream read")
    kafka_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_brokers)
                .option("kafka.security.protocol", "SASL_SSL")
                .option("kafka.sasl.mechanism", "OAUTHBEARER")
                .option("kafka.sasl.login.callback.handler.class", "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler")
                .option("kafka.sasl.jaas.config", 
                        "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;")
                .option("subscribe", kafka_input_topic)
                .option("startingOffsets", "latest")
                .load())
    
    # Parse the JSON message from Kafka
    print("parsing messages")
    parsed_df = kafka_df.select(
        f.from_json(
            f.col("value").cast("string"), schema).alias("data")) \
        .select("data.*")
    parsed_df.printSchema()
    # --- Alert Logic ---
    print("creating alert dataframe")
    alert_df = parsed_df.filter(f.col("data.remaining_at_stop") > 0)
    alert_df.printSchema()
    
    # Create a simple JSON string for the alert message.
    alert_df = alert_df.select(f.to_json(f.struct(
        "data.bus_ride_id",
        "data.bus_line",
        "data.bus_stop_id",
        "data.remaining_at_stop",
        "data.timestamp_at_stop"
    )).alias("value"))
    print("alert df json schema")
    alert_df.printSchema()
    # Write the alert messages to the alerts Kafka topic
    print("writing alerts df back to kafka")
    (alert_df.writeStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_brokers)
        .option("kafka.security.protocol", "SASL_SSL")
        .option("kafka.sasl.mechanism", "OAUTHBEARER")
        .option("kafka.sasl.login.callback.handler.class", "com.google.cloud.hosted.kafka.auth.GcpLoginCallbackHandler")
        .option("kafka.sasl.jaas.config", 
                "org.apache.kafka.common.security.oauthbearer.OAuthBearerLoginModule required;")
        .option("topic", kafka_alert_topic)
        .option("checkpointLocation", spark_checkpoint_location)
        .outputMode("append")
        .start())
    
    
    parsed_df_with_ts = parsed_df.withColumn("event_timestamp", f.col("data.timestamp_at_stop"))

    # Create a watermark to handle late data (adjust the time as needed)
    watermarked_df = parsed_df_with_ts.withWatermark("event_timestamp", "10 minutes")

    # Define the state schema
    state_schema = StructType([
        StructField("bus_line", StringType()),
        StructField("total_passengers", IntegerType()),
        StructField("total_capacity", IntegerType()),
        StructField("update_timestamp", TimestampType())
    ])

    spark.udf.register("stateful_function", update_state, state_schema)
    
    # Apply the stateful transformation
    stateful_df = watermarked_df.select("data.*") \
        .groupBy("bus_line_id") \
        .agg(f.collect_list(f.struct(
            "bus_line",
            "total_passengers",
            "total_capacity",
            "last_stop"
        )).alias("updates")) \
        .withColumn("state", f.expr("stateful_function(updates, bus_line_id)")) \
        .filter(f.col("state").isNotNull()) \
        .withColumn("bus_line", f.col("state.bus_line")) \
        .withColumn("total_passengers", f.col("state.total_passengers")) \
        .withColumn("total_capacity", f.col("state.total_capacity")) \
        .withColumn("update_timestamp", f.col("state.update_timestamp")) \
        .drop("updates", "state")
    print("stateful_df schema")
    stateful_df.printSchema()
    # Use forEachBatch to write to BigQuery, as direct streaming is not supported.
    # The 'complete' output mode ensures each micro-batch contains the full state,
    # which we then use to overwrite the BigQuery table.
    (stateful_df.writeStream
        .queryName("write_latest_bus_data_to_bq")
        .outputMode("complete")
        .option("checkpointLocation", f"{spark_checkpoint_location}/stateful_bq")
        .foreachBatch(lambda df, epoch_id: write_to_bigquery(df, epoch_id, bigquery_table, spark_tmp_bucket))
        .start())
    
    # Await termination for all streams
    spark.streams.awaitAnyTermination()


def pyspark_parse_args():
    parser = argparse.ArgumentParser(
        description="PySpark job to process bus updates from Kafka, generate alerts, and update bus state in BigQuery."
    )
    parser.add_argument(
        "--kafka-brokers",
        type=str,
        required=True,
        help="Comma-separated list of Kafka broker addresses (e.g., host1:port1,host2:port2)."
    )
    parser.add_argument(
        "--kafka-input-topic",
        type=str,
        required=True,
        default="bus-updates",
        help="Kafka topic to subscribe to for incoming bus update messages."
    )
    parser.add_argument(
        "--kafka-alert-topic",
        type=str,
        required=True,
        default="capacity-alerts",
        help="Kafka topic to publish capacity alert messages to."
    )
    parser.add_argument(
        "--spark-tmp-bucket",
        type=str,
        required=True,
        help="GCS bucket for Spark temporary files, especially for BigQuery connector."
    )
    parser.add_argument(
        "--spark-checkpoint-location",
        type=str,
        required=True,
        help="GCS path for Spark Structured Streaming checkpointing."
    )
    parser.add_argument(
        "--bigquery-table",
        type=str,
        required=True,
        help="Fully qualified BigQuery table name (e.g., project.dataset.table) to write bus state to."
    )

    return parser.parse_args()


if __name__ == "__main__":
    args = pyspark_parse_args()
    print(f"Running with args: {args}")
    run_pyspark(
        args.kafka_brokers, args.kafka_input_topic, args.kafka_alert_topic,
        args.spark_tmp_bucket, args.spark_checkpoint_location, args.bigquery_table
    )