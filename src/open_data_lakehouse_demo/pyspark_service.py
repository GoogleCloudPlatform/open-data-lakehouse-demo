from asyncio import Future

import pyspark.sql.connect.functions as F
from pyspark.sql.types import (
    StringType,
    StructType,
    DoubleType,
    TimestampType,
    IntegerType,
    LongType,
    BooleanType,
)
from pyspark.sql import Row, SparkSession
from google.cloud.dataproc_spark_connect import DataprocSparkSession
from google.cloud.dataproc_v1 import Session

spark: DataprocSparkSession = None
streaming_query = None
# A future object to track the background Spark job
spark_job_future: Future = None


def start_pyspark(kafka_brokers: str, kafka_topic: str):
    global spark

    if kafka_brokers.startswith("localhost") or kafka_brokers.startswith("127.0.0.1"):
        # dev environment
        spark = SparkSession.builder.master("local[*]").appName("streaming-bus-updates-consumer").getOrCreate()
    else:
        assert kafka_brokers.startswith("bootstrap."), f"{kafka_brokers} doesn't fit to dev or prod"
        session = Session()
        session.runtime_config.properties["spark.jars.packages"] = "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0"

        # Create the Spark session. This will take some time.
        spark = (
            DataprocSparkSession.builder
            .appName("streaming-bus-updates-consumer")
            .dataprocSessionConfig(session)
            .getOrCreate()
        )

    spark.sparkContext.setLogLevel("INFO")

    bus_data_schema = (StructType()
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
                       .add("total_passengers", IntegerType()))

    # Define the schema for the incoming Kafka messages
    schema = (StructType()
        .add("id", LongType())
        .add("timestamp", TimestampType())
        .add("data", bus_data_schema))

    kafka_df = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", kafka_brokers)
                .option("subscribe", kafka_topic)
                .option("startingOffsets", "latest")
                .load())

    # Parse the JSON message from Kafka
    parsed_df = kafka_df.select(F.from_json(F.col("value").cast("string"), schema).alias("data")) \
        .select("data.*")

    # --- Your Spark Logic Here ---
    # 1. Create in-memory statistics
    # Example: Count updates per bus ID
    stats_df = (parsed_df.groupBy("bus_line_id")
                .agg(F.count("*").alias("number_of_active_busses"))
                .agg(F.sum("total_passangers").alias("number_of_active_busses"))
                )

    # This will write the aggregated stats to an in-memory table named 'bus_statistics'
    # The 'complete' output mode means the entire updated result table will be written each time.
    stats_query = stats_df.writeStream \
        .queryName("bus_statistics") \
        .outputMode("complete") \
        .format("memory") \
        .start()
