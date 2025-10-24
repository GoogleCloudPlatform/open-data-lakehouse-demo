# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.7
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% id="bJQPVBt6OLWpJUZ1XMFjuLmo"
# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# %% [markdown] id="nWzii_MaWAoV"
# # Ridership Open Lakehouse Demo (Part 2): Simulate Bus Rides using Apache Spark
#
# This notebook will demonstrate a strategy to implement an open lakehouse on GCP, using Apache Iceberg,
# as an open source standard for managing data, while still leveraging GCP native capabilities. This demo will use
# BigQuery Managed Iceberg Tables, Managed Apache Kafka and Apache Kafka Connect to ingest streaming data, Vertex AI for Generative AI queries on top of the data and Dataplex to govern tables.
#
# This notebook will process the data to simulate bus rides. The original datasets describes `ridership` as passengers in bus stations. We created bus lines, but not the rides. This will be the goal of this notebook. We accomplish this using **Google's Cloud Serverless Apache Spark**.
#
# The processing will simulate `bus ridership` data, based on `bus station ridership` data. The `bus station ridership` shows passengers waiting at a given station at a given timestamp. Our PySpark processing pipelines will use Pandas UDFs, simulating a bus picking up those passengers while driving its route. The routes for the buses are taken from the pre-made `bus_lines` table.
#
# All data in this notebook was prepared in the previous `part0` notebook, and loaded in `part1` notebook.
#
# **Note about Iceberg catalogs, and how spark can use them:**
#
# In part 1, we loaded tables to bigquery in Iceberg format. To do so, we used 2 different kind of tables: 1) Managed Iceberg tables for BigQuery & 2) External Iceberg Tables for BigQuery.
#
# The main difference between them is that the managed tables are read-write for BigQuery, and read-only for spark (and other external engines) while the reverse is true for external tables.
#
# How to choose? If your main workloads are in BigQuery, and your datasets are created by BigQuery, use the managed tables, as BigQuery retains control on how tables are created, managed clustered etc. If your main workloads are in external engines, and are being written down to GCS by external tools like spark, use the external tables to mount the iceberg tables in BigQuery to make them readable by BigQuery to enable computation on that data.
#
# In order to read both types of tables, spark needs to access the metadata and the data itself - and it can do so in 2 ways:
#
# 1) access the data through BigQuery: This method is no different from reading BigQuery native tables, using the [Spark-BigQuery-Connector](https://github.com/GoogleCloudDataproc/spark-bigquery-connector), which is already preloaded in GCP environments (Like Colab Enterprise). This means that spark will access both metadata and data, simply by accessing the BigQuery API, and use BigQuery compute slots in order to retrieve this data. **Pros:** access any data, regardless of storage, in the same way. **Cons:** uses BigQuery slots, hence effects costs differently.
#
# 2) access the data through metadata on GCS: This method is the standard way to read Iceberg data, regardless of where it is stored - this means that we need to configure a catalog for spark, and tell it where the tables are stored in the iceberg format. Spark, using the `iceberg` libraries, will scan the catalogs for tables data and metadata. If the catalog was written by Spark, and is just accessible to BigQuery by external tables, we should be able to access the data the same way we have written it before, using the same catalog. If the data is managed by BigQuery, we just need to make sure to expose the up-to-date metadata to spark, which means **exporting the metadata** to GCS, before starting the spark process. **Pros:** Not using BigQuery slots, keeps your processing pipelines consistent and costs to a minimum for data access. **Cons:** just more configuration.
#
# In this notebook, we will access data in all different ways. We will configure the **external** catalog that was used in notebook 1 to write the `bus_lines` external table. We will configure the **BigQuery catalog** to read the `ridership` managed table. And we will read the `bus_stations` table directly through BigQuery, using BigQuery slots to do so.
#
# Ultimately, in a real-world scenario, you will probably have some BigQuery centric pipelines, writing down data in Managed Iceberg tables, backed in one GCS bucket, and some Spark centric pipelines, writing down data in external tables, backed by another GCS bucket. And maybe some BigQuery data, that is NOT accessible by Iceberg (like native tables, views etc.). This notebook will demonstrate the ability to access all types of data in spark, while the next notebook will demonstrate accessing all types of data in BigQuery - **A truly unified Data Lakehouse**.
#

# %% [markdown] id="ZwJxCNQ8YVNa"
# ## Setup environment

# %% id="t8b5qe-0v03P"
USER_AGENT = "cloud-solutions/data-to-ai-nb-v3"

# PROJECT_ID = !gcloud config get-value project
PROJECT_ID = PROJECT_ID[0]

LOCATION = "us-central1"

BQ_DATASET = "ridership_lakehouse"
BQ_CONNECTION_NAME = "cloud-resources-connection"

GENERAL_BUCKET_NAME = f"{PROJECT_ID}-ridership-lakehouse"
BQ_CATALOG_BUCKET_NAME = f"{PROJECT_ID}-iceberg-bq-catalog"
REST_CATALOG_BUCKET_NAME = f"{PROJECT_ID}-iceberg-rest-catalog"

BQ_CATALOG_PREFIX = "bq_namespace"
REST_CATALOG_PREFIX = "rest_namespace"

print(PROJECT_ID)

from google.api_core.client_info import ClientInfo

# %% id="RjO0NIXUemk8"
from google.cloud import bigquery, storage

# we will use the storage client only for demonstration purposes
storage_client = storage.Client(
    project=PROJECT_ID, client_info=ClientInfo(user_agent=USER_AGENT)
)

# we will use the bigquery client to prepare an empty table, backed by apache iceberg parquet format.
bigquery_client = bigquery.Client(
    project=PROJECT_ID, location=LOCATION, client_info=ClientInfo(user_agent=USER_AGENT)
)

# %% id="rvqNttdsR14g"
# Some helper functions

import pandas as pd

pd.set_option("display.max_colwidth", None)


def display_blobs_with_prefix(bucket_name: str, prefix: str, top=20):
    blobs = [
        [b.name, b.size, b.content_type, b.updated]
        for b in storage_client.list_blobs(
            bucket_name,
            prefix=prefix,
        )
    ]
    df = pd.DataFrame(blobs, columns=["Name", "Size", "Content Type", "Updated"])
    return df.head(top)


def delete_blobs_with_prefix(bucket_name: str, prefix: str):
    blobs = storage_client.list_blobs(bucket_name, prefix=prefix)
    for blob in blobs:
        blob.delete()


def select_top_rows(table_name: str, num_rows: int = 10):
    query = f"""
  SELECT *
  FROM `{PROJECT_ID}.{BQ_DATASET}.{table_name}`
  LIMIT {num_rows}
  """
    return bigquery_client.query(query).to_dataframe()


# %% id="3PfbmvdIsyr_"
# we will use these imports in multiple paragraphs of this notebook
import pyspark
import pyspark.sql.connect.functions as f
import pyspark.sql.types as t
from pyspark.sql import Row

# %% id="QTz61uUpus5K"
from google.cloud.dataproc_spark_connect import DataprocSparkSession
from google.cloud.dataproc_v1 import Session

session = Session()


external_catalog = "external_catalog"
bq_catalog = "bq_catalog"

# here, we're setting the options for the external catalog, where the tables are managed by spark, and read-only for bq
session.runtime_config.properties[
    f"spark.sql.catalog.{external_catalog}"
] = "org.apache.iceberg.spark.SparkCatalog"
session.runtime_config.properties[f"spark.sql.catalog.{external_catalog}.type"] = "rest"
session.runtime_config.properties[
    f"spark.sql.catalog.{external_catalog}.uri"
] = "https://biglake.googleapis.com/iceberg/v1/restcatalog"
session.runtime_config.properties[
    f"spark.sql.catalog.{external_catalog}.warehouse"
] = f"gs://{REST_CATALOG_BUCKET_NAME}"
session.runtime_config.properties[
    f"spark.sql.catalog.{external_catalog}.header.x-goog-user-project"
] = PROJECT_ID
session.runtime_config.properties[
    f"spark.sql.catalog.{external_catalog}.rest.auth.type"
] = "org.apache.iceberg.gcp.auth.GoogleAuthManager"
session.runtime_config.properties[
    f"spark.sql.catalog.{external_catalog}.io-impl"
] = "org.apache.iceberg.gcp.gcs.GCSFileIO"
session.runtime_config.properties[
    f"spark.sql.catalog.{external_catalog}.rest-metrics-reporting-enabled"
] = "false"

# here, we're setting the options for the bigquery catalog, where the tables are managed by bigquery, and read-only for spark
session.runtime_config.properties[
    f"spark.sql.catalog.{bq_catalog}"
] = "org.apache.iceberg.spark.SparkCatalog"
session.runtime_config.properties[
    f"spark.sql.catalog.{bq_catalog}.catalog-impl"
] = "org.apache.iceberg.gcp.bigquery.BigQueryMetastoreCatalog"
session.runtime_config.properties[
    f"spark.sql.catalog.{bq_catalog}.gcp_project"
] = PROJECT_ID
session.runtime_config.properties[
    f"spark.sql.catalog.{bq_catalog}.gcp_location"
] = LOCATION
session.runtime_config.properties[
    f"spark.sql.catalog.{bq_catalog}.warehouse"
] = f"gs://{BQ_CATALOG_BUCKET_NAME}"

# general packages and configuration
session.runtime_config.properties[
    "spark.jars.packages"
] = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,org.apache.iceberg:iceberg-gcp-bundle:1.10.0"
session.runtime_config.properties[
    "spark.jars"
] = "https://storage.googleapis.com/spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.42.1.jar"
session.runtime_config.properties[
    "spark.sql.extensions"
] = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"

# Create the Spark session. This will take some time.
spark = (
    DataprocSparkSession.builder.appName("simulate-bus-rides")
    .dataprocSessionConfig(session)
    .getOrCreate()
)
spark.conf.set("viewsEnabled", "true")

# %% [markdown] id="D5NrjhLyj7me"
# ## Let's take a look around
# We've created a spark session, with a couple of interesting connections. We're running a spark session with the spark-bigquery-connector, which gives us access to all BigQuery tables and Views, and also configured Iceberg catalogs (the one managed by BigLake, and the REST for managing external tables)
#
# Let's take a quick look at the **catalogs** - each containig **namespses** (also known as databases) and each namespace has **tables**. And compare the access patterns,

# %% id="6Ecb-59ZMkXp"
# This is the default catalog, named `spark_catalog`, we can see the managed tables we have in BigQuery
spark.sql(f"SHOW TABLES FROM spark_catalog.{BQ_DATASET};").toPandas()

# %% id="19qzk6NJxKH7"
# Let's take a closer look at the bus_stations table, that our spark-bigquery-connector sees
spark.sql(
    f"DESCRIBE TABLE EXTENDED spark_catalog.{BQ_DATASET}.bus_stations;"
).toPandas()

# %% id="Dm7m4HR2NZu1"
# And compare that with the bus_stations table that our BigLake catalog sees
spark.sql(f"DESCRIBE TABLE EXTENDED {bq_catalog}.{BQ_DATASET}.bus_stations;").toPandas()

# %% [markdown] id="8FflM0UEOaCT"
# Can you see the difference?
#
# There are a few differences between the 2 tables, and the way our spark reads the data. Although reading the data would produce the same logical dataset, it matters how get that data.
#
# Note that the spark-bigquery-connector version (using `spark_catalog`) says in the `Table Properties` section, that the storage layer is set to `bigquery`. This implies that in order to access the data with this table, we will go though BigQuery to get the data.
#
# Note in the second description, the `Table Properties` doesn't say that, rather says that the format is `iceberg/parquet`, and we do get the exact GCS location of the data as well. This implies that to access the data, we just let the spark-iceberg library access the data directly from GCS.
#
# The way we choose to read our data has a big impact on the pricing of access to the data, since accessing it through BQ will impact BQ slots usage.
#
# With that in mind, let's read all the datasets required, each with a different method.

# %% [markdown] id="vsqPqLS8YhHR"
# ## Load data from BigQuery
#
# For the BigQuery managed Iceberg tables, we can use 2 methods to read the data. One is the BigQuery standard way, which defers the reading of data to the BigQuery engine. This means that BigQuery slots are being used to read the data.
#
# We can also use the standard Iceberg libraries to read the data directly from GCS, and only use the BigQuery metastore to read the metadata of the iceberg files. **This will not use any BigQuery slots to read the data.** To do that, we need to export the metadata of the tables to an iceberg manifest files.
#
# In the next paragraphs we will demonstrate both methods.

# %% id="w6oXYjDzVKsW"
# Read the ridership iceberg data, from the bq catalog
ridership_df = spark.table(f"{bq_catalog}.`{BQ_DATASET}`.ridership")
ridership_df.printSchema()
ridership_df.show(5)

# %% id="Xq-ruiqQ4D_A"
# read the bus_lines data from the external catalog
bus_lines_df = spark.table(f"{external_catalog}.`{REST_CATALOG_PREFIX}`.bus_lines")
bus_lines_df.printSchema()
bus_lines_df.show(5)

# %% id="Bl3iCPGPIANj"
# To read the bus_stations data, we will use reading directly from bigquery, which uses the underlying bigquery-spark connector and uses bigquery slots to read
bus_stations_df = spark.read.format("bigquery").load(
    f"{PROJECT_ID}.{BQ_DATASET}.bus_stations"
)
bus_stations_df.printSchema()
bus_stations_df.show(5)

# %% [markdown] id="uuNx-12gYwcQ"
# ## Prepare bus data to be joined with stations ridership
#
# Our initial `ridership` table refers to passengers waiting at bus stations, along with minute-by-minute timestamps.
#
# In order to generate bus trips, picking up passengers along the way, we need to understand what are the available time ranges that we can simulate bus riders, then start simulating trips, from station to station.

# %% id="HK0qlEp46nQx"
# 1. Calculate min and max transit_timestamp for each station_id
station_time_summary_df = ridership_df.groupBy("station_id").agg(
    f.min("transit_timestamp").alias("min_station_timestamp"),
    f.max("transit_timestamp").alias("max_station_timestamp"),
)
station_time_summary_df.show(5)

# %% id="zYlqYY_n6mip"
# 2. Explode the 'stops' array in bus_lines_df to get each station_id on a new row
exploded_bus_stops_df = bus_lines_df.withColumn("station_id", f.explode("stops"))

# 3. Join the exploded bus_stops_df with the station_time_summary_df
# Use a 'left' join to keep all bus lines, even if some of their stops have no ridership data
bus_lines_with_station_times = exploded_bus_stops_df.join(
    station_time_summary_df, on="station_id", how="left"
)

# 4. Aggregate back by bus_line_id to find the overall min and max timestamp across all its stops
# we will take the max from the minimum stations, ensuring we have data for all the stations in the route of the bus line
# and the reverse for the maximum stations (take the min out of the max group)
bus_line_overall_times_df = bus_lines_with_station_times.groupBy("bus_line_id").agg(
    # The maximum of the min_station_timestamp for all stops on the line
    f.max("min_station_timestamp").alias("min_transit_timestamp"),
    # The minimum of the max_station_timestamp for all stops on the line
    f.min("max_station_timestamp").alias("max_transit_timestamp"),
)

# this will show us for each bus line, the time ranges in which we can simulate rides
bus_line_overall_times_df.show(10)

# %% id="Toh8J9nS1Qnz"
# join back to the full bus line data, to get the full context of each bus line
bus_lines_with_min_max = bus_line_overall_times_df.join(bus_lines_df, on="bus_line_id")
bus_lines_with_min_max.show(10)

# %% [markdown] id="lvgpzy5LbHfF"
# ## Simulate bus trips - without passengers (for now)

# %% id="CwdE8s1z1lHS"
from typing import List, Tuple
from datetime import datetime, timedelta
import random

# A bus ride is a tuple of:
# str: bus_ride_id - will be a composite of the bus line and the start-time
# int: bus_line_id
# str: bus_size (S, M, L)
# int: seating_capacity
# int: standing_capacity
# int: total_capacity
# int: stop_id
# int: stor_index
# datetime: stop datetime
BusRide = Tuple[str, int, str, str, int, int, int, int, int, bool, datetime]

# the equivalent pyspark structtype:
BusRideStructType = t.StructType(
    [
        t.StructField("bus_ride_id", t.StringType()),
        t.StructField("bus_line_id", t.IntegerType()),
        t.StructField("bus_line", t.StringType()),
        t.StructField("bus_size", t.StringType()),
        t.StructField("seating_capacity", t.IntegerType()),
        t.StructField("standing_capacity", t.IntegerType()),
        t.StructField("total_capacity", t.IntegerType()),
        t.StructField("bus_stop_id", t.IntegerType()),
        t.StructField("bus_stop_index", t.IntegerType()),
        t.StructField("num_of_bus_stops", t.IntegerType()),
        t.StructField("last_stop", t.BooleanType()),
        t.StructField("timestamp_at_stop", t.TimestampType()),
    ]
)

BUS_SIZE_MAP = {
    "S": (20, 20),
    "M": (30, 25),
    "L": (40, 30),
}


# Returns a list of bus_rides - this is a standard python function
# it will be wrapped to be a spark UDF
# it will be called for each bus line, that we have now the global start
# time and the last time stamp for which we have data for
# the argument `row` is a row from the bus lines described in the last paragraph
def generate_ride_times(row: t.Row) -> List[BusRide]:
    # global start time and end time (roughly 4 years of data between these timestamps)
    start_time = row.min_transit_timestamp
    end_time = row.max_transit_timestamp
    rides = []
    trips_count = 0
    # this loop will execute once while we're in the bounds of the global start and end times
    # basically each time this outer while loop runs, we will continue generating trips
    # the inner for loop will execute for each stop in one trip
    while start_time < end_time:
        # new ride id
        new_bus_ride_id = (
            f"{row.bus_line_id}_{start_time.strftime('%Y-%m-%d_%H-%M-%S')}"
        )

        # some parameters for this bus ride, choosing a size of bus
        bus_size = random.choice(list(BUS_SIZE_MAP.keys()))
        seating_capacity, standing_capacity = BUS_SIZE_MAP[bus_size]

        # this will cover all the stops in one ride, will be merged to the outer `rides` list
        one_trip_stops = []

        # stop time is the timestamp at which we arrived at a bus stop. we start a ride with the same ride time.
        stop_time = start_time

        # for each station / bus stop
        for i, stop in enumerate(row.stops):
            # if we are now past the global end time, just stop the loop
            if stop_time > end_time:
                break
            # generate one stop data
            one_trip_stops.append(
                (
                    new_bus_ride_id,
                    row.bus_line_id,
                    row.bus_line,
                    bus_size,
                    seating_capacity,
                    standing_capacity,
                    seating_capacity + standing_capacity,
                    stop,
                    i + 1,
                    len(row.stops),
                    i == len(row.stops) - 1,
                    stop_time,
                )
            )
            # move the stop_time to the next stop by small number of minutes
            stop_time = stop_time + timedelta(minutes=random.choice([1, 2, 3]))
            # End generating bus stop data, move the next station
        # finished all stops in a station - move one_trip_stops data to rides list, before it resets
        rides.extend(one_trip_stops)
        # increase trips_count
        trips_count += 1
        # move the start time to the next, but the number of the bus line frequency minutes.
        start_time = start_time + timedelta(minutes=trips_count * row.frequency_minutes)
    return rides


# wrap our function to be a spark udf
generate_ride_times_udf = f.udf(generate_ride_times, t.ArrayType(BusRideStructType))

# we now select the call to the UDF, passing in each row, and exploding the output, since it is returning a list/array
bus_rides = bus_lines_with_min_max.select(
    f.explode(generate_ride_times_udf(f.struct(*bus_lines_with_min_max.columns))).alias(
        "rides"
    )
).select("rides.*")

bus_rides.show(10)

# %% [markdown] id="pSbAjla4r6bR"
# ## Join with ridership data
#
# This is a short section, to join the `ridership` df, allowing us to see how many passengers are in each station of a trip, by bus_stop and timestamp

# %% id="Mp620fstyTRn"
# join with the ridership data, by each station_id and the timestamp
bus_rides_with_ridership = bus_rides.alias("rides").join(
    ridership_df.alias("ridership"),
    (f.col("rides.bus_stop_id") == f.col("ridership.station_id"))
    & (f.col("rides.timestamp_at_stop") == f.col("ridership.transit_timestamp")),
    "inner",
)

# drop redundant columns and rename `ridership` to `passengers_in_stop` for clarity
bus_rides_with_ridership = (
    bus_rides_with_ridership.drop("transit_timestamp")
    .drop("station_id")
    .withColumnRenamed("ridership", "passengers_in_stop")
)
# cache this, as this will be used again shortly
bus_rides_with_ridership.cache()
bus_rides_with_ridership.show(10)

# %% id="Gh0VvW-f7wkD"
bus_rides_with_ridership.printSchema()

# %% [markdown] id="w6uKurclsQqo"
# ## Aggregate passengers
#
# This section will aggregate for each ride, passengers from each station, taking into consideration the passengers "alighting" or  leaving the bus, and the overall bus capacity.
#
# This means we will take the `bus_rides_with_ridership` df, and extend it to contain more data, specifically about:
# - `passengers_alighting`: passengers departing at each stop
# - `passengers_boarding`: passengers onboarding at each stop (we can't necessarily fit all the people in a stop to our bus)
# - `remaining_capacity`: after passengers have boarded and alighted, how much room do we still have on the bus
# - `remaining_at_stop`: number of passengers who have no more room left and have to wait at the stop for the next bus
# - `total_passengers`: number of passengers on the bus, after boarding and alighting.

# %% id="pGoWARrtkpXp"
import pandas as pd

# Define the schema for the output of the Pandas UDF
# It should include all existing columns from bus_rides_with_ridership
# plus the new 'passengers_alighting' and 'total_passengers' columns.
simulated_ride_output_schema = t.StructType(
    bus_rides_with_ridership.schema.fields
    + [
        t.StructField("passengers_alighting", t.LongType(), True),
        t.StructField("passengers_boarding", t.LongType(), True),
        t.StructField("remaining_capacity", t.LongType(), True),
        t.StructField("remaining_at_stop", t.LongType(), True),
        t.StructField("total_passengers", t.LongType(), True),
    ]
)


# this python function accepts a dataframe, and returns the same.
# each call to the function will be of a dataframe representing one trip for one bus
# it will generate and calculate all the above attributes,
# and return an extended dataframe with all the relevant data
def calculate_bus_passengers(pdf: pd.DataFrame) -> pd.DataFrame:
    """
    Calculates the total number of passengers on the bus at each stop for a given bus ride.
    The number of passengers alighting is a random proportion of the boarding ridership,
    but cannot exceed the current number of passengers on the bus.
    this tracks to the logic that a central station, will have many people coming into the bus but also many people leaving the bus.
    note, that in a real-world scenario, this might be an overly simplistic heuristic, but our use case will be good enough.
    """
    import numpy as np

    # andle empty input DataFrame gracefully
    if pdf is None or pdf.empty:
        # Return an empty DataFrame with the exact schema expected by Spark
        # This prevents UnboundLocalError when a group is empty.
        empty_pdf_data = pd.DataFrame(
            columns=[c for c in simulated_ride_output_schema.fieldNames]
        )
        return pd.DataFrame(empty_pdf_data)

    # Sort the DataFrame by bus_stop_index to ensure correct iterative calculation
    pdf = pdf.sort_values(by="bus_stop_index").reset_index(drop=True)

    # Initialize passengers on the bus at the start of this ride segment
    current_passengers_on_bus = 0
    alighting_passengers_list = []
    passengers_boarding_list = []
    total_passengers_list = []
    remainig_capacity_list = []
    remaining_at_stop_list = []

    # for each bus stop along the ride
    for index, row in pdf.iterrows():
        # If we are at the last stop of this ride, all passengers are alighting and non are boarding
        if row["last_stop"]:
            passengers_alighting = current_passengers_on_bus
            passengers_boarding = 0
            passengers_in_stop = 0
            current_passengers_on_bus = 0
            remaining_at_stop = 0
        else:
            # Get passengers trying to board at this stop (passengers_in_stop), treating None as 0
            # not sure they all have room
            passengers_in_stop = (
                row["passengers_in_stop"]
                if pd.notnull(row["passengers_in_stop"])
                else 0
            )

            # Calculate random passengers alighting:
            # It's a random normal distribution, proportional to the passengers in the station (central stations have lots of people coming on and off).
            potential_alighting = int(
                np.random.normal(loc=passengers_in_stop, scale=passengers_in_stop / 4)
            )

            # Ensure alighting passengers do not exceed the number of passengers currently on the bus
            # and also ensure it's not negative.
            passengers_alighting = max(
                0, min(potential_alighting, current_passengers_on_bus)
            )

            # update the number of passengers on the bus (this will change again soon with the number of boarding passengers)
            current_passengers_on_bus = current_passengers_on_bus - passengers_alighting

            # Calculate the number of passengers currently on the bus after alighting
            temporary_capacity = row["total_capacity"] - current_passengers_on_bus

            # actual passengers on bus is the lower number from our remaining capacity after boarding, and people at the stop
            # meaning, that we will take all the passengers waiting, if we can.
            # if the capacity is lower, some will be left waiting on the bus station.
            passengers_boarding = min(temporary_capacity, passengers_in_stop)
            remaining_at_stop = passengers_in_stop - passengers_boarding

            # Passengers after boarding at this stop
            current_passengers_on_bus = current_passengers_on_bus + passengers_boarding

        # Store the results for this row
        alighting_passengers_list.append(passengers_alighting)
        total_passengers_list.append(current_passengers_on_bus)
        passengers_boarding_list.append(passengers_boarding)
        remainig_capacity_list.append(row["total_capacity"] - current_passengers_on_bus)
        remaining_at_stop_list.append(remaining_at_stop)

    # Add the new columns to the pandas DataFrame
    pdf["passengers_alighting"] = alighting_passengers_list
    pdf["total_passengers"] = total_passengers_list
    pdf["passengers_boarding"] = passengers_boarding_list
    pdf["remaining_capacity"] = remainig_capacity_list
    pdf["remaining_at_stop"] = remaining_at_stop_list
    return pdf


# Apply the Pandas UDF to the bus_rides_with_ridership DataFrame
# Group by bus_ride_id to apply the simulation independently for each ride
bus_rides_with_riders_and_totals = bus_rides_with_ridership.groupBy(
    "bus_ride_id"
).applyInPandas(calculate_bus_passengers, schema=simulated_ride_output_schema)

bus_rides_with_riders_and_totals.show(10, truncate=False)
bus_rides_with_riders_and_totals.printSchema()

# %% [markdown] id="PmA42JP2vORp"
# ## Store results to bigquery

# %% id="1qRROmbUvUgS"
TABLE_NAME = "bus_rides"

# drop an existing table, if needed
bus_rides_prefix = f"{BQ_CATALOG_PREFIX}/{TABLE_NAME}"
bigquery_client.delete_table(
    f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME}", not_found_ok=True
)
delete_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_rides_prefix)
display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_rides_prefix)

# %% id="N-_xAQMqvYr9"
bus_rides_uri = f"gs://{BQ_CATALOG_BUCKET_NAME}/{bus_rides_prefix}/"

fields = bus_rides_with_riders_and_totals.schema.fields
fields_sql = ""

for field in fields:
    type_name = field.dataType.typeName().replace("Type()", "").upper()
    if type_name == "LONG":
        type_name = "INT64"
    fields_sql += field.name + " " + type_name + ",\n"

query = f"""
CREATE TABLE {BQ_DATASET}.{TABLE_NAME}
(
  {fields_sql}
)
WITH CONNECTION `{PROJECT_ID}.{LOCATION}.{BQ_CONNECTION_NAME}`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = '{bus_rides_uri}');
"""
bigquery_client.query(query).result()

# %% id="_Eue5yezpmM4"
display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_rides_prefix)

# %% id="_V342-bis5qQ"
# save the bus_rides_with_riders_and_totals dataframe to bigquery, with iceberg format
bus_rides_with_riders_and_totals.write.format("bigquery").mode("overwrite").option(
    "table", f"{PROJECT_ID}.{BQ_DATASET}.{TABLE_NAME}"
).save()


# %% id="LSZcQp_6aFgL"
display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_rides_prefix)

# %% id="-gOiqF1Ya82X"
select_top_rows(TABLE_NAME)
