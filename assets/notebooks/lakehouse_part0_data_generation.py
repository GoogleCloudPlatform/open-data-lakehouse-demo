# %%
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

# %% [markdown]
# # Ridership Open Lakehouse Demo (Part 0): Generating our datasets
#
# This notebook will demonstrate a strategy to implement an open lakehouse on GCP, using Apache Iceberg,
# as an open source standard for managing data, while still leveraging GCP native capabilities. This demo will use
# BigQuery Manged Iceberg Tables, Managed Apache Kafka and Apache Kafka Connect to ingest streaming data, Vertex AI for Generative AI queries on top of the data and Dataplex to govern tables.
#
# This notebook will generate fake data and anonymized real-world data.
#
# the real-world data used in this notebook is from [MTA daily ridership data](https://data.ny.gov/Transportation/MTA-Daily-Ridership-Data-2020-2025/vxuj-8kew/data_preview).
#
# Rest of the data is being randomly generated inside the notebook.

# %% [markdown]
# ## Setup the environment

# %%
import os

USER_AGENT = "cloud-solutions/data-to-ai-nb-v3"

# PROJECT_ID = !gcloud config get-value project
PROJECT_ID = PROJECT_ID[0]
STAGING_BQ_DATASET = "ridership_lakehouse_staging"
BUCKET_NAME = f"{PROJECT_ID}-ridership-lakehouse"
LOCATION = "us-central1"
BQ_CONNECTION_NAME = "cloud-resources-connection"

print(PROJECT_ID)
print(BUCKET_NAME)

# %%
# !pip install faker sodapy --quiet

# %%
from google.cloud import bigquery, storage
from google.api_core.client_info import ClientInfo
from google.cloud import exceptions

bigquery_client = bigquery.Client(
    project=PROJECT_ID, location=LOCATION, client_info=ClientInfo(user_agent=USER_AGENT)
)
storage_client = storage.Client(
    project=PROJECT_ID, client_info=ClientInfo(user_agent=USER_AGENT)
)

# %%
dataset = bigquery.Dataset(f"{PROJECT_ID}.{STAGING_BQ_DATASET}")
dataset.location = LOCATION
dataset = bigquery_client.create_dataset(dataset, exists_ok=True)

# %% [markdown]
# ## MTA Data - PLEASE READ!
#
# This part is tricky - this is the raw data from the MTA subways of new york.
# The MTA website and API are slow... very slow. Checking the [hourly ridership data](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-2020-2024/wujg-7c2s/about_data), we have about 110 million records to get.
#
# If you created the demo, using our terraform scripts, you should be able to access the MTA raw CSV under the bucket `<YOUR_PROJECT_ID>-ridership-lakehouse`, as the terraform script copies over the CSV from a publicly available bucket `gs://data-lakehouse-demo-data-assets/mta-raw/`.
#
# If you haven't created this demo using our terraform scripts, the easiest way to get a hold of the data would be to run the following `gsutil` command:
#
# ```bash
# gsutil -m rsync -r gs://data-lakehouse-demo-data-assets/  gs://<YOUR_BUCKET_NAME>/
# ```
#
# Downloading the CSV manually is the more efficient option, but still very slow, so you would have to send the request, and keep your machine and browser awake for a few hours (yes, hours, was about 2 hours in my case) before the CSV starts downloading.
#
# for programmatic download using the API, the situation might be worse. the API is prone to timeouts. The default records limit per request is 1,000, and the maximum is 50,000, which means we have to do chunking of API calls, but the latency is increasing expo. when increasing the limit.
#
# I've written the function to download the data and write each request to be appended to a file, but this ran for 4 hours, and got around 8% of the data, before I gave up.
#
# The next cell has the function to download the data using the API, but the call to the function is commented out, since it is very slow to run.
#
# the cell after that allows you to fill in the path to GCS, so, whichever method you want to get the data, just make sure, that the variable `MTA_RAW_CSV` points to a valid and accessible path on GCS that holds the MTA hourly ridership data.
#
# happy thoughts!

# %%
from csv import DictWriter
from sodapy import Socrata

FILENAME = "raw-mta-data.csv"
fieldnames = [
    "transit_timestamp",
    "transit_mode",
    "station_complex_id",
    "station_complex",
    "borough",
    "payment_method",
    "fare_class_category",
    "ridership",
    "transfers",
    "latitude",
    "longitude",
    "georeference",
    ":@computed_region_kjdx_g34t",
    ":@computed_region_yamh_8v7k",
    ":@computed_region_wbg7_3whc",
]

# This function will use the api to download the data into a csv locally
# Note that the final size of the CSV would be around 17GB
# also note that the API is slow and prone to timeout errors
# this is why it has a back-off mechanism where we start with the
# maximum records allowed and back off whenever we have a timeout error
# as mentioned above, a much easier approach would be to go to the website
# and download the CSV manually (although that takes some time as well)
# then upload the CSV to GCS
# This method is here for convenience and is not currently being called.

# this method was planned to be able to resume from where it last stopped
# we have the option to read an existing CSV that we started downloading
# if you want to ignore the existing file, you can delete it manually or just flip this flag
FORCE_CLEAR_DATA = False

# I got this number after downloading the full file and looking at it.
# Since this should be a static dataset, the number shouldn't change over time
TOTAL_NUMBER_OF_RECORDS = 110_696_370
STEP = 50_000


def programmatically_download_mta_data():
    import requests

    client = Socrata("data.ny.gov", None)

    # is there is current file already exists
    existing_file = os.path.exists(FILENAME)

    if not existing_file or FORCE_CLEAR_DATA:
        # if we no current file exists, or flag to ignore it is raised
        rows_got = 0
        with open(FILENAME, "w") as mta_fp:
            mta_writer = DictWriter(mta_fp, fieldnames=fieldnames)
            # write headers
            mta_writer.writeheader()
    else:
        with open(FILENAME, "r") as f:
            # read how many records we have already (minus 1 for headers)
            rows_got = sum((1 for _ in f)) - 1
        print(
            f"""Starting from existing data. already got {rows_got:,}
        records ({round(rows_got / TOTAL_NUMBER_OF_RECORDS * 100, 2)}%)"""
        )
    current_step = STEP
    # while the number of rows we got is smaller than the total number of rows expected
    while rows_got < TOTAL_NUMBER_OF_RECORDS:
        try:
            # get more data
            results = client.get("wujg-7c2s", limit=current_step, offset=rows_got)
        except requests.exceptions.ReadTimeout:
            # in case of a timeout, ask for less data
            current_step = current_step - 1000
            print(f"Got timeout, adjusting limit to {current_step}")
        else:
            # when we get data, append it to the file.
            with open(FILENAME, "a") as mta_fp:
                mta_writer = DictWriter(mta_fp, fieldnames=fieldnames)
                mta_writer.writerows(results)
            rows_got = rows_got + len(results)
            print(
                f"Got {rows_got:,} rows so far ({round(rows_got / TOTAL_NUMBER_OF_RECORDS * 100, 2)}%)"
            )

    # TODO: implement code to upload the local CSV to GCS


# %%
# This should be a path pointing to the file in GCS.
MTA_RAW_CSV_PATH_IN_GCS = (
    "mta-raw/mta-manual-downloaded-data_MTA_Subway_Hourly_Ridership.csv"
)
MTA_RAW_CSV = f"gs://{BUCKET_NAME}/{MTA_RAW_CSV_PATH_IN_GCS}"

bucket = storage_client.bucket(BUCKET_NAME)
_mta_raw_csv_blob = bucket.get_blob(MTA_RAW_CSV_PATH_IN_GCS)

if _mta_raw_csv_blob:
    print(f"Path '{MTA_RAW_CSV}' found")
else:
    raise ValueError(
        f"Path '{MTA_RAW_CSV}' doesn't appear to point to a valid GCS object"
    )

# %%
# Load raw data to a BQ, to continue transformation of the data
# Due to its size, it will be easier to load data to bq and transform it there

from google.cloud.bigquery import SchemaField

# schema for the original raw file
mta_schema = [
    # Note that the timestamp col is loaded as string, due to US format, which is not automatically detected by BQ
    SchemaField("transit_timestamp", "STRING"),
    SchemaField("transit_mode", "STRING"),
    SchemaField("station_complex_id", "STRING"),
    SchemaField("station_complex", "STRING"),
    SchemaField("borough", "STRING"),
    SchemaField("payment_method", "STRING"),
    SchemaField("fare_class_category", "STRING"),
    SchemaField("ridership", "INTEGER"),
    SchemaField("transfers", "INTEGER"),
    SchemaField("latitude", "FLOAT"),
    SchemaField("longitude", "FLOAT"),
    SchemaField("Georeference", "GEOGRAPHY"),
]

BQ_TABLE = "raw_mta_data"

table_ref = dataset.table(BQ_TABLE)

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    schema=mta_schema,
)
load_job = bigquery_client.load_table_from_uri(
    MTA_RAW_CSV, table_ref, job_config=job_config
)
load_job.result()

print("created {}.{}".format(STAGING_BQ_DATASET, BQ_TABLE))


# %% [markdown]
# ### The `mta_data_stations` table
#
# the raw data has a non-normalized dataset, where each station appear in full detail, for each hourly data point
# we will create a table just for the stations, and we will reference the `station_id` from the thinner time-series table.
#
# note, we are saving the results to a pandas dataframe, to be used later

# %%
bigquery_client.query(
    f"DROP TABLE IF EXISTS {STAGING_BQ_DATASET}.mta_data_stations;"
).result()

_query = f"""
CREATE TABLE {STAGING_BQ_DATASET}.mta_data_stations AS
SELECT
  CAST(REPLACE(station_complex_id, 'TRAM', '98765') AS INT64) AS station_id,
  station_complex,
  borough,
  latitude,
  longitude,
FROM
  (
    SELECT
      *,
      ROW_NUMBER() OVER (PARTITION BY station_complex_id ORDER BY transit_timestamp ASC) as rn
    FROM
      `{STAGING_BQ_DATASET}.raw_mta_data`
  )
WHERE
  rn = 1;
"""
bigquery_client.query(_query).result()

stations_df = bigquery_client.query(
    f"SELECT * FROM {STAGING_BQ_DATASET}.mta_data_stations;"
).to_dataframe()
stations_df

# %% [markdown]
# ### The `mta_data_parsed` table - temporary table
#
# This table is a temporary table to hold an hourly data points, with parsed timestamps and station IDs as integers.

# %%
bigquery_client.query(
    f"DROP TABLE IF EXISTS {STAGING_BQ_DATASET}.mta_data_parsed;"
).result()

_query = f"""
    CREATE TABLE `{STAGING_BQ_DATASET}.mta_data_parsed` AS
        SELECT
            PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', transit_timestamp) AS `transit_timestamp`,
            CAST(REPLACE(station_complex_id, 'TRAM', '98765') AS INT64) AS `station_id`,
            SUM(ridership) as ridership
        FROM `{STAGING_BQ_DATASET}.raw_mta_data`
        GROUP BY PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', transit_timestamp),
        CAST(REPLACE(station_complex_id, 'TRAM', '98765') AS INT64);
"""
bigquery_client.query(_query).result()
bigquery_client.query(
    f"SELECT * FROM {STAGING_BQ_DATASET}.mta_data_parsed LIMIT 20;"
).to_dataframe()

# %% [markdown]
# ### The `ridership` table
#
# This is the output table to hold minute-by-minute data points, spreading each hour evenly between 60 minutes within the hour.

# %%
bigquery_client.query(f"DROP TABLE IF EXISTS {STAGING_BQ_DATASET}.ridership;").result()
_query = f"""
    CREATE TABLE `{STAGING_BQ_DATASET}.ridership` AS
    SELECT
        TIMESTAMP_ADD(t.transit_timestamp, INTERVAL minute_offset MINUTE) AS transit_timestamp,
        t.station_id,
        CAST(ROUND(
            (FLOOR(t.ridership / 60)) +
            CASE
                WHEN minute_offset < MOD(t.ridership, 60) THEN 1
                ELSE 0
            END
        ) AS INTEGER) AS ridership
    FROM {STAGING_BQ_DATASET}.mta_data_parsed AS t,
    UNNEST(GENERATE_ARRAY(0, 59)) AS minute_offset
    ORDER BY station_id, transit_timestamp;
"""
bigquery_client.query(_query).result()
bigquery_client.query(
    f"SELECT * FROM {STAGING_BQ_DATASET}.ridership LIMIT 20;"
).to_dataframe()

# %%
# This query is just a verification that the sum of each hour in our minute-by-minute data equals to the data in
# the temporary hourly data
# The query re-aggregates the data by hour, and compars to the original hourly data
# it should return 0 rows, as it filters for hours, where the sum of ridership in an hour doesn't equal the original
# data.
_query = f"""
    WITH minutly_agg AS (
        SELECT
            TIMESTAMP_TRUNC(transit_timestamp, hour) AS transit_timestamp,
            station_id,
            SUM(ridership) AS ridership_agg
        FROM `{STAGING_BQ_DATASET}.ridership`
        GROUP BY TIMESTAMP_TRUNC(transit_timestamp, hour), station_id
    )
    SELECT
        minutly_agg.transit_timestamp,
        minutly_agg.station_id,
        minutly_agg.ridership_agg,
        parsed.ridership
    FROM minutly_agg
    JOIN `{STAGING_BQ_DATASET}.mta_data_parsed` as parsed
        ON minutly_agg.transit_timestamp = parsed.transit_timestamp AND
            minutly_agg.station_id = parsed.station_id
    WHERE minutly_agg.ridership_agg != parsed.ridership"""
bigquery_client.query(_query).to_dataframe()

# %% [markdown]
# ## Generate fake data for bus lines and bus stops (routes)
#
# This is based on the stations data in thr original dataset, but the station addresses are faked (using faker),
# and then routes are being constructed just by picking randomly from the stations list. We're using `random.sample`
# to select a range but without duplicates (as that might create a circular route for a bus, which is not a typical route)
#
# We will then load the faked data into BigQuery, in order to create time windows of the ridership (that represents people waiting in stations) to simulate bus riders, accumulating riders into a bus driving through their stations.

# %%
from faker import Faker
import numpy as np

Faker.seed(42)
fake = Faker()

import random
from typing import List


def generate_bus_line(i: int, stations_ids: List[int]) -> dict:
    # randomize a number of stops for this line, with the mean of 35, and roughly between 30 and 40
    number_of_stops = int(np.random.normal(loc=35, scale=2))
    return {
        "bus_line_id": i + 1,
        "bus_line": fake.unique.bothify("?-###").upper(),
        "number_of_stops": number_of_stops,
        "stops": random.sample(stations_ids, k=number_of_stops),
        "frequency_minutes": random.choice([5, 10, 15, 20]),
    }


def fakify_station(station: dict) -> dict:
    return {
        "bus_stop_id": station["station_id"],
        "address": fake.unique.address().split(",")[0].replace("\n", ", "),
        "school_zone": fake.boolean(),
        "seating": fake.boolean(),
        "borough": station["borough"],
        "latitude": station["latitude"],
        "longitude": station["longitude"],
    }


stations_lst = stations_df.to_dict("records")
fake_stations_lst = [fakify_station(station) for station in stations_lst]
stations_ids = [station["bus_stop_id"] for station in fake_stations_lst]
BUS_LINES_NUM = 25
bus_lines = [generate_bus_line(i, stations_ids) for i in range(BUS_LINES_NUM)]

random_bus_line = random.choice(bus_lines)
print(
    f"Generated {len(bus_lines)} random bus lines. One for example: {random_bus_line}"
)
print(
    f"Anonymized {len(fake_stations_lst)} bus_stations. The first stations for the random bus line above is: {next(filter(lambda x: x['bus_stop_id'] == random_bus_line['stops'][0], fake_stations_lst))}"
)

# %%
with open("bus_stations.csv", "w") as fp:
    writer = DictWriter(fp, fieldnames=fake_stations_lst[0].keys())
    writer.writeheader()
    writer.writerows(fake_stations_lst)

import json

with open("bus_lines.json", "w") as fp:
    for line in bus_lines:
        json.dump(line, fp)
        fp.write("\n")

# %% [markdown]
# we will load the `bus_lines` to BigQuery as an ICEBERG table, and then export it with the ICEBERG manifest and data files. This will enable us to simulate in the next notebook to mount ICEBERG tables coming from external sources.

# %%
bus_lines_schema = [
    SchemaField("bus_line_id", "INTEGER"),
    SchemaField("bus_line", "STRING"),
    SchemaField("number_of_stops", "INTEGER"),
    SchemaField("stops", "INTEGER", mode="REPEATED"),
    SchemaField("frequency_minutes", "INTEGER"),
]

BQ_TABLE = "bus_lines"

dataset = bigquery.Dataset(f"{PROJECT_ID}.{STAGING_BQ_DATASET}")


table_ref = dataset.table(BQ_TABLE)

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    schema=bus_lines_schema,
)
with open("bus_lines.json", "rb") as fp:
    load_job = bigquery_client.load_table_from_file(
        fp, table_ref, job_config=job_config
    )
    load_job.result()

print("created {}.{}".format(STAGING_BQ_DATASET, BQ_TABLE))

# %%
bus_stations_schema = [
    SchemaField("bus_stop_id", "INTEGER"),
    SchemaField("address", "STRING"),
    SchemaField("school_zone", "BOOLEAN"),
    SchemaField("seating", "BOOLEAN"),
    SchemaField("borough", "STRING"),
    SchemaField("latitude", "NUMERIC"),
    SchemaField("longitude", "NUMERIC"),
]

BQ_TABLE = "bus_stations"

dataset = bigquery.Dataset(f"{PROJECT_ID}.{STAGING_BQ_DATASET}")


table_ref = dataset.table(BQ_TABLE)

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
    schema=bus_stations_schema,
)
with open("bus_stations.csv", "rb") as fp:
    load_job = bigquery_client.load_table_from_file(
        fp, table_ref, job_config=job_config
    )
    load_job.result()

print("created {}.{}".format(STAGING_BQ_DATASET, BQ_TABLE))


# %% [markdown]
# ## Extract data to GCS
#
# Now, that we have all data we need, the data needs to reside in BigQuery & GCS, we will extract the BigQuery tables to GCS, so we can load them in the next notebook, where we build the open source data lakehouse.
#
# - `bus_stations` - we will just use our CSV we built locally, and upload it as is to GCS.
#
# - `bus_lines` - we will export the data as `parquet` format, to be read by spark in the next notebook, to demonstrate iceberg **external** tables.
#
# - `ridership` - this table is the largest by far (972,829,500 rows). We will export this table to parquet format to be loaded as a managed iceberg table.

# %%
# bus stations
bus_stations_blob = bucket.blob("mta_staging_data/bus_stations.csv")
if bus_stations_blob.exists():
    bus_stations_blob.delete()
bus_stations_blob.upload_from_filename("bus_stations.csv")

# %%
from google.cloud.bigquery import ExtractJobConfig

target_glob = "mta_staging_data/bus_lines"
destination_uri = "gs://{}/{}/*.parquet".format(BUCKET_NAME, target_glob)

blob_list = bucket.list_blobs(prefix=target_glob)
blob_list = [blob for blob in blob_list]
bucket.delete_blobs(blob_list)

job_config = ExtractJobConfig()
job_config.destination_format = bigquery.DestinationFormat.PARQUET

table_ref_1 = dataset.table("bus_lines")
extract_job = bigquery_client.extract_table(
    table_ref_1, destination_uri, location=LOCATION, job_config=job_config
)
extract_job.result()

# %%
target_glob = "mta_staging_data/ridership"
destination_uri = "gs://{}/{}/*.parquet".format(BUCKET_NAME, target_glob)

blob_list = bucket.list_blobs(match_glob=target_glob)
blob_list = [blob for blob in blob_list]
bucket.delete_blobs(blob_list)

job_config = ExtractJobConfig()
job_config.destination_format = bigquery.DestinationFormat.PARQUET

table_ref_1 = dataset.table("ridership")
extract_job = bigquery_client.extract_table(
    table_ref_1, destination_uri, location=LOCATION, job_config=job_config
)
extract_job.result()

# %%
try:
    for table in bigquery_client.list_tables(dataset):
        bigquery_client.delete_table(table)
    bigquery_client.delete_dataset(dataset)
except exceptions.NotFound:
    print("Dataset looks already dropped")
