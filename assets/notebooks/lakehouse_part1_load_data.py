# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.14.7
#   kernelspec:
#     display_name: Python 3 (ipykernel)
#     language: python
#     name: python3
# ---

# %% id="3rDg9KfcBdJP"
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

# %% [markdown] id="Hbol"
# # Ridership Open Lakehouse Demo (Part 1): Load data to BigQuery Iceberg tables
#
# This notebook will demonstrate a strategy to implement an open lakehouse on GCP, using Apache Iceberg,
# as an open source standard for managing data, while still leveraging GCP native capabilities. This demo will use
# BigQuery Manged Iceberg Tables, Managed Apache Kafka and Apache Kafka Connect to ingest streaming data, Vertex AI for Generative AI queries on top of the data and Dataplex to govern tables.
#
# This notebook will load data into BigQuery, backed by Parquet files, in the Apache Iceberg specification.
#
# If you created the demo, using our terraform scripts, you should be able to access the data files under the `<YOUR_PROJECT_ID>-ridership-lakehouse` bucket, as the terraform script copies over the data files from a publicly available bucket `gs://data-lakehouse-demo-data-assets/staged-data/`.
#
# If you haven't created this demo using our terraform scripts, the easiest way to get a hold of the data would be to run the following `gsutil` command:
#
# ```bash
# gsutil -m rsync -r gs://data-lakehouse-demo-data-assets/  gs://<YOUR_BUCKET_NAME>/
# ```
#
# All data in this notebook was prepared in the previous `part0` notebook.

# %% [markdown] id="MJUe"
# ## Setup the environment

# %% id="vblA"
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

# %% id="OO8ccqmVrDdd"
# !pip install fastavro --quiet

# %% id="lEQa"
from google.cloud import bigquery, storage
from google.api_core.client_info import ClientInfo

bigquery_client = bigquery.Client(
    project=PROJECT_ID, location=LOCATION, client_info=ClientInfo(user_agent=USER_AGENT)
)
storage_client = storage.Client(
    project=PROJECT_ID, client_info=ClientInfo(user_agent=USER_AGENT)
)

general_bucket = storage_client.bucket(GENERAL_BUCKET_NAME)
print(bigquery_client.project)
print(general_bucket.exists())

# %% id="ugEQEeAtZ1n1"
# create/reference the bq dataset, and clean all tables
dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{BQ_DATASET}")
dataset_ref.location = LOCATION

# %% id="DAJqwAKqaz5j"
# WARNING: don't auto-run this cell, it contains destructive actions
# create/reference the bq dataset
# deletes all the tables
dataset = bigquery_client.create_dataset(dataset_ref, exists_ok=True)

for table in bigquery_client.list_tables(dataset):
    bigquery_client.delete_table(table)

# %% id="OWx9xx9aWpGu"
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


# %% [markdown] id="z2TGzD069Kwp"
# ## Create the tables and load data

# %% [markdown] id="vyt1ywHoviSE"
# ### Different types of Iceberg tables in BigQuery
#
# BigQuery offers two ways to work with Apache Iceberg tables:
#
# 1. **[BigQuery Tables for Apache Iceberg](https://cloud.google.com/bigquery/docs/iceberg-tables#create-iceberg-tables)**.
# 2. **[BigLake metastore with the Iceberg REST catalog](https://cloud.google.com/bigquery/docs/blms-rest-catalog)**
#
# For most migration and native BigQuery use cases, **BigQuery Tables for Apache Iceberg (managed by BigQuery) is the strongly preferred method.**
#
# -----
#
# **1\. BigQuery Tables for Apache Iceberg (Managed by BigQuery)**
#
#
# **This is the recommended approach for migrating your data and integrating Iceberg within BigQuery.** These tables offer full BigQuery management of Iceberg, eliminating the need for a separate catalog.
#
# **SQL Example:**
#
# ```sql
# CREATE OR REPLACE TABLE `your-project.your_dataset.your_iceberg_table`(
#     <column_definition>
# )
# WITH CONNECTION `your-region.your_connection_name`
# OPTIONS (
#     file_format = 'PARQUET',
#     table_format = 'ICEBERG',
#     storage_uri = 'gs://your-bucket/iceberg/your_table_name'
# );
# ```
#
# **Why should you prefer Managed Tables:**
#
# BigQuery-managed Iceberg tables unlock powerful features essential for modern data solutions:
#
#   * **Native Integration:** Seamless experience, similar to standard BigQuery tables.
#   * **Full DML Support:** Perform `INSERT`, `UPDATE`, `DELETE`, `MERGE` directly with GoogleSQL.
#   * **Unified Ingestion:** Supports both batch and high-throughput streaming via the Storage Write API.
#   * **Schema Evolution:** BigQuery handles schema changes (add, drop, rename columns, type changes) effortlessly.
#   * **Automatic Optimization:** Benefits from BigQuery's built-in optimizations like adaptive file sizing, clustering, and garbage collection.
#   * **Robust Security:** Leverage BigQuery's column-level security and data masking.
#   * **Simplified Operations:** Reduced overhead by letting BigQuery manage the Iceberg table lifecycle.
#
# This method provides a more robust, integrated, and efficient way to leverage Iceberg data within the BigQuery ecosystem.
#
#
# -----
#
# **2\. BigLake metastore with the Iceberg REST catalog**
#
#
# These tables allow BigQuery to query Iceberg data managed by external systems like Spark or Hive. They are best for hybrid setups where multiple tools need read access and an external system controls the table's lifecycle.
#
# **SQL Example:**
#
# ```sql
# CREATE OR REPLACE EXTERNAL TABLE `your-project.your_dataset.your_external_iceberg_table`
#   WITH CONNECTION `your-region.your_connection_name`
#   OPTIONS (
#          format = 'ICEBERG',
#          uris = ["gs://mybucket/mydata/mytable/metadata/iceberg.metadata.json"]
#    )
# ```
#
# **Key Points:**
#
#   * **External Control:** Metadata and data managed outside BigQuery.
#   * **Read-Only:** BigQuery can only query; DML operations are not supported.
#   * **Hybrid Fit:** Ideal for shared access from various tools.
#   * **Metadata:** Manual updates for static JSON pointers; BigLake Metastore preferred for dynamic syncing in GCP.
#
#
# #### How to Choose?
#
# Generally, to leverage the most out of you Iceberg data, prefer the managed tables. They provide better integration and automatic optimization.
#
# If you have BigQuery centric pipelines, with data generated by BigQuery, managed iceberg tables are the obvious choice.
#
# Choose external tables, if you have spark centric pipelines (or another external engine) that generate and write Iceberg data in GCS, and BigQuery only requires read-only access.
#
# In a real world scenario, you will probably have some of both, so a truly unified data platform would have a mixture of both tables.
#
# In this notebook, we will create the 2 different types of tables, to demonstrate that the 2 methods can be combined according to your needs.
#
# We will generate managed tables for the `bus_stations` and `ridership` datasets, while for the `bus_lines` dataset, we will write iceberg data directly to GCS, using Apache Spark, and mount the data as an external table in BigQuery.
#

# %% [markdown] id="GDsTeoKVcA2a"
# ### The `bus_stations` table
#
# This table will be loaded as a BigQuery Iceberg table (option 2)- managed by BigQuery, read-only access to other processing engines.
#

# %% id="Xref"
bus_stops_prefix = f"{BQ_CATALOG_PREFIX}/bus_stations"
bus_stops_uri = f"gs://{BQ_CATALOG_BUCKET_NAME}/{bus_stops_prefix}/"

# Clear the GCS path before
delete_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_stops_prefix)
display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_stops_prefix)

# %%
# drop the table
bigquery_client.query(f"DROP TABLE IF EXISTS {BQ_DATASET}.bus_stations;").result()

# create the table
query = f"""
CREATE TABLE {BQ_DATASET}.bus_stations
(
  bus_stop_id INTEGER,
  address STRING,
  school_zone BOOLEAN,
  seating BOOLEAN,
  borough STRING,
  latitude FLOAT64,
  longitude FLOAT64
)
WITH CONNECTION `{PROJECT_ID}.{LOCATION}.{BQ_CONNECTION_NAME}`
OPTIONS (
  file_format = 'PARQUET',
  table_format = 'ICEBERG',
  storage_uri = '{bus_stops_uri}');
"""
bigquery_client.query(query).result()

# %%
# We can view the GCS path, and see that there is now an ICEBERG metadata file, but no data
display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_stops_prefix)

# %% id="pzbgTsLImHtN"
import json

# Let's review the metadata file that we have
file = list(
    storage_client.list_blobs(
        BQ_CATALOG_BUCKET_NAME,
        match_glob=f"{bus_stops_prefix}/metadata/*.metadata.json",
    )
)[0]
metadata = file.download_as_string()
print(json.loads(metadata.decode("utf-8")))

# %% [markdown] id="Lr7eXVQicRVW"
# Not much there. Just the fact that we have a table, and not much more.
#
# We'll now load data into the table, and see what happens.

# %% id="qjUf5Qlbma05"
# we will now load the data from the CSV in GCS

# BQ tables for Apache Iceberg do not support load with truncating, so we will truncate manually, and then load
truncate = bigquery_client.query(f"DELETE FROM {BQ_DATASET}.bus_stations WHERE TRUE")
truncate.result()

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    source_format=bigquery.SourceFormat.CSV,
    skip_leading_rows=1,
)

job = bigquery_client.load_table_from_uri(
    f"gs://{GENERAL_BUCKET_NAME}/staged-data/bus_stations.csv",
    dataset.table("bus_stations"),
    job_config=job_config,
)

job.result()

# %% id="rggvYQ7ApP2N"
# We can verify that the data is actually loaded in the iceberg specification and the format used is parquet
display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_stops_prefix)

# %% [markdown] id="NBGqkxosXcie"
# We can see in the output that we have some parquet files generated under the `iceberg_data/bus_stations/data/` folder, and one `v0.metadata.json` under the `iceberg_data/bus_stations/metadata/` folder.
#
# The `iceberg_data/bus_stations/data` folder, contains the `parquet` files with the actual data.
#
# The `iceberg_data/bus_stations/metadata` folder contains the metadata files - currently only one - but more data will be in there, as our tables evolve (schema changes, rollbacks etc.).
#
# The `v0.metadata.json` file, which can have other prefixes, contains the important info for this version of the table. Let's take a quick look at this file.

# %% id="Xs0MIDDxYLjI"
file = list(
    storage_client.list_blobs(
        BQ_CATALOG_BUCKET_NAME,
        match_glob=f"{bus_stops_prefix}/metadata/*.metadata.json",
    )
)[0]
metadata = file.download_as_string()
print(json.loads(metadata.decode("utf-8")))

# %% [markdown] id="FO69Buz5wf4J"
# Still not much there, but we'll compare this content to a file generated by our `EXTERNAL` catalog, to see the differences.
#
# While the data was loaded to the table, and it is available to BigQuery - currently, the fact we loaded the data, **DOES NOT** trigger a metadata refresh
#
# So, let's verify that the data is available, and then trigger a metadata refresh.
#
# **REMEMBER** the fact that the metadata json file isn't updated, doesn't mean that the metadata is not kept in BigQuery internal engine. We just need to trigger a metadata refresh to make the metadata available to other engines.

# %% id="n0Ep6-y5nm86"
select_top_rows("bus_stations")

# %% id="kxKg_T0OojWp"
# Now let's refresh the metadata
bigquery_client.query(f"EXPORT TABLE METADATA FROM {BQ_DATASET}.bus_stations").result()

# %% id="ZMC327HUo0Z-"
# Now let's take a closer look at the metadata folder only
display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_stops_prefix + "/metadata")

# %% [markdown] id="Rv-DM4YSpKId"
# We now basically see, what happens when we create a metadata snapshot - iceberg creates a new snapshot of the metadata.
#
# We also see a `version-hint.text` file to help us find the most up-to-date version of the metadata.
#
# Let's take a closer look:

# %%
from pprint import pprint

all_metadata_blobs = list(
    storage_client.list_blobs(
        BQ_CATALOG_BUCKET_NAME, match_glob=f"{bus_stops_prefix}/metadata/*"
    )
)

version_hint_blob = list(
    filter(lambda x: x.name.endswith("version-hint.text"), all_metadata_blobs)
)[0]
version_hint = version_hint_blob.download_as_string().decode("utf-8")

# The version hint just has the right version, so now we know which json file to look for
print(f"Latest Version of metadata: {version_hint}")
print("")
print("-" * 20)

latest_json_file = list(
    filter(
        lambda x: x.name.endswith(f"v{version_hint}.metadata.json"), all_metadata_blobs
    )
)[0]
latest_json = json.loads(latest_json_file.download_as_string().decode("utf-8"))
print(f"Latest metadata from our file metadata (v{version_hint}.metadata.json):")
print("")
pprint(latest_json)

# %%
# Definitely much more metadata!!!!
# One last thing before we continue, let's take a look at the avro files
# in the json file, we see one of them being mentioned (the "manifest-list")
# but not the other. Let's take a look at the manifest-list file

import fastavro

manifest_list_file = list(
    filter(lambda x: "manifest-list-000" in x.name, all_metadata_blobs)
)[0]

with manifest_list_file.open("rb") as fo:
    avro_reader = fastavro.reader(fo)
    for record in avro_reader:
        pprint(record)

# %% id="Aixw99UVu-9f"
# We can see the metadata that was generated when we loaded the data through BigQuery
# Number of files that we saw (parquet files)
# rows added etc.

# we also see a reference to the other avro file - so let's take a look there:

avro_file = list(
    filter(
        lambda x: "manifest-list-000" not in x.name and x.name.endswith(".avro"),
        all_metadata_blobs,
    )
)[0]

with avro_file.open("rb") as fo:
    avro_reader = fastavro.reader(fo)
    for record in avro_reader:
        pprint(record)

# %% [markdown] id="ofBoYgs90zqI"
# So, now the picture is complete - this is how Apache iceberg can keep track on all the data in our table. We can see each data file listed here, with some metadata.
#
# we saw the manifest list file hold overall metadata about the snapshot of the data we took
#
# and saw how it is all connected in the `json` file.
#
# This is how Iceberg is able to operate, and give every processing engine a way to read the data in a unified way.

# %% [markdown] id="Z5vzIsf4UnnM"
# ### The `bus_lines` table
#
# For the `bus_lines` table, we want to simulate a table that is managed by Spark, and BigQuery is just needs to read the table.
#
# For that we will use the `EXTERNAL` Iceberg tables (method 2), managed by OSS engines, read-only by BigQuery.
#
# To simulate that, we will start a PySpark process to read the data from the staged bucket, and write it back in Iceberg format to another bucket
#
# Then mount the data in a BigQuery external table.
#
# We will also look at the Iceberg metadata generated by spark, which will show us the similarities to the BigQuery metadata we just saw.
#
# The first step is to create a REST catalog, using the **`biglake`** API.

# %% id="GH9LHM82Vmez"
import json

# access_token = !gcloud auth application-default print-access-token
access_token = access_token[0]

# catalog_metadata = !curl -H "x-goog-user-project: {PROJECT_ID}" \
#   -H "Accept: application/json" \
#   -H "Authorization: Bearer {access_token}" \
#   https://biglake.googleapis.com/iceberg/v1/restcatalog/v1/config?warehouse=gs://{REST_CATALOG_BUCKET_NAME}

catalog_metadata = json.loads("".join(catalog_metadata))
catalog_metadata

# %% [markdown] id="SHgBenFkecwX"
# Now that we have a rest catalog, we can start up a spark session, with the required configurations

# %%
from google.cloud.dataproc_spark_connect import DataprocSparkSession
from google.cloud.dataproc_v1 import Session

session = Session()

catalog_name = "external_catalog"

session.runtime_config.properties[
    f"spark.sql.catalog.{catalog_name}"
] = "org.apache.iceberg.spark.SparkCatalog"
session.runtime_config.properties[f"spark.sql.catalog.{catalog_name}.type"] = "rest"
session.runtime_config.properties[
    f"spark.sql.catalog.{catalog_name}.uri"
] = "https://biglake.googleapis.com/iceberg/v1/restcatalog"
session.runtime_config.properties[
    f"spark.sql.catalog.{catalog_name}.warehouse"
] = f"gs://{REST_CATALOG_BUCKET_NAME}"
session.runtime_config.properties[
    f"spark.sql.catalog.{catalog_name}.header.x-goog-user-project"
] = PROJECT_ID
session.runtime_config.properties[
    f"spark.sql.catalog.{catalog_name}.rest.auth.type"
] = "org.apache.iceberg.gcp.auth.GoogleAuthManager"
session.runtime_config.properties[
    f"spark.sql.catalog.{catalog_name}.io-impl"
] = "org.apache.iceberg.gcp.gcs.GCSFileIO"
session.runtime_config.properties[
    f"spark.sql.catalog.{catalog_name}.rest-metrics-reporting-enabled"
] = "false"
session.runtime_config.properties[
    "spark.sql.extensions"
] = "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
session.runtime_config.properties["spark.sql.defaultCatalog"] = catalog_name


# Create the Spark session. This will take some time.
spark: DataprocSparkSession = (
    DataprocSparkSession.builder.appName("mount-bus-lines")
    .dataprocSessionConfig(session)
    .getOrCreate()
)

# %%
# In spark, we need to create a namespace to work with our catalog.
spark.sql(f"CREATE NAMESPACE IF NOT EXISTS `{REST_CATALOG_PREFIX}`;")
spark.sql(f"USE `{REST_CATALOG_PREFIX}`;")

# Read the staged data from the original bucket
df = spark.read.format("parquet").load(
    f"gs://{GENERAL_BUCKET_NAME}/staged-data/bus_lines/"
)

# and write it back to the Iceberg catalog as a table.
df.write.format("iceberg").mode("overwrite").saveAsTable(
    f"{REST_CATALOG_PREFIX}.bus_lines"
)


# %% id="H77pWZzhYjZ0"
# Now we can see the data written in our EXTERNAL catalog bucket.
display_blobs_with_prefix(REST_CATALOG_BUCKET_NAME, REST_CATALOG_PREFIX)

# %% [markdown] id="DQO5bIO0i46Y"
# Like before, we can see a similar structure - with some differences
#
# - The top folder, `rest-namespace` is just a reference to the namespace we created in Spark
# - Under the namespace `rest-namespace`, we can see a `bus_lines` folder, referencing our table, and then the familiar structure of a `data` folder, and a `metadata` folder.
# - The files under the `data` folder, are still `parquet` files
# - The files under the `metadata` folder looks slightly different, since Spark was the one creating them, and not BQ, but their purpose is the same. we can repeat the process of reading the json file, which will point to the `snap-` avro file, and so on and so on.
#
# Iceberg is the layer that enables interoperability between processing engines.
#
# Now, we just need to "mount" the `bus_lines` Iceberg Data in BigQuery:

# %% id="9kxivahUxGPe"
metadata_blob = list(
    storage_client.list_blobs(
        REST_CATALOG_BUCKET_NAME,
        match_glob=f"{REST_CATALOG_PREFIX}/bus_lines/metadata/*.metadata.json",
    )
)[0]

bigquery_client.query(
    f"""
CREATE OR REPLACE EXTERNAL TABLE `{BQ_DATASET}.bus_lines`
  WITH CONNECTION `{PROJECT_ID}.{LOCATION}.{BQ_CONNECTION_NAME}`
  OPTIONS (
         format = 'ICEBERG',
         uris = ["gs://{REST_CATALOG_BUCKET_NAME}/{metadata_blob.name}"]
   )
"""
).result()

# %% id="QOgaM-h_GNix"
# show sample rows
select_top_rows("bus_lines")

# %% id="CydSvZ5rleMq"
# now that we confirmed that the data is available, we're done with Spark, so we can stop the session
spark.stop()

# %% [markdown] id="BYtC"
# ### The `ridership` table
#
# Lastly, the `ridership` table will be loaded just like the `bus_stations` table, but this time we will [cluster](https://cloud.google.com/bigquery/docs/clustered-tables) the table by the timestamp.

# %%
ridership_prefix = f"{BQ_CATALOG_PREFIX}/ridership/"
ridership_uri = f"gs://{BQ_CATALOG_BUCKET_NAME}/{ridership_prefix}"


delete_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, ridership_prefix)

bigquery_client.query(f"DROP TABLE IF EXISTS {BQ_DATASET}.ridership;").result()

_create_table_stmt = f"""
    CREATE TABLE {BQ_DATASET}.ridership (
        transit_timestamp TIMESTAMP,
        station_id INTEGER,
        ridership INTEGER
    )
    CLUSTER BY transit_timestamp
    WITH CONNECTION `{PROJECT_ID}.{LOCATION}.{BQ_CONNECTION_NAME}`
    OPTIONS (
        file_format = 'PARQUET',
        table_format = 'ICEBERG',
        storage_uri = '{ridership_uri}'
    );
"""
bigquery_client.query(_create_table_stmt).result()

# %%
# Load data into the table
table_ref = dataset_ref.table("ridership")

# BQ tables for Apache Iceberg do not support load with truncating, so we will truncate manually, and then load
truncate = bigquery_client.query(f"DELETE FROM {BQ_DATASET}.ridership WHERE TRUE")
truncate.result()

job_config = bigquery.LoadJobConfig(
    write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    source_format=bigquery.SourceFormat.PARQUET,
)

job = bigquery_client.load_table_from_uri(
    f"gs://{GENERAL_BUCKET_NAME}/staged-data/ridership/*.parquet",
    table_ref,
    job_config=job_config,
)

job.result()

# Export the metadata
bigquery_client.query(f"EXPORT TABLE METADATA FROM {BQ_DATASET}.ridership").result()

# %% id="R3couPLLQffj"
# show sample rows
select_top_rows("ridership")

# %% id="Sb9SM9u6uanm"
display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, ridership_prefix + "data/")

# %% id="eWXX4g3vA0IR"
display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, ridership_prefix + "metadata")

# %% [markdown]
# # Conclusion
#
# In this notebook, we created our tables, loaded data - and observed what happens on GCS when managing Iceberg tables either by BigQuery or by other engines like Spark.
#
# We saw the metadata and examined how Iceberg keeps pointer to the actual data and manages schemas and metadata.
#
# In the next notebook, we will use Apache Spark to do some data processing, and focus our attention on how to read data from the different catalogs, and the pros and cons for each method of reading the data.
