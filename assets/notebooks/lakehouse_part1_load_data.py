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
# # Ridership Open Lakehouse Demo (Part 1): Load data to BigQuery Iceberg tables
#
# This notebook will demonstrate a strategy to implement an open lakehouse on GCP, using Apache Iceberg,
# as an open source standard for managing data, while still leveraging GCP native capabilities. This demo will use
# BigQuery Managed Iceberg Tables, the Lakehouse Runtime Catalog (Apache Iceberg REST Catalog with Credential Vending), Serverless for Apache Spark, Managed Service for Apache Kafka and Apache Kafka Connect to ingest streaming data, BigQuery AI (`AI.FORECAST` powered by TimesFM) for forecasting, and Knowledge Catalog to govern tables, lineage, and data quality.
#
# This notebook will load data into BigQuery, backed by Parquet files, in the Apache Iceberg specification.
#
# If you created the demo, using our terraform scripts, you should be able to access the data files under the `<YOUR_PROJECT_ID>-ridership-lakehouse` bucket, as the terraform script copies over the data files from a publicly available bucket `gs://data-lakehouse-demo-data-assets/staged-data/`.
#
# If you haven't created this demo using our terraform scripts, the easiest way to get a hold of the data would be to run the following `gcloud storage` command:
#
# ```bash
# gcloud storage cp --recursive --no-clobber "gs://data-lakehouse-demo-data-assets/*" "gs://<YOUR_BUCKET_NAME>/"
# ```
#
# All data in this notebook was prepared in the previous `part0` notebook.

# %% [markdown]
# ## Setup the environment

# %%
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

# %%
# !pip install fastavro google-cloud-dataplex --quiet

# %%
from google.cloud import bigquery, storage

bigquery_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
storage_client = storage.Client(project=PROJECT_ID)

general_bucket = storage_client.bucket(GENERAL_BUCKET_NAME)
print(bigquery_client.project)
print(general_bucket.exists())

# %%
# create/reference the bq dataset, and clean all tables
dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{BQ_DATASET}")
dataset_ref.location = LOCATION

# %%
# WARNING: don't auto-run this cell, it contains destructive actions
# create/reference the bq dataset
# deletes all the tables
dataset = bigquery_client.create_dataset(dataset_ref, exists_ok=True)

for table in bigquery_client.list_tables(dataset):
    bigquery_client.delete_table(table)

# %%
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


# %% [markdown]
# ## Create the tables and load data

# %% [markdown]
# ### Different types of Iceberg tables in BigQuery
#
# BigQuery offers two ways to work with Apache Iceberg tables:
#
# 1. **[BigQuery Tables for Apache Iceberg](https://cloud.google.com/bigquery/docs/iceberg-tables#create-iceberg-tables)** (*Lakehouse for Apache Iceberg* — managed by BigQuery).
# 2. **[Lakehouse Runtime Catalog (Apache Iceberg REST Catalog with Credential Vending)](https://cloud.google.com/bigquery/docs/blms-rest-catalog)** — queried dynamically in BigQuery via 4-part `P.C.N.T` syntax.
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
# **2\. Lakehouse Runtime Catalog (Apache Iceberg REST Catalog with Credential Vending)**
#
#
# These tables allow BigQuery to dynamically query Iceberg data managed by external engines such as Serverless for Apache Spark, Trino, or Flink without static `metadata.json` pointers. With **Credential Vending** (`CREDENTIAL_MODE_VENDED_CREDENTIALS`), only the catalog's managed service account holds GCS bucket access; authorized engines receive short-lived, table-scoped storage tokens.
#
# **SQL Example (4-part `project.catalog.namespace.table` syntax):**
#
# ```sql
# SELECT *
# FROM `your-project.your-catalog-id.your_namespace.your_iceberg_table`
# LIMIT 10;
#
# -- Optional compatibility view for downstream SQL and apps:
# CREATE OR REPLACE VIEW `your-project.your_dataset.your_iceberg_table` AS
# SELECT *
# FROM `your-project.your-catalog-id.your_namespace.your_iceberg_table`;
# ```
#
# **Key Points:**
#
#   * **External Control:** Metadata and data managed outside BigQuery via the open Iceberg REST Catalog API.
#   * **Dynamic Snapshot Resolution:** Always reads the latest committed snapshot with zero manual `metadata.json` refreshes.
#   * **Least-Privilege Credential Vending:** Table-level catalog IAM (`roles/biglake.viewer`) replaces broad GCS bucket permissions.
#   * **Automatic Knowledge Catalog Discovery:** Catalogs, namespaces, and tables register automatically in **Knowledge Catalog**.
#
#
# #### How to Choose?
#
# Generally, to leverage the most out of your Iceberg data, prefer the managed tables. They provide better integration and automatic optimization.
#
# If you have BigQuery-centric pipelines, with data generated by BigQuery, managed Iceberg tables are the obvious choice.
#
# Choose the **Lakehouse Runtime Catalog** when you have Spark-centric pipelines (or other external engines) that write Iceberg data to GCS and want dynamic, governed read access from BigQuery via 4-part `P.C.N.T` naming.
#
# In a real-world scenario, you will probably use both, so a truly unified data platform combines both table types.
#
# In this notebook, we will create both types of tables to demonstrate how they work together:
#
# We will create BigQuery Managed Iceberg tables for `bus_stations` and `ridership`, while for `bus_lines` we will write Apache Iceberg data using **Serverless for Apache Spark** into our Terraform-provisioned **Lakehouse Runtime Catalog** (with **Credential Vending**) and expose it dynamically in BigQuery via a 4-part `P.C.N.T` view.
#

# %% [markdown]
# ### The `bus_stations` table
#
# This table will be loaded as a BigQuery Managed Iceberg table (option 1) — managed by BigQuery, with read-only access for external processing engines.
#

# %%
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

# %%
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

# %% [markdown]
# Not much there. Just the fact that we have a table, and not much more.
#
# We'll now load data into the table, and see what happens.

# %%
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

# %%
# We can verify that the data is actually loaded in the iceberg specification and the format used is parquet
display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_stops_prefix)

# %% [markdown]
# We can see in the output that we have some parquet files generated under the `bq_namespace/bus_stations/data/` folder, and one `v0.metadata.json` under the `bq_namespace/bus_stations/metadata/` folder.
#
# The `bq_namespace/bus_stations/data` folder contains the `parquet` files with the actual data.
#
# The `bq_namespace/bus_stations/metadata` folder contains the metadata files - currently only one - but more data will be in there as our tables evolve (schema changes, rollbacks, etc.).
#
# The `v0.metadata.json` file, which can have other prefixes, contains the important info for this version of the table. Let's take a quick look at this file.

# %%
file = list(
    storage_client.list_blobs(
        BQ_CATALOG_BUCKET_NAME,
        match_glob=f"{bus_stops_prefix}/metadata/*.metadata.json",
    )
)[0]
metadata = file.download_as_string()
print(json.loads(metadata.decode("utf-8")))

# %% [markdown]
# Still not much there, but we'll compare this content to a file generated by our `EXTERNAL` catalog, to see the differences.
#
# While the data was loaded to the table, and it is available to BigQuery - currently, the fact we loaded the data, **DOES NOT** trigger a metadata refresh
#
# So, let's verify that the data is available, and then trigger a metadata refresh.
#
# **REMEMBER** the fact that the metadata json file isn't updated, doesn't mean that the metadata is not kept in BigQuery internal engine. We just need to trigger a metadata refresh to make the metadata available to other engines.

# %%
select_top_rows("bus_stations")

# %%
# Now let's refresh the metadata
bigquery_client.query(f"EXPORT TABLE METADATA FROM {BQ_DATASET}.bus_stations").result()

# %%
# Now let's take a closer look at the metadata folder only
display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_stops_prefix + "/metadata")

# %% [markdown]
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

# %%
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

# %% [markdown]
# So, now the picture is complete - this is how Apache iceberg can keep track on all the data in our table. We can see each data file listed here, with some metadata.
#
# we saw the manifest list file hold overall metadata about the snapshot of the data we took
#
# and saw how it is all connected in the `json` file.
#
# This is how Iceberg is able to operate, and give every processing engine a way to read the data in a unified way.

# %% [markdown]
# ### The `bus_lines` table
#
# For the `bus_lines` table, we simulate a table managed by **Serverless for Apache Spark** where BigQuery reads the latest committed snapshot dynamically.
#
# For that we use the **Lakehouse Runtime Catalog** (option 2 — Apache Iceberg REST Catalog), pre-provisioned by Terraform with **Credential Vending** (`CREDENTIAL_MODE_VENDED_CREDENTIALS`).
#
# To simulate that, we start a Serverless for Apache Spark Connect session (with native **OpenLineage** enabled for **Knowledge Catalog**), read the staged Parquet files, write `bus_lines` in Iceberg format using vended credentials, and expose it in BigQuery via 4-part `P.C.N.T` syntax and a live View.
#
# First, let's inspect the REST catalog configuration returned by the `biglake.googleapis.com` Iceberg REST endpoint.

# %%
import json

# access_token = !gcloud auth application-default print-access-token
access_token = access_token[0]

# catalog_metadata = !curl -s -H "x-goog-user-project: {PROJECT_ID}" \
#   -H "Accept: application/json" \
#   -H "X-Iceberg-Access-Delegation: vended-credentials" \
#   -H "Authorization: Bearer {access_token}" \
#   "https://biglake.googleapis.com/iceberg/v1/restcatalog/v1/config?warehouse=gs://{REST_CATALOG_BUCKET_NAME}"

catalog_metadata = json.loads("".join(catalog_metadata))
catalog_metadata

# %% [markdown]
# Now that we have verified our Lakehouse Runtime Catalog config, let's start a **Serverless for Apache Spark** session (via Spark Connect) with **Credential Vending** and native **OpenLineage** capture enabled.

# %%
import sys
from google.cloud.dataproc_spark_connect import DataprocSparkSession
from google.cloud.dataproc_v1 import Session

session = Session()

# Match the Serverless for Apache Spark runtime Python version to the notebook kernel
# (Runtime 4.0 = Python 3.13, Runtime 3.0 = Python 3.12, Runtime 2.3 = Python 3.11).
_py_minor = sys.version_info[:2]
session.runtime_config.version = (
    "4.0" if _py_minor >= (3, 13) else "3.0" if _py_minor == (3, 12) else "2.3"
)
print(
    f"Using Serverless Spark runtime {session.runtime_config.version} "
    f"for Python {_py_minor[0]}.{_py_minor[1]}"
)

catalog_name = "external_catalog"

# Lakehouse Runtime Catalog (Apache Iceberg REST Catalog) — GA v1 endpoint.
# Note: If a specific Spark runtime does not emit Iceberg lineage edges against v1,
# switch the URI below to https://biglake.googleapis.com/iceberg/v1beta/restcatalog.
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
    f"spark.sql.catalog.{catalog_name}.header.X-Iceberg-Access-Delegation"
] = "vended-credentials"
session.runtime_config.properties[
    f"spark.sql.catalog.{catalog_name}.gcs.oauth2.refresh-credentials-endpoint"
] = "https://oauth2.googleapis.com/token"
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

# Native Serverless for Apache Spark OpenLineage -> Knowledge Catalog.
# Do not manually set spark.extraListeners here; spark.dataproc.lineage.enabled
# automatically injects the OpenLineage listener and gcplineage transport.
session.runtime_config.properties["spark.dataproc.lineage.enabled"] = "true"
session.runtime_config.properties[
    "spark.openlineage.namespace"
] = "ridership_lakehouse_demo"


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


# %%
# Now we can see the data written in our EXTERNAL catalog bucket.
display_blobs_with_prefix(REST_CATALOG_BUCKET_NAME, REST_CATALOG_PREFIX)

# %% [markdown]
# Like before, we can see a similar structure - with some differences
#
# - The top folder, `rest_namespace`, references the namespace we created in Spark.
# - Under `rest_namespace`, we see a `bus_lines` folder with the familiar `data` and `metadata` folders.
# - The files under the `data` folder are still `parquet` files.
# - The files under the `metadata` folder look slightly different because Spark wrote them instead of BigQuery, but their structure and purpose are identical (`metadata.json` -> `snap-*.avro` -> manifest Avro files).
#
# Iceberg is the layer that enables interoperability between processing engines.
#
# Now we query `bus_lines` directly from BigQuery using **4-part `P.C.N.T` syntax** (`project.catalog.namespace.table`) and expose a live BigQuery View (`ridership_lakehouse.bus_lines`) — eliminating fragile static `metadata.json` external table pointers.

# %%
# Query the Spark-managed Iceberg table directly via 4-part P.C.N.T syntax.
# Wrapping the full 4-part identifier in backticks safely handles hyphenated IDs.
bigquery_client.query(
    f"""
SELECT *
FROM `{PROJECT_ID}.{REST_CATALOG_BUCKET_NAME}.{REST_CATALOG_PREFIX}.bus_lines`
LIMIT 10
"""
).to_dataframe()

# %%
# Create a live BigQuery View so downstream notebooks and the Cloud Run dashboard
# always read the latest committed Iceberg snapshot without updating static pointers.
bigquery_client.query(
    f"""
CREATE OR REPLACE VIEW `{PROJECT_ID}.{BQ_DATASET}.bus_lines` AS
SELECT *
FROM `{PROJECT_ID}.{REST_CATALOG_BUCKET_NAME}.{REST_CATALOG_PREFIX}.bus_lines`
"""
).result()

# %%
# show sample rows
select_top_rows("bus_lines")

# %%
# now that we confirmed that the data is available, we're done with Spark, so we can stop the session
spark.stop()

# %% [markdown]
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

# %%
# show sample rows
select_top_rows("ridership")

# %%
display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, ridership_prefix + "data/")

# %%
display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, ridership_prefix + "metadata")

# %% [markdown]
# ## Verify Automatic Discovery in Knowledge Catalog
#
# Both **BigQuery Managed Iceberg Tables** (`bus_stations`, `ridership`) and **Lakehouse Runtime Catalog** assets (`bus_lines`) are automatically registered in **Knowledge Catalog** alongside the Terraform-provisioned Business Glossary (`ridership-lakehouse-glossary`).

# %%
from google.cloud import dataplex_v1

catalog_client = dataplex_v1.CatalogServiceClient()

discovered_rows = []
for table_query in ["bus_lines", "bus_stations", "ridership"]:
    request = dataplex_v1.SearchEntriesRequest(
        name=f"projects/{PROJECT_ID}/locations/global",
        scope=f"projects/{PROJECT_ID}",
        query=table_query,
        page_size=10,
    )
    for result in catalog_client.search_entries(request=request):
        entry = result.dataplex_entry
        discovered_rows.append(
            {
                "query": table_query,
                "entry_type": entry.entry_type.split("/")[-1],
                "fully_qualified_name": entry.fully_qualified_name,
                "resource": entry.entry_source.resource if entry.entry_source else "",
                "entry_name": entry.name,
            }
        )

print(
    f"Knowledge Catalog Search: https://console.cloud.google.com/dataplex/search?project={PROJECT_ID}&q=ridership"
)
print(
    f"Business Glossary: https://console.cloud.google.com/dataplex/glossaries/projects/{PROJECT_ID}/locations/{LOCATION}/glossaries/ridership-lakehouse-glossary?project={PROJECT_ID}"
)
pd.DataFrame(
    discovered_rows,
    columns=["query", "entry_type", "fully_qualified_name", "resource", "entry_name"],
).drop_duplicates(subset=["entry_name"])

# %% [markdown]
# # Conclusion
#
# In this notebook, we created our tables, loaded data, and observed how Apache Iceberg manages Parquet files and metadata snapshots on Cloud Storage across **BigQuery Managed Iceberg Tables** and the **Lakehouse Runtime Catalog** (with **Credential Vending**).
#
# We queried `bus_lines` dynamically from BigQuery using 4-part `P.C.N.T` syntax wrapped in a live View, captured OpenLineage metadata from **Serverless for Apache Spark**, and verified automatic asset registration in **Knowledge Catalog**.
#
# In the next notebook, we will use **Serverless for Apache Spark** (with **Lightning Engine**) to simulate bus rides, verify end-to-end **OpenLineage**, and run an automated **Knowledge Catalog Data Quality** scan.
