# /// script
# dependencies = ["fastavro"]
# ///

import marimo

__generated_with = "0.18.1"
app = marimo.App()

with app.setup:
    import marimo as mo
    from helpers import (
        PROJECT_ID, 
        LOCATION, 
        GENERAL_BUCKET_NAME,
        BQ_CATALOG_BUCKET_NAME,
        REST_CATALOG_BUCKET_NAME,
        MAIN_BQ_DATASET, 
        BQ_CONNECTION_NAME,
        BQ_CATALOG_PREFIX,
        REST_CATALOG_PREFIX,
        LakehouseBigQueryClient,
        LakehouseStorageClient,
        sh,
    )

    from google.cloud import storage, bigquery
    from google.api_core.client_info import ClientInfo
    import json

    bigquery_client = LakehouseBigQueryClient()
    storage_client = LakehouseStorageClient()


@app.cell
def _():
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
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Ridership Open Lakehouse Demo (Part 1): Load data to BigQuery Iceberg tables

    This notebook will demonstrate a strategy to implement an open lakehouse on GCP, using Apache Iceberg,
    as an open source standard for managing data, while still leveraging GCP native capabilities. This demo will use
    BigQuery Manged Iceberg Tables, Managed Apache Kafka and Apache Kafka Connect to ingest streaming data, Vertex AI for Generative AI queries on top of the data and Dataplex to govern tables.

    This notebook will load data into BigQuery, backed by Parquet files, in the Apache Iceberg specification.

    If you created the demo, using our terraform scripts, you should be able to access the data files under the `<YOUR_PROJECT_ID>-ridership-lakehouse` bucket, as the terraform script copies over the data files from a publicly available bucket `gs://data-lakehouse-demo-data-assets/staged-data/`.

    If you haven't created this demo using our terraform scripts, the easiest way to get a hold of the data would be to run the following `gsutil` command:

    ```bash
    gsutil -m rsync -r gs://data-lakehouse-demo-data-assets/  gs://<YOUR_BUCKET_NAME>/
    ```

    All data in this notebook was prepared in the previous `part0` notebook.
    """)
    return


@app.cell
def _():
    # create/reference the bq dataset, and clean all tables
    dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{MAIN_BQ_DATASET}")
    dataset_ref.location = LOCATION

    dataset = bigquery_client.create_dataset(dataset_ref, exists_ok=True)
    dataset
    return dataset, dataset_ref


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## Create the tables and load data
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### Different types of Iceberg tables in BigQuery

    BigQuery offers two ways to work with Apache Iceberg tables:

    1. **[BigQuery Tables for Apache Iceberg](https://cloud.google.com/bigquery/docs/iceberg-tables#create-iceberg-tables)**.
    2. **[BigLake metastore with the Iceberg REST catalog](https://cloud.google.com/bigquery/docs/blms-rest-catalog)**

    For most migration and native BigQuery use cases, **BigQuery Tables for Apache Iceberg (managed by BigQuery) is the strongly preferred method.**

    -----

    **1\. BigQuery Tables for Apache Iceberg (Managed by BigQuery)**


    **This is the recommended approach for migrating your data and integrating Iceberg within BigQuery.** These tables offer full BigQuery management of Iceberg, eliminating the need for a separate catalog.

    **SQL Example:**

    ```sql
    CREATE OR REPLACE TABLE `your-project.your_dataset.your_iceberg_table`(
        <column_definition>
    )
    WITH CONNECTION `your-region.your_connection_name`
    OPTIONS (
        file_format = 'PARQUET',
        table_format = 'ICEBERG',
        storage_uri = 'gs://your-bucket/iceberg/your_table_name'
    );
    ```

    **Why should you prefer Managed Tables:**

    BigQuery-managed Iceberg tables unlock powerful features essential for modern data solutions:

      * **Native Integration:** Seamless experience, similar to standard BigQuery tables.
      * **Full DML Support:** Perform `INSERT`, `UPDATE`, `DELETE`, `MERGE` directly with GoogleSQL.
      * **Unified Ingestion:** Supports both batch and high-throughput streaming via the Storage Write API.
      * **Schema Evolution:** BigQuery handles schema changes (add, drop, rename columns, type changes) effortlessly.
      * **Automatic Optimization:** Benefits from BigQuery's built-in optimizations like adaptive file sizing, clustering, and garbage collection.
      * **Robust Security:** Leverage BigQuery's column-level security and data masking.
      * **Simplified Operations:** Reduced overhead by letting BigQuery manage the Iceberg table lifecycle.

    This method provides a more robust, integrated, and efficient way to leverage Iceberg data within the BigQuery ecosystem.


    -----

    **2\. BigLake metastore with the Iceberg REST catalog**


    These tables allow BigQuery to query Iceberg data managed by external systems like Spark or Hive. They are best for hybrid setups where multiple tools need read access and an external system controls the table's lifecycle.

    **SQL Example:**

    ```sql
    CREATE OR REPLACE EXTERNAL TABLE `your-project.your_dataset.your_external_iceberg_table`
      WITH CONNECTION `your-region.your_connection_name`
      OPTIONS (
             format = 'ICEBERG',
             uris = ["gs://mybucket/mydata/mytable/metadata/iceberg.metadata.json"]
       )
    ```

    **Key Points:**

      * **External Control:** Metadata and data managed outside BigQuery.
      * **Read-Only:** BigQuery can only query; DML operations are not supported.
      * **Hybrid Fit:** Ideal for shared access from various tools.
      * **Metadata:** Manual updates for static JSON pointers; BigLake Metastore preferred for dynamic syncing in GCP.


    #### How to Choose?

    Generally, to leverage the most out of you Iceberg data, prefer the managed tables. They provide better integration and automatic optimization.

    If you have BigQuery centric pipelines, with data generated by BigQuery, managed iceberg tables are the obvious choice.

    Choose external tables, if you have spark centric pipelines (or another external engine) that generate and write Iceberg data in GCS, and BigQuery only requires read-only access.

    In a real world scenario, you will probably have some of both, so a truly unified data platform would have a mixture of both tables.

    In this notebook, we will create the 2 different types of tables, to demonstrate that the 2 methods can be combined according to your needs.

    We will generate managed tables for the `bus_stations` and `ridership` datasets, while for the `bus_lines` dataset, we will write iceberg data directly to GCS, using Apache Spark, and mount the data as an external table in BigQuery.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### The `bus_stations` table

    This table will be loaded as a BigQuery Iceberg table (option 2)- managed by BigQuery, read-only access to other processing engines.
    """)
    return


@app.cell
def _():
    bus_stops_prefix = f"{BQ_CATALOG_PREFIX}/bus_stations"
    bus_stops_uri = f"gs://{BQ_CATALOG_BUCKET_NAME}/{bus_stops_prefix}/"

    # Clear the GCS path before
    storage_client.delete_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_stops_prefix)
    storage_client.display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_stops_prefix)
    return bus_stops_prefix, bus_stops_uri


@app.cell
def _(bus_stops_uri):
    # drop the table
    bigquery_client.query(f"DROP TABLE IF EXISTS {MAIN_BQ_DATASET}.bus_stations;").result()

    # create the table
    query = f"""
    CREATE TABLE {MAIN_BQ_DATASET}.bus_stations
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
    return


@app.cell
def _(bus_stops_prefix):
    # We can view the GCS path, and see that there is now an ICEBERG metadata file, but no data
    storage_client.display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_stops_prefix)
    return


@app.cell
def _(bus_stops_prefix):
    # Let's review the metadata file that we have
    _metadata_blob = list(
        storage_client.list_blobs(
            BQ_CATALOG_BUCKET_NAME,
            match_glob=f"{bus_stops_prefix}/metadata/*.metadata.json",
        )
    )[0]
    _metadata_string = _metadata_blob.download_as_string()
    mo.json(json.loads(_metadata_string.decode("utf-8")))
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Not much there. Just the fact that we have a table, and not much more.

    We'll now load data into the table, and see what happens.
    """)
    return


@app.cell
def _(dataset):
    # we will now load the data from the CSV in GCS

    # BQ tables for Apache Iceberg do not support load with truncating, so we will truncate manually, and then load
    _truncate = bigquery_client.query(f"DELETE FROM {MAIN_BQ_DATASET}.bus_stations WHERE TRUE")
    _truncate.result()

    _job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
    )

    _job = bigquery_client.load_table_from_uri(
        f"gs://{GENERAL_BUCKET_NAME}/staged-data/bus_stations.csv",
        dataset.table("bus_stations"),
        job_config=_job_config,
    )

    _job.result()
    return


@app.cell
def _(bus_stops_prefix):
    # We can verify that the data is actually loaded in the iceberg specification and the format used is parquet
    storage_client.display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_stops_prefix)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    We can see in the output that we have some parquet files generated under the `iceberg_data/bus_stations/data/` folder, and one `v0.metadata.json` under the `iceberg_data/bus_stations/metadata/` folder.

    The `iceberg_data/bus_stations/data` folder, contains the `parquet` files with the actual data.

    The `iceberg_data/bus_stations/metadata` folder contains the metadata files - currently only one - but more data will be in there, as our tables evolve (schema changes, rollbacks etc.).

    The `v0.metadata.json` file, which can have other prefixes, contains the important info for this version of the table. Let's take a quick look at this file.
    """)
    return


@app.cell
def _(bus_stops_prefix):
    # Let's review the metadata file that we have
    _metadata_blob = list(
        storage_client.list_blobs(
            BQ_CATALOG_BUCKET_NAME,
            match_glob=f"{bus_stops_prefix}/metadata/*.metadata.json",
        )
    )[0]
    _metadata_string = _metadata_blob.download_as_string()
    mo.json(json.loads(_metadata_string.decode("utf-8")))
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Still not much there, but we'll compare this content to a file generated by our `EXTERNAL` catalog, to see the differences.

    While the data was loaded to the table, and it is available to BigQuery - currently, the fact we loaded the data, **DOES NOT** trigger a metadata refresh

    So, let's verify that the data is available, and then trigger a metadata refresh.

    **REMEMBER** the fact that the metadata json file isn't updated, doesn't mean that the metadata is not kept in BigQuery internal engine. We just need to trigger a metadata refresh to make the metadata available to other engines.
    """)
    return


@app.cell
def _():
    bigquery_client.select_top_rows(MAIN_BQ_DATASET, "bus_stations")
    return


@app.cell
def _():
    # Now let's refresh the metadata
    bigquery_client.query(f"EXPORT TABLE METADATA FROM {MAIN_BQ_DATASET}.bus_stations").result()
    return


@app.cell
def _(bus_stops_prefix):
    # Now let's take a closer look at the metadata folder only
    storage_client.display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, bus_stops_prefix + "/metadata")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    We now basically see, what happens when we create a metadata snapshot - iceberg creates a new snapshot of the metadata.

    We also see a `version-hint.text` file to help us find the most up-to-date version of the metadata.

    Let's take a closer look:
    """)
    return


@app.cell
def _(bus_stops_prefix):
    all_metadata_blobs = list(
        storage_client.list_blobs(
            BQ_CATALOG_BUCKET_NAME, match_glob=f"{bus_stops_prefix}/metadata/*"
        )
    )

    version_hint_blob = list(
        filter(lambda x: x.name.endswith("version-hint.text"), all_metadata_blobs)
    )[0]
    version_hint = version_hint_blob.download_as_string().decode("utf-8")

    with mo.redirect_stdout():
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
        print(f"Latest metadata from our metadata file (v{version_hint}.metadata.json):")

        mo.output.append(mo.json(latest_json))
    return (all_metadata_blobs,)


@app.cell
def _(all_metadata_blobs):
    # Definitely much more metadata!!!!
    # One last thing before we continue, let's take a look at the avro files
    # in the json file, we see one of them being mentioned (the "manifest-list")
    # but not the other. Let's take a look at the manifest-list file

    import fastavro

    manifest_list_file = list(
        filter(lambda x: "manifest-list-000" in x.name, all_metadata_blobs)
    )[0]

    with manifest_list_file.open("rb") as _fo:
        _avro_reader = fastavro.reader(_fo)
        for _i, _record in enumerate(_avro_reader):
            mo.output.append(f"Record {_i+1} ()")
            mo.output.append(mo.json(_record))
    return (fastavro,)


@app.cell
def _(all_metadata_blobs, fastavro):
    # We can see the metadata that was generated when we loaded the data through BigQuery
    # Number of files that we saw (parquet files)
    # rows added etc.
    avro_files = list(filter(lambda x: 'manifest-list-000' not in x.name and x.name.endswith('.avro'), all_metadata_blobs))
    # we also see a reference to the other avro file - so let's take a look there:
    for avro_file in avro_files:
        with avro_file.open('rb') as _fo:
            _avro_reader = fastavro.reader(_fo)
            for _i, _record in enumerate(_avro_reader):
                mo.output.append(f"Record {_i+1} ({avro_file.name})")
                mo.output.append(mo.json(_record))
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    So, now the picture is complete - this is how Apache iceberg can keep track on all the data in our table. We can see each data file listed here, with some metadata.

    we saw the manifest list file hold overall metadata about the snapshot of the data we took

    and saw how it is all connected in the `json` file.

    This is how Iceberg is able to operate, and give every processing engine a way to read the data in a unified way.
    """)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### The `bus_lines` table

    For the `bus_lines` table, we want to simulate a table that is managed by Spark, and BigQuery is just needs to read the table.

    For that we will use the `EXTERNAL` Iceberg tables (method 2), managed by OSS engines, read-only by BigQuery.

    To simulate that, we will start a PySpark process to read the data from the staged bucket, and write it back in Iceberg format to another bucket

    Then mount the data in a BigQuery external table.

    We will also look at the Iceberg metadata generated by spark, which will show us the similarities to the BigQuery metadata we just saw.

    The first step is to create a REST catalog, using the **`biglake`** API.
    """)
    return


@app.cell
def _():
    import requests

    import google.auth
    import google.auth.transport.requests

    # Get default credentials
    credentials, project = google.auth.default()
    print(type(credentials))
    print(dir(credentials))
    print(credentials.service_account_email)
    print(credentials.token)
    print(credentials.token_state)
    # print(help(credentials.refresh))

    request = google.auth.transport.requests.Request()

    credentials.refresh(request)
    # print(credentials.service_account_email)
    # The access token is now available
    _access_token = credentials.token
    _url = f"https://biglake.googleapis.com/iceberg/v1/restcatalog/v1/config?warehouse=gs://{REST_CATALOG_BUCKET_NAME}"
    print(_url)
    res = requests.post(
        _url, 
        headers={
            "x-goog-user-project": PROJECT_ID,
            "Accept": "application/json",
            "Authorization": f"Bearer {_access_token}"
        }
    )
    print(res.status_code)
    print(res.content)
    # catalog_metadata = json.loads(res.content)
    # mo.json(catalog_metadata)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Now that we have a rest catalog, we can start up a spark session, with the required configurations
    """)
    return


@app.cell
def _():
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
    return (spark,)


@app.cell
def _(spark: "DataprocSparkSession"):
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
    return


@app.cell
def _(display_blobs_with_prefix):
    # Now we can see the data written in our EXTERNAL catalog bucket.
    display_blobs_with_prefix(REST_CATALOG_BUCKET_NAME, REST_CATALOG_PREFIX)
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    Like before, we can see a similar structure - with some differences

    - The top folder, `rest-namespace` is just a reference to the namespace we created in Spark
    - Under the namespace `rest-namespace`, we can see a `bus_lines` folder, referencing our table, and then the familiar structure of a `data` folder, and a `metadata` folder.
    - The files under the `data` folder, are still `parquet` files
    - The files under the `metadata` folder looks slightly different, since Spark was the one creating them, and not BQ, but their purpose is the same. we can repeat the process of reading the json file, which will point to the `snap-` avro file, and so on and so on.

    Iceberg is the layer that enables interoperability between processing engines.

    Now, we just need to "mount" the `bus_lines` Iceberg Data in BigQuery:
    """)
    return


@app.cell
def _(BQ_DATASET):
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
    return


@app.cell
def _(select_top_rows):
    # show sample rows
    select_top_rows("bus_lines")
    return


@app.cell
def _(spark: "DataprocSparkSession"):
    # now that we confirmed that the data is available, we're done with Spark, so we can stop the session
    spark.stop()
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ### The `ridership` table

    Lastly, the `ridership` table will be loaded just like the `bus_stations` table, but this time we will [cluster](https://cloud.google.com/bigquery/docs/clustered-tables) the table by the timestamp.
    """)
    return


@app.cell
def _(BQ_DATASET, delete_blobs_with_prefix):
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
    return (ridership_prefix,)


@app.cell
def _(BQ_DATASET, dataset_ref):
    # Load data into the table
    table_ref = dataset_ref.table('ridership')
    truncate_1 = bigquery_client.query(f'DELETE FROM {BQ_DATASET}.ridership WHERE TRUE')
    # BQ tables for Apache Iceberg do not support load with truncating, so we will truncate manually, and then load
    truncate_1.result()
    job_config_1 = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, source_format=bigquery.SourceFormat.PARQUET)
    job_1 = bigquery_client.load_table_from_uri(f'gs://{GENERAL_BUCKET_NAME}/staged-data/ridership/*.parquet', table_ref, job_config=job_config_1)
    job_1.result()
    # Export the metadata
    bigquery_client.query(f'EXPORT TABLE METADATA FROM {BQ_DATASET}.ridership').result()
    return


@app.cell
def _(select_top_rows):
    # show sample rows
    select_top_rows("ridership")
    return


@app.cell
def _(display_blobs_with_prefix, ridership_prefix):
    display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, ridership_prefix + "data/")
    return


@app.cell
def _(display_blobs_with_prefix, ridership_prefix):
    display_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, ridership_prefix + "metadata")
    return


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # Conclusion

    In this notebook, we created our tables, loaded data - and observed what happens on GCS when managing Iceberg tables either by BigQuery or by other engines like Spark.

    We saw the metadata and examined how Iceberg keeps pointer to the actual data and manages schemas and metadata.

    In the next notebook, we will use Apache Spark to do some data processing, and focus our attention on how to read data from the different catalogs, and the pros and cons for each method of reading the data.
    """)
    return


if __name__ == "__main__":
    app.run()
