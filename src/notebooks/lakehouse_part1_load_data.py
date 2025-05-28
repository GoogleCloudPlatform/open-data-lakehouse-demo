import marimo

__generated_with = "0.13.13"
app = marimo.App()


app._unparsable_cell(
    r"""
    # Ridership Open Lakehouse Demo

    This notebook will demonstrate a strategy to implement an open lakehouse on GCP, using Apache Iceberg,
    as an open source standard for managing data, while still leveraging GCP native capabilities.
    
    This demo will use BigQuery Manged Iceberg Tables, Managed Apache Kafka and Apache Kafka Connect to ingest
    streaming data, Vertex AI for Generative AI queries on top of the data and Dataplex to govern tables.

    This notebook will load the data generated in the previous notebook to BQ, and setup streaming resources
    """,
    name="_"
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Setup the environment
        """
    )
    return

app._unparsable_cell(
    r"""
    PROJECT_ID = \"your project ID here\" # @param {type:\"string\"}
    LOCATION = \"us-central1\" # @param {type:\"string\"}

    # in-case someone didn't update the project manually, assume current project is the right one
    if PROJECT_ID == \"your project ID here\":
        PROJECT_ID = !gcloud config get-value project
        PROJECT_ID = PROJECT_ID[0]

    BUCKET = f\"{PROJECT_ID}-ridership-lakehouse\" # bucket will be created in a subsequant step
    SOURCE_DATA_BUCKET = f\"{PROJECT_ID}-ridership-lakehouse\"
    USER_AGENT = \"cloud-solutions/data-to-ai-nb-v3\"
    BQ_DATASET = \"ridership_lakehouse\"
    PROJECT_ID
    """,
    name="_"
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Create Clients
        """
    )
    return


@app.cell
def _(LOCATION, PROJECT_ID, USER_AGENT):
    from google.cloud import storage, bigquery
    from google.api_core import exceptions
    from google.api_core.client_info import ClientInfo
    from google.cloud.exceptions import NotFound

    bigquery_client = bigquery.Client(
        project=PROJECT_ID,
        location=LOCATION,
        client_info=ClientInfo(user_agent=USER_AGENT)
    )
    storage_client = storage.Client(
        project=PROJECT_ID,
        client_info=ClientInfo(user_agent=USER_AGENT)
    )
    return NotFound, bigquery, bigquery_client, exceptions, storage_client


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## TODO: Move the next 5 cells to Terraform
        """
    )
    return


@app.cell
def _(BUCKET, LOCATION, exceptions, storage_client):
    try:
        bucket = storage_client.create_bucket(BUCKET, location=LOCATION)
        print(f"Bucket {BUCKET} created")
    except exceptions.Conflict:
        # Bucket already exists - return the existing bucket
        bucket = storage_client.bucket(BUCKET)
        print(f"Bucket {BUCKET} already exists")
    except Exception as e:
        print(f"Error creating bucket {BUCKET}: {e}")
    return


@app.cell
def _(
    BQ_DATASET,
    LOCATION,
    NotFound,
    PROJECT_ID,
    bigquery,
    bigquery_client,
    e,
):
    try:
      dataset = bigquery.Dataset(f'{PROJECT_ID}.{BQ_DATASET}')
      dataset.location = LOCATION
      bigquery_client.get_dataset(BQ_DATASET)
      print("dataset exists")
    except NotFound:
      bigquery_client.create_dataset(dataset, timeout=30)
      print('dataset created {}'.format(e))

    dataset_ref = bigquery_client.dataset(BQ_DATASET)
    return (dataset_ref,)


app._unparsable_cell(
    r"""
    !bq mk \
    --connection \
    --location={LOCATION} \
    --project_id={PROJECT_ID} \
    --connection_type=CLOUD_RESOURCE \
     {BQ_DATASET}
    """,
    name="_"
)


app._unparsable_cell(
    r"""
    import json
    connection_details_json_str = !bq show --format json --connection {PROJECT_ID}.{LOCATION}.{BQ_DATASET}
    connection_details_dict = json.loads(connection_details_json_str[0])
    CONNECTION_SA_ID = connection_details_dict[\"cloudResource\"][\"serviceAccountId\"]
    if not CONNECTION_SA_ID:
        # it's possible that this command failed, when ran immediately after the previous command
        # this is due to the time it takes the API to be consistent due to async actions on GCP
        # we will wait 10 seconds, and try again
        # if this still fails, we'll throw an exception
        import time
        time.sleep(10)
        connection_details_json_str = !bq show --format json --connection {PROJECT_ID}.{LOCATION}.{BQ_DATASET}
        connection_details_dict = json.loads(connection_details_json_str[0])
        CONNECTION_SA_ID = connection_details_dict[\"cloudResource\"][\"serviceAccountId\"]
    if not CONNECTION_SA_ID:
        raise ValueError(\"No Service Account detected for BQ Connection\")
    """,
    name="_"
)


app._unparsable_cell(
    r"""
    !gcloud storage buckets add-iam-policy-binding 'gs://{BUCKET_NAME}' \
        --member='serviceAccount:{CONNECTION_SA_ID}' \
        --role=roles/storage.objectUser \
        --quiet

    !gcloud storage buckets add-iam-policy-binding 'gs://{BUCKET_NAME}' \
        --member='serviceAccount:{CONNECTION_SA_ID}' \
        --role=roles/storage.legacyBucketReader \
        --quiet
    """,
    name="_"
)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Create Tables in BigQuery
        """
    )
    return


@app.cell
def _(BQ_DATASET, BUCKET, LOCATION, PROJECT_ID, bigquery_client):
    bus_stops_uri = f"gs://{BUCKET}/iceberg_data/bus_stations/"
    bus_lines_uri = f"gs://{BUCKET}/iceberg_data/bus_lines/"
    ridership_uri = f"gs://{BUCKET}/iceberg_data/ridership/"

    bigquery_client.query(f"DROP TABLE IF EXISTS {BQ_DATASET}.bus_stations;").result()
    query = f"""
    CREATE TABLE {BQ_DATASET}.bus_stations
    (
      bus_stop_id INTEGER,
      address STRING,
      school_zone BOOLEAN,
      seating BOOLEAN,
      latitude FLOAT64,
      longtitude FLOAT64
    )
    WITH CONNECTION `{PROJECT_ID}.{LOCATION}.{BQ_DATASET}`
    OPTIONS (
      file_format = 'PARQUET',
      table_format = 'ICEBERG',
      storage_uri = '{bus_stops_uri}');
    """
    bigquery_client.query(query).result()
    return bus_lines_uri, ridership_uri


@app.cell
def _(BQ_DATASET, LOCATION, PROJECT_ID, bigquery_client, bus_lines_uri):
    bigquery_client.query(f'DROP TABLE IF EXISTS {BQ_DATASET}.bus_lines;').result()
    query_1 = f"\nCREATE TABLE {BQ_DATASET}.bus_lines\n(\n  bus_line_id INTEGER,\n  bus_line STRING,\n  number_of_stops INTEGER,\n  stops ARRAY<INTEGER>\n)\nWITH CONNECTION `{PROJECT_ID}.{LOCATION}.{BQ_DATASET}`\nOPTIONS (\n  file_format = 'PARQUET',\n  table_format = 'ICEBERG',\n  storage_uri = '{bus_lines_uri}');\n"
    bigquery_client.query(query_1).result()
    return


@app.cell
def _(BQ_DATASET, LOCATION, PROJECT_ID, bigquery_client, ridership_uri):
    bigquery_client.query(f'DROP TABLE IF EXISTS {BQ_DATASET}.ridership;').result()
    query_2 = f"\nCREATE TABLE {BQ_DATASET}.ridership\n(\n  transit_timestamp TIMESTAMP,\n  station_id INTEGER,\n  ridership INTEGER\n)\nWITH CONNECTION `{PROJECT_ID}.{LOCATION}.{BQ_DATASET}`\nOPTIONS (\n  file_format = 'PARQUET',\n  table_format = 'ICEBERG',\n  storage_uri = '{ridership_uri}');\n"
    bigquery_client.query(query_2).result()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Load data to Lakehouse tables

        """
    )
    return


@app.cell
def _(BQ_DATASET, SOURCE_DATA_BUCKET, bigquery, bigquery_client, dataset_ref):
    table_ref = dataset_ref.table("bus_lines")

    # BQ tables for Apache Iceberg do not support load with truncating, so we will truncate manually, and then load
    truncate = bigquery_client.query(f"DELETE FROM {BQ_DATASET}.bus_lines WHERE TRUE")
    truncate.result()

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
    )

    job = bigquery_client.load_table_from_uri(
        f"gs://{SOURCE_DATA_BUCKET}/mta_staging_data/bus_lines.json",
        table_ref,
        job_config=job_config,
    )

    job.result()
    return


@app.cell
def _(BQ_DATASET, SOURCE_DATA_BUCKET, bigquery, bigquery_client, dataset_ref):
    table_ref_1 = dataset_ref.table('bus_stations')
    truncate_1 = bigquery_client.query(f'DELETE FROM {BQ_DATASET}.bus_stations WHERE TRUE')
    truncate_1.result()
    job_config_1 = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1)
    job_1 = bigquery_client.load_table_from_uri(f'gs://{SOURCE_DATA_BUCKET}/mta_staging_data/bus_stations.csv', table_ref_1, job_config=job_config_1)
    job_1.result()
    return


@app.cell
def _(BQ_DATASET, SOURCE_DATA_BUCKET, bigquery, bigquery_client, dataset_ref):
    table_ref_2 = dataset_ref.table('ridership')
    truncate_2 = bigquery_client.query(f'DELETE FROM {BQ_DATASET}.ridership WHERE TRUE')
    truncate_2.result()
    job_config_2 = bigquery.LoadJobConfig(write_disposition=bigquery.WriteDisposition.WRITE_APPEND, source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1)
    job_2 = bigquery_client.load_table_from_uri(f'gs://{SOURCE_DATA_BUCKET}/mta_staging_data/ridership/*.csv', table_ref_2, job_config=job_config_2)
    job_2.result()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Basic Analytics
        After loading the data to our open data lakehouse, we will demonstrate some basic analytics, but we will repeat the process with several different engines
        - BigQuery
        - Spark (serverless?)
        - Dataflow
        """
    )
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
