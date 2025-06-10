import marimo

__generated_with = "0.13.15"
app = marimo.App(width="medium")


@app.cell
def _(mo):
    mo.md(
        r"""
    # Ridership Open Lakehouse Demo

    This notebook will demonstrate a strategy to implement an open lakehouse on GCP, using Apache Iceberg,
    as an open source standard for managing data, while still leveraging GCP native capabilities.

    This demo will use BigQuery Manged Iceberg Tables, Managed Apache Kafka and Apache Kafka Connect to ingest
    streaming data, Vertex AI for Generative AI queries on top of the data and Dataplex to govern tables.

    This notebook will load the data generated in the previous notebook to BQ, and setup streaming resources
    """
    )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Setup the environment""")
    return


@app.cell
def _():
    import os
    LOCATION = os.environ.get("LOCATION", "us-central1") 
    USER_AGENT = "cloud-solutions/data-to-ai-nb-v3"
    BQ_DATASET = "ridership_lakehouse"

    PROJECT_ID = os.environ.get("PROJECT_ID")
    if not PROJECT_ID:
        import subprocess
        PROJECT_ID = subprocess.run(["gcloud", "config", "get-value", "project"], capture_output=True)
        PROJECT_ID = PROJECT_ID.stdout.decode("utf-8").strip()
    assert PROJECT_ID, "Please set the PROJECT_ID environment variable"
    BUCKET_NAME = f"{PROJECT_ID}-ridership-lakehouse"
    return BQ_DATASET, BUCKET_NAME, LOCATION, PROJECT_ID, USER_AGENT


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Create Clients""")
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
    return bigquery, bigquery_client


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Create Tables in BigQuery""")
    return


@app.cell
def _(BQ_DATASET, BUCKET_NAME, LOCATION, PROJECT_ID, bigquery_client):
    bus_stops_uri = f"gs://{BUCKET_NAME}/iceberg_data/bus_stations/"
    bus_lines_uri = f"gs://{BUCKET_NAME}/iceberg_data/bus_lines/"
    ridership_uri = f"gs://{BUCKET_NAME}/iceberg_data/ridership/"

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
    bigquery_client.query(
        f'DROP TABLE IF EXISTS {BQ_DATASET}.bus_lines;'
    ).result()
    _create_table_stmt = f"""
        CREATE TABLE {BQ_DATASET}.bus_lines (
            bus_line_id INTEGER,
            bus_line STRING,
            number_of_stops INTEGER,
            stops ARRAY<INTEGER>
        )
        WITH CONNECTION `{PROJECT_ID}.{LOCATION}.{BQ_DATASET}`
        OPTIONS (
            file_format = 'PARQUET',
            table_format = 'ICEBERG',
            storage_uri = '{bus_lines_uri}'
        );
    """
    bigquery_client.query(_create_table_stmt).result()
    return


@app.cell
def _(BQ_DATASET, LOCATION, PROJECT_ID, bigquery_client, ridership_uri):
    bigquery_client.query(
        f'DROP TABLE IF EXISTS {BQ_DATASET}.ridership;'
    ).result()
    _create_table_stmt = f"""
        CREATE TABLE {BQ_DATASET}.ridership (
            transit_timestamp TIMESTAMP,
            station_id INTEGER,
            ridership INTEGER
        )
        WITH CONNECTION `{PROJECT_ID}.{LOCATION}.{BQ_DATASET}`
        OPTIONS (
            file_format = 'PARQUET',
            table_format = 'ICEBERG',
            storage_uri = '{ridership_uri}'
        );
    """
    bigquery_client.query(_create_table_stmt).result()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Load data to Lakehouse tables""")
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
