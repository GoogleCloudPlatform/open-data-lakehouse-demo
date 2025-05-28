import marimo

__generated_with = "0.13.13"
app = marimo.App()


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        # Ridership Open Lakehouse Demo

        This notebook will demonstrate a strategy to implement an open lakehouse on GCP, using Apache Iceberg, 
        as an open source standard for managing data, while still leveraging GCP native capabilities. This demo will use 
        BigQuery Manged Iceberg Tables, Managed Apache Kafka and Apache Kafka Connect to ingest streaming data, Vertex AI for Generative AI queries on top of the data and Dataplex to govern tables.

        This notebook will generate fake data and anonimized real-world data.

        the real-world data used in this notebook is from [MTA daily ridership data](https://data.ny.gov/Transportation/MTA-Daily-Ridership-Data-2020-2025/vxuj-8kew/data_preview).


        Rest of the data is being randomly generated inside the notebook.
        """
    )
    return


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
    !pip install faker pandas sodapy --upgrade --quiet
    """,
    name="_"
)


@app.cell(
    import os
    LOCATION = os.environ.get("LOCATION", "us-central1") 
    PROJECT_ID = os.environ.get("PROJECT_ID", "us-central1")
    
    return LOCATION, PROJECT_ID
)


@app.cell
def _(LOCATION, PROJECT_ID, USER_AGENT):
    from google.cloud import bigquery, storage
    # @title Create clients for gcs and bq
    from google.api_core.client_info import ClientInfo

    bigquery_client = bigquery.Client(
        project=PROJECT_ID,
        location=LOCATION,
        client_info=ClientInfo(user_agent=USER_AGENT)
    )
    storage_client = storage.Client(
        project=PROJECT_ID,
        client_info=ClientInfo(user_agent=USER_AGENT)
    )
    return bigquery, bigquery_client, storage_client


@app.cell
def _(BUCKET_NAME, LOCATION, storage_client):
    # @title Create GCS bucket, or reference the existing one
    from google.cloud import exceptions

    try:
        bucket = storage_client.create_bucket(BUCKET_NAME, location=LOCATION)
        print(f"Bucket {BUCKET_NAME} created")
    except exceptions.Conflict:
        # Bucket already exists - return the existing bucket
        bucket = storage_client.bucket(BUCKET_NAME)
        print(f"Bucket {BUCKET_NAME} already exists")
    except Exception as e:
        print(f"Error creating bucket {BUCKET_NAME}: {e}")
    return (exceptions,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## MTA Data

        This part is tricky - this is the raw data from the MTA subways of new york.
        The MTA website and API are slow... very slow. Checking the [hourly ridership data](https://data.ny.gov/Transportation/MTA-Subway-Hourly-Ridership-2020-2024/wujg-7c2s/about_data), we have about 110 million records to get.

        Downloading the CSV manually is the more efficiant option, but still very slow, so you would have to send the request, and keep your machine awake and browser for a few hours (yes, hours, was about 2 hours in my case) before the CSV starts downloading.

        for programatic download using the API, the situation might be worse. the API is prone to timeouts. The default records limit per request is 1,000, and the maximum is 50,000, which means we have to do chunking of API calls, but the latency is increasing expo. when increasing the limit.

        I've written the function to donwload the data and write each request to be appended to a file, but this ran for 4 hours, and got around 8% of the data, before I gave up.

        The next cell has the function to download the data using the API, but the call to the function is commented out, since it is very slow to run.

        the cell after that allows you to fill in the path to GCS, so, whichever method you want to get the data, just make sure, that the variable `MTA_RAW_CSV` points to a valid and accisble path on GCS that holds the MTA hourly ridership data.

        happy thoughts!
        """
    )
    return


@app.cell
def _(os):
    from csv import DictWriter
    from sodapy import Socrata
    FORCE_CLEAR_DATA = False
    FILENAME = 'raw-mta-data.csv'
    fieldnames = ['transit_timestamp', 'transit_mode', 'station_complex_id', 'station_complex', 'borough', 'payment_method', 'fare_class_category', 'ridership', 'transfers', 'latitude', 'longitude', 'georeference', ':@computed_region_kjdx_g34t', ':@computed_region_yamh_8v7k', ':@computed_region_wbg7_3whc']

    def programmatically_download_mta_data():
        import requests
        client = Socrata('data.ny.gov', None)
        TOTAL_NUMBER_OF_RECORDS = 110696370
        STEP = 50000
        existing_file = os.path.exists(FILENAME)
        if not existing_file or FORCE_CLEAR_DATA:
            rows_got = 0
            with open(FILENAME, 'w') as fp:
                writer = DictWriter(fp, fieldnames=fieldnames)
                writer.writeheader()
        else:
            with open(FILENAME, 'r') as f:
                rows_got = sum((1 for line in f)) - 1
            print(f'Starting from existing data. already got {rows_got:,} records ({round(rows_got / TOTAL_NUMBER_OF_RECORDS * 100, 2)}%)')
        while rows_got < TOTAL_NUMBER_OF_RECORDS:
            try:
                results = client.get('wujg-7c2s', limit=STEP, offset=rows_got)
            except requests.exceptions.ReadTimeout as te:
                STEP = STEP - 1000
                print(f'Got timeout, adjusting limit to {STEP}')
            else:
                with open(FILENAME, 'a') as fp:
                    writer = DictWriter(fp, fieldnames=fieldnames)
                    writer.writerows(results)
                rows_got = rows_got + len(results)
                print(f'Got {rows_got:,} rows so far ({round(rows_got / TOTAL_NUMBER_OF_RECORDS * 100, 2)}%)')
    return (DictWriter,)


app._unparsable_cell(
    r"""
    MTA_RAW_CSV_PATH_IN_GCS = \"mta-manual-downloaded-data/MTA_Subway_Hourly_Ridership.csv\"  # @param {type: 'string'}
    MTA_RAW_CSV = f\"gs://{BUCKET_NAME}/{MTA_RAW_CSV_PATH_IN_GCS}\"

    blob_exists_output = !gsutil ls {MTA_RAW_CSV}

    if blob_exists_output[0].startswith(\"CommandException\"):
      raise ValueError(f\"Path '{MTA_RAW_CSV}' doesn't appear to point to a valid GCS object\")
    else:
      print(f\"Path '{MTA_RAW_CSV}' found\")
    """,
    name="_"
)


@app.cell
def _(
    BQ_DATASET,
    LOCATION,
    MTA_RAW_CSV,
    PROJECT_ID,
    bigquery,
    bigquery_client,
    exceptions,
):
    # @title Load raw data to a BQ, to continue transformation of the data
    # Due to it's size, it will be easier to load data to bq and transform it there

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
        SchemaField("longtitude", "FLOAT"),
        SchemaField("Georeference", "GEOGRAPHY"),
        ]

    BQ_TABLE = "raw_mta_data"

    dataset = bigquery.Dataset(f'{PROJECT_ID}.{BQ_DATASET}')
    dataset.location = LOCATION

    # create or get a reference to existing dataset
    try:
      bigquery_client.get_dataset(BQ_DATASET)
      print("dataset exists")
    except exceptions.NotFound:
      bigquery_client.create_dataset(dataset, timeout=30)
      print('dataset created {}'.format(BQ_DATASET))

    try:
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

        print('created {}.{}'.format(BQ_DATASET, BQ_TABLE))

    except Exception as e:
        print('mta load failed {}'.format(e))
    return (dataset,)


@app.cell
def _(BQ_DATASET, bigquery_client):
    bigquery_client.query(f"DROP TABLE IF EXISTS {BQ_DATASET}.mta_data_stations;").result()

    # the raw data has a non-normalized dataset, where each station appear in full detail, for each hourly data point
    # we will create a table just for the stations, and we will reference the `station_id` from the thinner time-series table.
    # note, we are saving the results to a pandas dataframe, to be used later

    query = f"""
    CREATE TABLE {BQ_DATASET}.mta_data_stations AS
    SELECT
      CAST(REPLACE(station_complex_id, 'TRAM', '98765') AS INT64) AS station_id,
      station_complex,
      borough,
      latitude,
      longtitude,
    FROM
      (
        SELECT
          *,
          ROW_NUMBER() OVER (PARTITION BY station_complex_id ORDER BY transit_timestamp ASC) as rn
        FROM
          `{BQ_DATASET}.raw_mta_data`
      )
    WHERE
      rn = 1;
    """
    bigquery_client.query(query).result()

    stations_df = bigquery_client.query(f"SELECT * FROM {BQ_DATASET}.mta_data_stations;").to_dataframe()
    stations_df
    return (stations_df,)


@app.cell
def _(BQ_DATASET, bigquery_client):
    bigquery_client.query(f'DROP TABLE IF EXISTS {BQ_DATASET}.mta_data_parsed;').result()
    query_1 = f"\nCREATE TABLE `{BQ_DATASET}.mta_data_parsed` AS\nSELECT\n  PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', transit_timestamp) AS `transit_timestamp`,\n  CAST(REPLACE(station_complex_id, 'TRAM', '98765') AS INT64) AS `station_id`,\n  SUM(ridership) as ridership\nFROM `{BQ_DATASET}.raw_mta_data`\nGROUP BY PARSE_TIMESTAMP('%m/%d/%Y %I:%M:%S %p', transit_timestamp), CAST(REPLACE(station_complex_id, 'TRAM', '98765') AS INT64);\n"
    bigquery_client.query(query_1).result()
    bigquery_client.query(f'SELECT * FROM {BQ_DATASET}.mta_data_parsed LIMIT 20;').to_dataframe()
    return


@app.cell
def _(BQ_DATASET, bigquery_client):
    bigquery_client.query(f'DROP TABLE IF EXISTS {BQ_DATASET}.ridership;').result()
    query_2 = f'\nCREATE TABLE `{BQ_DATASET}.ridership` AS\nSELECT\n  TIMESTAMP_ADD(t.transit_timestamp, INTERVAL minute_offset MINUTE) AS transit_timestamp,\n  t.station_id,\n  ROUND(CAST(\n    (FLOOR(t.ridership / 60)) +\n    CASE\n      WHEN minute_offset < MOD(t.ridership, 60) THEN 1\n      ELSE 0\n    END AS INTEGER\n  )) AS ridership\nFROM\n  {BQ_DATASET}.mta_data_parsed AS t,\n  UNNEST(GENERATE_ARRAY(0, 59)) AS minute_offset\nORDER BY\n  station_id,\n  transit_timestamp;\n'
    bigquery_client.query(query_2).result()
    bigquery_client.query(f'SELECT * FROM {BQ_DATASET}.ridership LIMIT 20;').to_dataframe()
    return


@app.cell
def _(BQ_DATASET, bigquery_client):
    query_3 = f'\nWITH minutly_agg AS (\n  SELECT TIMESTAMP_TRUNC(transit_timestamp, hour) AS transit_timestamp,\n  station_id,\n  SUM(ridership) AS ridership_agg\n  FROM `{BQ_DATASET}.ridership`\n  GROUP BY TIMESTAMP_TRUNC(transit_timestamp, hour),\n  station_id\n)\nSELECT\n  minutly_agg.transit_timestamp, minutly_agg.station_id, minutly_agg.ridership_agg, parsed.ridership\nFROM minutly_agg JOIN `{BQ_DATASET}.mta_data_parsed` as parsed ON\n  minutly_agg.transit_timestamp = parsed.transit_timestamp AND\n  minutly_agg.station_id = parsed.station_id\nWHERE minutly_agg.ridership_agg != parsed.ridership\n'
    bigquery_client.query(query_3).to_dataframe()
    return


@app.cell
def _(stations_df):
    # @title Generate fake data for bus lines and bus stops (Routes)

    from faker import Faker
    Faker.seed(42)
    fake = Faker()

    import random
    def generate_bus_line(i: int) -> dict:
      number_of_stops = fake.random_int(min=40, max=60)
      return {
          "bus_line_id": i+1,
          "bus_line": fake.unique.bothify("?-###").upper(),
          "number_of_stops": number_of_stops,
          "stops": [random.choice(stations_ids) for _ in range(number_of_stops)]
      }

    def fakify_station(station: dict) -> dict:
      return {
          "bus_stop_id": station["station_id"],
          "address": fake.unique.address().split(",")[0].replace("\n", ", "),
          "school_zone": fake.boolean(),
          "seating": fake.boolean(),
          "latitude": station["latitude"],
          "longtitude": station["longtitude"],
      }

    stations_lst = stations_df.to_dict('records')
    fake_stations_lst = [fakify_station(station) for station in stations_lst]
    stations_ids = [station["bus_stop_id"] for station in fake_stations_lst]
    BUS_LINES_NUM = 25
    bus_lines = [generate_bus_line(i) for i in range(BUS_LINES_NUM)]

    random_bus_line = random.choice(bus_lines)
    print(f"Genrated {len(bus_lines)} random bus lines. One for example: {random_bus_line}")
    print(f"Anonimized {len(fake_stations_lst)} bus_stations. The first stations for the random bus line above is: {next(filter(lambda x: x['bus_stop_id'] == random_bus_line['stops'][0], fake_stations_lst))}")
    return bus_lines, fake_stations_lst


@app.cell(hide_code=True)
def _(mo):
    mo.md(
        r"""
        ## Extract data to GCS
        Now, that we have all data we need, the data needs to reside in GCS. We will extract the `ridership` BigQuery table to GCS as a CSV, we will store the local datasets (`bus_lines` & `fake_stations_lst`) we will save to local files (JSONL and CSV, respectively) and upload to GCS.
        """
    )
    return


@app.cell
def _(BUCKET_NAME, LOCATION, bigquery_client, dataset):
    destination_uri = 'gs://{}/{}'.format(BUCKET_NAME, 'mta_staging_data/ridership/ridership_part_*.csv')
    table_ref_1 = dataset.table('ridership')
    extract_job = bigquery_client.extract_table(table_ref_1, destination_uri, location=LOCATION)
    extract_job.result()
    return


@app.cell
def _(DictWriter, bus_lines, fake_stations_lst):
    with open("bus_stations.csv", "w") as fp:
      writer = DictWriter(fp, fieldnames=fake_stations_lst[0].keys())
      writer.writeheader()
      writer.writerows(fake_stations_lst)

    import json
    with open("bus_lines.json", "w") as fp:
      for line in bus_lines:
        json.dump(line, fp)
        fp.write("\n")
    return


app._unparsable_cell(
    r"""
    !gsutil cp bus_lines.json gs://{BUCKET_NAME}/mta_staging_data/bus_lines.json
    !gsutil cp bus_stations.csv gs://{BUCKET_NAME}/mta_staging_data/bus_stations.csv
    """,
    name="_"
)


@app.cell
def _():
    # # @title Drop tables and dataset, just to keep things clean
    # try:
    #   for table in bigquery_client.list_tables(dataset):
    #     bigquery_client.delete_table(table)
    #   bigquery_client.delete_dataset(dataset)
    # except exceptions.NotFound:
    #   print("Dataset looks already dropped")
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
