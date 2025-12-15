import marimo

__generated_with = "0.18.1"
app = marimo.App()


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
def _(mo):
    mo.md(r"""
    # Ridership Open Lakehouse Demo (Part 3): Time-series forecasting of ridership data

    This notebook will demonstrate a strategy to implement an open lakehouse on GCP, using Apache Iceberg, as an open source standard for managing data, while still leveraging GCP native capabilities. This demo will use BigQuery Manged Iceberg Tables, Managed Apache Kafka and Apache Kafka Connect to ingest streaming data, Vertex AI for Generative AI queries on top of the data and Dataplex to govern tables.

    This notebook will use the `bus_rides` data and ML models to generate a time-series forcasting of ridership in the future, in order to alert us when a bus about to become full.

    We will evaluate the models accuracy and generate future data to be used in the next chapters for real-time predictions and alerting.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Environment Setup
    """)
    return


app._unparsable_cell(
    r"""
    USER_AGENT = \"cloud-solutions/data-to-ai-nb-v3\"

    PROJECT_ID = !gcloud config get-value project
    PROJECT_ID = PROJECT_ID[0]

    LOCATION = \"us-central1\"

    BQ_DATASET = \"ridership_lakehouse\"
    BQ_CONNECTION_NAME = \"cloud-resources-connection\"

    GENERAL_BUCKET_NAME = f\"{PROJECT_ID}-ridership-lakehouse\"
    BQ_CATALOG_BUCKET_NAME = f\"{PROJECT_ID}-iceberg-bq-catalog\"
    REST_CATALOG_BUCKET_NAME = f\"{PROJECT_ID}-iceberg-rest-catalog\"

    BQ_CATALOG_PREFIX = \"bq_namespace\"
    REST_CATALOG_PREFIX = \"rest_namespace\"

    print(PROJECT_ID)

    from google.api_core.client_info import ClientInfo
    """,
    name="_"
)


@app.cell
def _(ClientInfo, LOCATION, PROJECT_ID, USER_AGENT):
    from google.cloud import bigquery, storage

    bigquery_client = bigquery.Client(
        project=PROJECT_ID, location=LOCATION, client_info=ClientInfo(user_agent=USER_AGENT)
    )
    storage_client = storage.Client(
        project=PROJECT_ID, client_info=ClientInfo(user_agent=USER_AGENT)
    )
    return bigquery_client, storage_client


@app.cell
def _(BQ_DATASET, PROJECT_ID, bigquery_client, storage_client):
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
    return delete_blobs_with_prefix, select_top_rows


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Feature Engineering

    In our demo, we want to predict rise and spikes to the demand of bus lines. In order to do that, we will calculate a `demand_metric`, which will be high when all passengers cannot fit on a bus, and low when we have remaining capacity on a bus. This metric would allow us to deploy additional buses when it is above some threshold so that we can reduce the number of passengers that cannot board.

    Let's assume that the metric `demand_metric` depends on a number of factors:
      * Station
      * Borough
      * Bus line
      * Date and time (time series)

      We will use the data generated in notebooks 1 & 2 to forecast ridership, based on these four factors.

      Obviously, a couple of these variables are related (the station and borough are related). In a real-world scenario, this could be considered a bad practice, and lead to overfitting towards a specific variable.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Create bus rides table with features
    """)
    return


@app.cell
def _(
    BQ_CATALOG_BUCKET_NAME,
    BQ_CATALOG_PREFIX,
    BQ_CONNECTION_NAME,
    BQ_DATASET,
    LOCATION,
    PROJECT_ID,
    bigquery_client,
    delete_blobs_with_prefix,
):
    FEATURES_TABLE_NAME = "bus_rides_features"
    prefix = f"{BQ_CATALOG_PREFIX}/{FEATURES_TABLE_NAME}"

    ridership_features_uri = f"gs://{BQ_CATALOG_BUCKET_NAME}/{prefix}/"

    bigquery_client.query(
        f"DROP TABLE IF EXISTS {BQ_DATASET}.{FEATURES_TABLE_NAME};"
    ).result()
    delete_blobs_with_prefix(BQ_CATALOG_BUCKET_NAME, prefix)

    query = f"""
    CREATE TABLE `{BQ_DATASET}.{FEATURES_TABLE_NAME}`
    WITH CONNECTION `{PROJECT_ID}.{LOCATION}.{BQ_CONNECTION_NAME}`
    OPTIONS (
      file_format = 'PARQUET',
      table_format = 'ICEBERG',
      storage_uri = '{ridership_features_uri}'
    )
    AS
    (
      SELECT
      r.timestamp_at_stop,
      r.bus_ride_id,
      r.bus_stop_id,
      r.bus_line_id,
      l.bus_line,
      r.bus_size,
      r.total_capacity,
      s.borough,
      r.last_stop,
      r.passengers_in_stop,
      r.passengers_boarding,
      r.passengers_alighting,
      r.remaining_capacity,
      r.remaining_at_stop,
      (r.remaining_at_stop - r.remaining_capacity) AS demand_metric,
      COALESCE(SAFE_DIVIDE(r.remaining_capacity, r.total_capacity), 0) AS remaining_capacity_percentage,
      COALESCE(SAFE_DIVIDE(r.remaining_at_stop, r.passengers_in_stop), 0) AS passengers_left_behind_percentage
    FROM `{BQ_DATASET}.bus_rides` AS r
    LEFT JOIN `{BQ_DATASET}.bus_stations` AS s ON s.bus_stop_id = r.bus_stop_id
    LEFT JOIN `{BQ_DATASET}.bus_lines` AS l ON l.bus_line_id = r.bus_line_id
    );
    """
    bigquery_client.query(query).result()
    return (FEATURES_TABLE_NAME,)


@app.cell
def _(FEATURES_TABLE_NAME, select_top_rows):
    select_top_rows(FEATURES_TABLE_NAME)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Visualize Projected data

    Let's now create a visualization for each of these features, the relationship with the `ridership` column, to see the effects of different features on the target variable.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Demand per bus line over time
    Let's take a look at the last 90 days of ridership data. We'll specifically look at the `remaining_at_stop` as our metric to determined if we need to alert of high demand. The higher the `remaining_at_stop`, the more demand we have, and we might want to dispatch more buses.
    """)
    return


@app.cell
def _(BQ_DATASET, FEATURES_TABLE_NAME, bigquery_client):
    import matplotlib.pyplot as plt
    import random
    DAYS_BACK = 90
    demand_per_bus_line_df = bigquery_client.query(f'\nDECLARE max_ts TIMESTAMP DEFAULT (SELECT MAX(timestamp_at_stop) FROM {BQ_DATASET}.{FEATURES_TABLE_NAME});\nSELECT bus_line, timestamp_at_stop as timestamp_at_stop, AVG(demand_metric) AS demand_metric\n  FROM `{BQ_DATASET}.{FEATURES_TABLE_NAME}`\n  WHERE timestamp_at_stop > TIMESTAMP_SUB(max_ts, INTERVAL {DAYS_BACK} DAY)\n  GROUP BY bus_line, timestamp_at_stop\n  ORDER BY bus_line, timestamp_at_stop;\n').result().to_dataframe()
    random.seed(42)
    bus_line_ids = random.sample(list(demand_per_bus_line_df.bus_line.unique()), k=20)
    figure = plt.figure(figsize=(20, 6))
    plt.xlabel('Timestamp at stop')
    for bus_line_id in bus_line_ids:
        station_data = demand_per_bus_line_df[demand_per_bus_line_df['bus_line'] == bus_line_id].sort_values(by='timestamp_at_stop')
        plt.bar(station_data['timestamp_at_stop'], station_data['demand_metric'], label=bus_line_id)
    plt.xlabel('Timestamp at stop')
    plt.ylabel('Remaining Passengers')
    plt.title('Remaining Passengers Over Time by Bus Line')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    # we'll sample 20 random lines, as displaying all of them is not practical
    # Group data by station ID
    # Customize the plot
    plt.show()  # Plot ridership over time for the current bus line
    return plt, random


@app.cell
def _(BQ_DATASET, FEATURES_TABLE_NAME, bigquery_client, plt, random):
    DAYS_BACK_1 = 90
    demand_per_bus_stop_df = bigquery_client.query(f'\nDECLARE max_ts TIMESTAMP DEFAULT (SELECT MAX(timestamp_at_stop) FROM {BQ_DATASET}.{FEATURES_TABLE_NAME});\nSELECT bus_stop_id, timestamp_at_stop as timestamp_at_stop, AVG(demand_metric) AS demand_metric\n  FROM `{BQ_DATASET}.{FEATURES_TABLE_NAME}`\n  WHERE timestamp_at_stop > TIMESTAMP_SUB(max_ts, INTERVAL {DAYS_BACK_1} DAY)\n  GROUP BY bus_stop_id, timestamp_at_stop\n  ORDER BY bus_stop_id, timestamp_at_stop;\n').result().to_dataframe()
    random.seed(42)
    bus_stop_ids = random.sample(list(demand_per_bus_stop_df.bus_stop_id.unique()), k=20)
    figure_1 = plt.figure(figsize=(20, 6))
    plt.xlabel('Timestamp at stop')
    for bus_stop_id in bus_stop_ids:
        station_data_1 = demand_per_bus_stop_df[demand_per_bus_stop_df['bus_stop_id'] == bus_stop_id].sort_values(by='timestamp_at_stop')
        plt.bar(station_data_1['timestamp_at_stop'], station_data_1['demand_metric'], label=bus_stop_id)
    plt.xlabel('Timestamp at stop')
    plt.ylabel('Demand')
    plt.title('Demand Over Time by Bus Stop')
    plt.legend()
    plt.xticks(rotation=45)
    plt.tight_layout()
    # we'll sample 20 random lines, as displaying all of them is not practical
    # Group data by station ID
    # Customize the plot
    plt.show()  # Plot ridership over time for the current station
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Distribution of demand per Borough (boxplot)
    """)
    return


@app.cell
def _(BQ_DATASET, FEATURES_TABLE_NAME, bigquery_client, plt):
    average_demand_per_borough = (
        bigquery_client.query(
            f"""
    SELECT
      borough,
      APPROX_QUANTILES(demand_metric, 100)[OFFSET(0)] AS whislo,
      APPROX_QUANTILES(demand_metric, 100)[OFFSET(25)] AS q1,
      APPROX_QUANTILES(demand_metric, 100)[OFFSET(50)] AS med,
      AVG(demand_metric) AS mean,
      APPROX_QUANTILES(demand_metric, 100)[OFFSET(75)] AS q3,
      APPROX_QUANTILES(demand_metric, 100)[OFFSET(100)] AS whishi
     FROM `{BQ_DATASET}`.`{FEATURES_TABLE_NAME}`
     GROUP BY borough
    """
        )
        .result()
        .to_dataframe()
    )

    lst_of_dicts = average_demand_per_borough.to_dict(orient="records")
    fig, ax = plt.subplots()

    ax.bxp(
        lst_of_dicts,
        showfliers=False,
        showmeans=True,
        label=[x["borough"] for x in lst_of_dicts],
        meanline=True,
    )
    ax.set_xticklabels([x["borough"] for x in lst_of_dicts])

    plt.ylabel("Demand")
    plt.title("Demand Per Borough")

    plt.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Average demand per month
    """)
    return


@app.cell
def _(BQ_DATASET, FEATURES_TABLE_NAME, bigquery_client, plt):
    # Calculate average demand per calendar month
    average_demand_per_month = (
        bigquery_client.query(
            f"""
    SELECT
      EXTRACT(MONTH FROM timestamp_at_stop) AS transit_month,
      AVG(demand_metric) AS demand_metric
    FROM
      `{BQ_DATASET}`.`{FEATURES_TABLE_NAME}`
    GROUP BY
      transit_month;
    """
        )
        .result()
        .to_dataframe()
    )

    # Sort by month to ensure correct chronological order in the plot
    average_demand_per_month = average_demand_per_month.sort_values(
        by="transit_month"
    ).reset_index()

    plt.figure(figsize=(10, 6))  # Adjust figure size

    plt.bar(
        average_demand_per_month["transit_month"], average_demand_per_month["demand_metric"]
    )

    # Add plot labels and title
    plt.xlabel("Transit Month")
    plt.ylabel("Average Demand")
    plt.title("Average Demand per Month")
    plt.xticks(
        average_demand_per_month["transit_month"]
    )  # Ensure all months are shown on x-axis
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Average Demand per Day-of-Week
    """)
    return


@app.cell
def _(BQ_DATASET, FEATURES_TABLE_NAME, bigquery_client, plt):
    # Group average demand by day of week
    average_demand_per_day_of_week = (
        bigquery_client.query(
            f"""
    SELECT
      EXTRACT(DAYOFWEEK FROM timestamp_at_stop) AS transit_day_of_week,
      AVG(demand_metric) AS demand_metric
    FROM
      `{BQ_DATASET}`.`{FEATURES_TABLE_NAME}`
    GROUP BY
      transit_day_of_week;
    """
        )
        .result()
        .to_dataframe()
    )

    # Sort by month to ensure correct chronological order in the plot
    average_demand_per_day_of_week = average_demand_per_day_of_week.sort_values(
        by="transit_day_of_week"
    ).reset_index()

    days_of_week = {
        1: "Sunday",
        2: "Monday",
        3: "Tuesday",
        4: "Wednesday",
        5: "Thursday",
        6: "Friday",
        7: "Saturday",
    }
    average_demand_per_day_of_week["transit_day_of_week"] = average_demand_per_day_of_week[
        "transit_day_of_week"
    ].map(days_of_week)
    plt.figure(figsize=(10, 6))  # Adjust figure size

    plt.bar(
        average_demand_per_day_of_week["transit_day_of_week"],
        average_demand_per_day_of_week["demand_metric"],
    )

    # Add plot labels and title
    plt.xlabel("Transit Day of Week")
    plt.ylabel("Average Demand")
    plt.title("Average Demand per Day of Week")
    plt.xticks(
        average_demand_per_day_of_week["transit_day_of_week"]
    )  # Ensure all months are shown on x-axis
    plt.grid(True, linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Forecast bus ridership

    For time-series forecasting, there are 3 main options to choose from.

    ### ARIMA_PLUS
    This model is an enhanced version of the traditional ARIMA, offering improved performance and often including automatic parameter selection for better ease of use in forecasting univariate time series. It's a **Univariate** model, meaning, it is built to predict a target value based on only the timestamp variable. You cannot incorporate extra variables that might influence the prediction. For that reason, we will **NOT** use it here in our demo.

    ### ARIMA_PLUS_XREG
    Extending `ARIMA_PLUS`, this model incorporates the ability to utilize external regressors (exogenous variables) to enhance the forecasting accuracy by accounting for the influence of other relevant time series. Like the `ARIMA_PLUS`, it still relies on linearity assumptions between regressors and target. It is a multivariate model, in terms of inputs, but still forecasts a single output.

    ### TimesFM
    `TimesFM` is a deep learning-based forecasting model that leverages transformer architectures to capture complex temporal dependencies and long-range patterns in time series data, often excelling in multi-variate forecasting tasks. It's considered excellent at capturing complex non-linear patterns and long-range dependencies; naturally handles multivariate inputs and outputs; generally performs well on large datasets. It is known to overfit on small datasets.

    ## What's next?

    In the rest of this notebook, we will create predictions using `ARIMA_PLUS_XREG` and `TimesFM` and evaluate both models and compare their performance.

    We will create a summarized bus_rides table, aggregate the `demand_metric` by the hour, and use the linear time gaps filling strategy to train the ARIMA_PLUS_XREG model.
    """)
    return


@app.cell
def _():
    # some constants for later use

    ARIMA_PLUS_XREG_MODEL_NAME = "demand_arima"
    ARIMA_PLUS_XREG_FORECAST_TABLE_NAME = "arima_demand_results"

    MIN_TS = "2022-01-01T00:00:00"
    MAX_TS = "2024-11-30T23:59:59"

    EVAL_MIN_TS = "2024-12-01T00:00:00"
    EVAL_MAX_TS = "2024-12-31T23:59:59"

    DAYS_FORWARD_TO_FORECAST = 7

    SUMMARIZED_FEATURES_TABLE = "summarized_features"
    INTERVALS_MINUTES = 5

    BUS_LINE_FOR_SAMPLING = "P-936"
    DAY_FOR_SAMPLING = "2024-12-05"

    TIMESFM_TABLE_NAME = "timesfm_demand_results"

    ACTUAL_VS_FORECAST_TABLE_NAME = "actual_vs_forecast"

    HORIZON = int(DAYS_FORWARD_TO_FORECAST * 24 * 60 / INTERVALS_MINUTES)
    return (
        ACTUAL_VS_FORECAST_TABLE_NAME,
        ARIMA_PLUS_XREG_FORECAST_TABLE_NAME,
        ARIMA_PLUS_XREG_MODEL_NAME,
        BUS_LINE_FOR_SAMPLING,
        DAY_FOR_SAMPLING,
        EVAL_MAX_TS,
        EVAL_MIN_TS,
        HORIZON,
        INTERVALS_MINUTES,
        MAX_TS,
        MIN_TS,
        SUMMARIZED_FEATURES_TABLE,
        TIMESFM_TABLE_NAME,
    )


@app.cell
def _(
    BQ_DATASET,
    FEATURES_TABLE_NAME,
    INTERVALS_MINUTES,
    SUMMARIZED_FEATURES_TABLE,
    bigquery_client,
):
    query_1 = f"\nCREATE OR REPLACE TABLE `{BQ_DATASET}`.`{SUMMARIZED_FEATURES_TABLE}` AS\n\nWITH agg_rides AS (\n  SELECT\n    TIMESTAMP_BUCKET(timestamp_at_stop, INTERVAL {INTERVALS_MINUTES} MINUTE) AS time,\n    bus_line,\n    bus_stop_id,\n    borough,\n    AVG(demand_metric) AS demand\n   FROM `{BQ_DATASET}.{FEATURES_TABLE_NAME}`\n   GROUP BY time, bus_line, bus_stop_id, borough)\n\nSELECT *\nFROM GAP_FILL(\n  TABLE agg_rides,\n  ts_column => 'time',\n  bucket_width => INTERVAL {INTERVALS_MINUTES} MINUTE,\n  partitioning_columns => ['bus_line', 'bus_stop_id', 'borough'],\n  value_columns => [\n    ('demand', 'linear')\n  ]\n)\nORDER BY time, bus_line, bus_stop_id;\n"
    bigquery_client.query(query_1).result()
    return


@app.cell
def _(SUMMARIZED_FEATURES_TABLE, select_top_rows):
    select_top_rows(SUMMARIZED_FEATURES_TABLE)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Multivariate forecasting using the ARIMA_PLUS_XREG model
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Train the model


    The same CREATE MODEL statement is used to train this model Many options, e.g,  `time_series_data_col`, `time_series_timestamp_col`,  `time_series_id_col` have the same meaning as for the ARIMA_PLUS model.

    The main difference - the ARIMA_PLUS_XREG model uses all columns besides those identified by the options above as the feature columns and uses linear regression to calculate covariate weights.

    For details on the additional options, explanation of the training process, and best practices when training and using the model please refer to BigQuery documentation on [the CREATE MODEL statement for ARIMA_PLUS_XREG models](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-create-multivariate-time-series).

    The next query, will create a model based on data between June 1st, 2022 and 30th of November 2024. The limitation of date ranges is due to our data size, which can't all fit to the training limitations.

    We also chose a training dataset in the past, but leave some data "unseen" to the model, to help us evaluate its accuracy.

    Note, we are treating each bus line, as a separate time series dataset, using the `TIME_SERIES_ID_COL` parameter.
    """)
    return


@app.cell
def _(
    ARIMA_PLUS_XREG_MODEL_NAME,
    BQ_DATASET,
    HORIZON,
    MAX_TS,
    MIN_TS,
    SUMMARIZED_FEATURES_TABLE,
    bigquery_client,
):
    query_2 = f'''\nCREATE OR REPLACE MODEL `{BQ_DATASET}.{ARIMA_PLUS_XREG_MODEL_NAME}`\nOPTIONS(\n  MODEL_TYPE = 'ARIMA_PLUS_XREG',\n  TIME_SERIES_ID_COL = ['bus_line', 'bus_stop_id'],\n  TIME_SERIES_DATA_COL = 'demand',\n  TIME_SERIES_TIMESTAMP_COL = 'time',\n  AUTO_ARIMA=TRUE,\n  HORIZON={HORIZON},\n  DATA_FREQUENCY='AUTO_FREQUENCY',\n  HOLIDAY_REGION = "US"  -- the original dataset is from NY\n)\nAS SELECT\n    time,\n    bus_line,\n    bus_stop_id,\n    borough,\n    demand\n  FROM `{BQ_DATASET}.{SUMMARIZED_FEATURES_TABLE}`\n  WHERE\n    time BETWEEN TIMESTAMP("{MIN_TS}") AND TIMESTAMP("{MAX_TS}");\n'''
    bigquery_client.query(query_2).result()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Evaluating the ARIMA_PLUS_XREG model
    """)
    return


@app.cell
def _(ARIMA_PLUS_XREG_MODEL_NAME, BQ_DATASET, bigquery_client):
    bigquery_client.query(
        f"SELECT * FROM ML.ARIMA_EVALUATE(MODEL `{BQ_DATASET}.{ARIMA_PLUS_XREG_MODEL_NAME}`);"
    ).result().to_dataframe()
    return


@app.cell
def _(
    ARIMA_PLUS_XREG_MODEL_NAME,
    BQ_DATASET,
    EVAL_MAX_TS,
    EVAL_MIN_TS,
    SUMMARIZED_FEATURES_TABLE,
    bigquery_client,
):
    query_3 = f'\nSELECT *\nFROM ML.EVALUATE(MODEL `{BQ_DATASET}.{ARIMA_PLUS_XREG_MODEL_NAME}`,\n  (SELECT * FROM `{BQ_DATASET}.{SUMMARIZED_FEATURES_TABLE}`\n    WHERE time BETWEEN TIMESTAMP("{EVAL_MIN_TS}") AND TIMESTAMP("{EVAL_MAX_TS}")\n  )\n);\n'
    bigquery_client.query(query_3).result().to_dataframe()
    return


@app.cell
def _(
    ARIMA_PLUS_XREG_FORECAST_TABLE_NAME,
    ARIMA_PLUS_XREG_MODEL_NAME,
    BQ_DATASET,
    HORIZON,
    bigquery_client,
    select_top_rows,
):
    # Let's save the forecast results to a table, so we can compare the 2 models later
    query_4 = f'\nCREATE OR REPLACE TABLE `{BQ_DATASET}.{ARIMA_PLUS_XREG_FORECAST_TABLE_NAME}`\nAS\nSELECT * FROM ML.FORECAST (\n    MODEL `{BQ_DATASET}.{ARIMA_PLUS_XREG_MODEL_NAME}`,\n    STRUCT (\n      {HORIZON} AS horizon,\n      0.9 AS confidence_level\n    ),\n    (\n      SELECT\n        timestamp_at_stop AS time,\n        bus_line,\n        bus_stop_id,\n        borough\n      FROM `{BQ_DATASET}.bus_rides_features`\n    )\n  )\n'
    bigquery_client.query(query_4).result()
    # print(query)
    select_top_rows(ARIMA_PLUS_XREG_FORECAST_TABLE_NAME)
    return


@app.cell
def _(
    ARIMA_PLUS_XREG_MODEL_NAME,
    BQ_DATASET,
    BUS_LINE_FOR_SAMPLING,
    DAY_FOR_SAMPLING,
    HORIZON,
    INTERVALS_MINUTES,
    bigquery_client,
):
    # We can even try to manually compare the forecast results with the actual requests
    # let's pick one bus line at random, and compare the forecast results during a given day to the actual results from the same day
    query_5 = f"\nWITH forecast AS (\n  SELECT * FROM ML.FORECAST (\n    MODEL `{BQ_DATASET}.{ARIMA_PLUS_XREG_MODEL_NAME}`,\n    STRUCT (\n      {HORIZON} AS horizon,\n      0.9 AS confidence_level\n    ),\n    (\n      SELECT\n        timestamp_at_stop AS time,\n        bus_line,\n        bus_stop_id,\n        borough\n      FROM `{BQ_DATASET}.bus_rides_features`\n    )\n  )\n), actual AS (\n  SELECT\n    timestamp_at_stop AS time,\n    bus_line,\n    bus_stop_id,\n    borough,\n    demand_metric as observed_value,\n    TIMESTAMP_BUCKET(timestamp_at_stop, INTERVAL {INTERVALS_MINUTES} MINUTE) AS time_bucket\n  FROM {BQ_DATASET}.bus_rides_features\n)\n\nSELECT\n  forecast.bus_line,\n  forecast.bus_stop_id,\n  forecast.forecast_timestamp,\n  actual.time_bucket,\n  actual.time AS actual_time,\n  forecast.forecast_value,\n  actual.observed_value\nFROM forecast\nINNER JOIN actual ON\n  forecast.forecast_timestamp = time_bucket AND\n  actual.bus_line = forecast.bus_line AND\n  actual.bus_stop_id = forecast.bus_stop_id\n\n  WHERE\n  actual.bus_line = '{BUS_LINE_FOR_SAMPLING}' AND\n  actual.time_bucket BETWEEN TIMESTAMP('{DAY_FOR_SAMPLING}T00:00:00') AND\n    TIMESTAMP('{DAY_FOR_SAMPLING}T23:59:59')\n"
    # print(query)
    bigquery_client.query(query_5).result().to_dataframe()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## TimesFM Model

    BigQuery's `TimesFM` is a pretrained, zero-shot foundation model for time-series forecasting. It's a decoder-only transformer model, similar to those used for language, but adapted for time series data. TimesFM was trained on a massive dataset of billions of real-world time points, which allows it to make accurate predictions on new datasets without needing any specific training on that data. This makes it highly versatile and easy for data analysts to use directly in BigQuery with a simple SQL function like `AI.FORECAST`.

    In this section, we will create a table with our forecast, view the results and compare the to the results to the forecasting results from the `ARIMA_PLUS_XREG` model. Although, we don't have a built-in function to evaluate the timesfm model.

    For the `ARIMA_PLUS_XREG` has a built-in `ML.EVALUATE` function, we will simulate the same for our `TimesFM` model, and compare the results.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### Create a results table
    """)
    return


@app.cell
def _(
    BQ_DATASET,
    HORIZON,
    MAX_TS,
    MIN_TS,
    TIMESFM_TABLE_NAME,
    bigquery_client,
    select_top_rows,
):
    # Create a table with the timesfm forecast
    query_6 = f"\nCREATE OR REPLACE TABLE `{BQ_DATASET}.{TIMESFM_TABLE_NAME}`\nAS\nSELECT *\nFROM\n  AI.FORECAST(\n    (\n      SELECT\n        time,\n        bus_line,\n        bus_stop_id,\n        borough,\n        demand\n      FROM `{BQ_DATASET}.summarized_features`\n      WHERE\n        time BETWEEN TIMESTAMP('{MIN_TS}') AND TIMESTAMP('{MAX_TS}')\n    ),\n    horizon => {HORIZON},\n    confidence_level => 0.95,\n    id_cols => ['bus_line', 'bus_stop_id'],\n    timestamp_col => 'time',\n    data_col => 'demand');\n"
    bigquery_client.query(query_6).result()
    select_top_rows(TIMESFM_TABLE_NAME)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ### View results against observed results
    """)
    return


@app.cell
def _(
    BQ_DATASET,
    BUS_LINE_FOR_SAMPLING,
    INTERVALS_MINUTES,
    TIMESFM_TABLE_NAME,
    bigquery_client,
):
    # Now we can review the forecast against the actual observed data
    query_7 = f"\nWITH forecast AS (\n  SELECT\n    bus_line,\n    bus_stop_id,\n    forecast_timestamp,\n    forecast_value,\n    prediction_interval_lower_bound,\n    prediction_interval_upper_bound\n  FROM `{BQ_DATASET}.{TIMESFM_TABLE_NAME}`\n), actual AS (\n  SELECT\n    timestamp_at_stop AS time,\n    bus_line,\n    bus_stop_id,\n    borough,\n    demand_metric as observed_value,\n    TIMESTAMP_BUCKET(timestamp_at_stop, INTERVAL {INTERVALS_MINUTES} MINUTE) AS time_bucket\n  FROM {BQ_DATASET}.bus_rides_features\n)\n\nSELECT\n  forecast.bus_line,\n  forecast.bus_stop_id,\n  forecast.forecast_timestamp,\n  actual.time_bucket,\n  actual.time AS actual_time,\n  forecast.forecast_value,\n  actual.observed_value\nFROM forecast\nLEFT JOIN actual ON\n  forecast.forecast_timestamp = actual.time AND\n  actual.bus_line = forecast.bus_line AND\n  actual.bus_stop_id = forecast.bus_stop_id\n  WHERE actual.bus_line = '{BUS_LINE_FOR_SAMPLING}'\nORDER BY forecast_timestamp\n"
    # print(query)
    bigquery_client.query(query_7).result().to_dataframe()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    ## Comparing results from both models
    """)
    return


@app.cell
def _(
    ACTUAL_VS_FORECAST_TABLE_NAME,
    ARIMA_PLUS_XREG_FORECAST_TABLE_NAME,
    BQ_DATASET,
    INTERVALS_MINUTES,
    TIMESFM_TABLE_NAME,
    bigquery_client,
    select_top_rows,
):
    # Finally, let's compare results of the forecast
    # between the arima model and our timesfm model
    query_8 = f'\nCREATE OR REPLACE TABLE `{BQ_DATASET}.{ACTUAL_VS_FORECAST_TABLE_NAME}` AS\n\nWITH timesfm AS (\n  SELECT\n    bus_line,\n    bus_stop_id,\n    forecast_timestamp,\n    forecast_value as timesfm_forecast_value,\n  FROM `{BQ_DATASET}.{TIMESFM_TABLE_NAME}`\n), arima AS (\n  SELECT\n    bus_line,\n    bus_stop_id,\n    forecast_timestamp,\n    forecast_value as arima_forecast\n    FROM `{BQ_DATASET}.{ARIMA_PLUS_XREG_FORECAST_TABLE_NAME}`\n), actual AS (\n  SELECT\n    timestamp_at_stop AS time,\n    bus_line,\n    bus_stop_id,\n    borough,\n    demand_metric as observed_value,\n    TIMESTAMP_BUCKET(timestamp_at_stop, INTERVAL {INTERVALS_MINUTES} MINUTE) AS time_bucket\n  FROM {BQ_DATASET}.bus_rides_features\n)\nSELECT\n  actual.time AS actual_time,\n  actual.bus_line,\n  actual.bus_stop_id,\n  actual.borough,\n  actual.observed_value,\n  actual.time_bucket,\n  timesfm.timesfm_forecast_value,\n  arima.arima_forecast,\n  ABS(actual.observed_value - timesfm.timesfm_forecast_value) AS timesfm_abs_error,\n  ABS(actual.observed_value - arima.arima_forecast) AS arima_abs_error\nFROM actual\nLEFT JOIN timesfm ON\n  actual.time_bucket = timesfm.forecast_timestamp AND\n  actual.bus_line = timesfm.bus_line AND\n  actual.bus_stop_id = timesfm.bus_stop_id\nLEFT JOIN arima ON\n  actual.time_bucket = arima.forecast_timestamp AND\n  actual.bus_line = arima.bus_line AND\n  actual.bus_stop_id = arima.bus_stop_id\nWHERE\n  arima.arima_forecast IS NOT NULL AND\n  timesfm.timesfm_forecast_value IS NOT NULL\nORDER BY actual.time, actual.bus_line, actual.bus_stop_id\n'
    # We'll start off by joining the 2 forecast tables with the actual data
    # and save the results as a new table.
    bigquery_client.query(query_8).result().to_dataframe()
    actual_vs_forecast_df = select_top_rows(ACTUAL_VS_FORECAST_TABLE_NAME)
    # print(query)
    actual_vs_forecast_df.head()
    return (actual_vs_forecast_df,)


@app.cell
def _(actual_vs_forecast_df, plt):
    # Aggregate data for plotting
    plot_df = (
        actual_vs_forecast_df.groupby("time_bucket")
        .agg(
            {
                "observed_value": "mean",
                "timesfm_forecast_value": "mean",
                "arima_forecast": "mean",
            }
        )
        .reset_index()
    )

    # Create the plot
    plt.figure(figsize=(15, 7))
    plt.plot(
        plot_df["time_bucket"],
        plot_df["observed_value"],
        label="Observed Value",
        marker="o",
    )
    plt.plot(
        plot_df["time_bucket"],
        plot_df["timesfm_forecast_value"],
        label="TimesFM Forecast",
        marker="x",
    )
    plt.plot(
        plot_df["time_bucket"],
        plot_df["arima_forecast"],
        label="ARIMA Forecast",
        marker="s",
    )

    # Customize the plot
    plt.xlabel("Time Bucket")
    plt.ylabel("Value")
    plt.title("Observed vs. Forecasted Values Over Time")
    plt.legend()
    plt.xticks(rotation=45)
    plt.grid(True)
    plt.tight_layout()
    plt.show()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    # Conclusion
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""
    In this notebook, we used our open data lakehouse and connected it to BigQueryML and VertexAI to leverage native Google Cloud's capabilities to enhance our workflow, without losing mobility and freedom of our data.

    We've created tried 2 time-series forecasting models, and compared the results. We've seen the TimesFM model being the clearly better model for our demo. That should not take away from trying out more options. Even here, the ARIMA_PLUS_XREG model can still be tweaked and changed in order to improve its accuracy.

    In the next sections, we will use the TimesFM model to perform near-real-time predictions to anticipate spikes given new data from buses.
    """)
    return


@app.cell
def _():
    import marimo as mo
    return (mo,)


if __name__ == "__main__":
    app.run()
