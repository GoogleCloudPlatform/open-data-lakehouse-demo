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
# # Ridership Open Lakehouse Demo (Part 3): Time-series forecasting of ridership data
#
# This notebook will demonstrate a strategy to implement an open lakehouse on GCP, using Apache Iceberg, as an open source standard for managing data, while still leveraging GCP native capabilities. This demo will use BigQuery Managed Iceberg Tables, the Lakehouse Runtime Catalog (Apache Iceberg REST Catalog with Credential Vending), Serverless for Apache Spark, Managed Service for Apache Kafka and Apache Kafka Connect to ingest streaming data, BigQuery AI (`AI.FORECAST` powered by TimesFM) and BigQuery ML (`ARIMA_PLUS_XREG`) for forecasting, and Knowledge Catalog to govern tables, lineage, and data quality.
#
# This notebook will use the `bus_rides` data and ML models to generate a time-series forcasting of ridership in the future, in order to alert us when a bus about to become full.
#
# We will evaluate the models accuracy and generate future data to be used in the next chapters for real-time predictions and alerting.

# %% [markdown]
# ## Environment Setup

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
# !pip install google-cloud-dataplex google-colabsqlviz --quiet

# %%
from google.cloud import bigquery, storage

bigquery_client = bigquery.Client(project=PROJECT_ID, location=LOCATION)
storage_client = storage.Client(project=PROJECT_ID)

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
# # Feature Engineering
#
# In our demo, we want to predict rise and spikes to the demand of bus lines. In order to do that, we will calculate a `demand_metric`, which will be high when all passengers cannot fit on a bus, and low when we have remaining capacity on a bus. This metric would allow us to deploy additional buses when it is above some threshold so that we can reduce the number of passengers that cannot board.
#
# Let's assume that the metric `demand_metric` depends on a number of factors:
#   * Station
#   * Borough
#   * Bus line
#   * Date and time (time series)
#
#   We will use the data generated in notebooks 1 & 2 to forecast ridership, based on these four factors.
#
#   Obviously, a couple of these variables are related (the station and borough are related). In a real-world scenario, this could be considered a bad practice, and lead to overfitting towards a specific variable.

# %% [markdown]
# ### Create bus rides table with features

# %%
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

# Export updated Iceberg metadata snapshot to GCS for external query engines
bigquery_client.query(
    f"EXPORT TABLE METADATA FROM {BQ_DATASET}.{FEATURES_TABLE_NAME}"
).result()

# %%
select_top_rows(FEATURES_TABLE_NAME)

# %% [markdown]
# ## Visualize Projected data
#
# Let's interactively explore each feature and its relationship with `demand_metric` using [`google-colabsqlviz`](https://docs.cloud.google.com/managed-spark/docs/guides/visualize-dataframes-notebooks) (`explore_dataframe`). Built on `anywidget` and the Data Studio charting engine, each cell renders an interactive widget where you can switch between **Chart**, **Data** (paginated table), and **Split** views, filter dimensions, change aggregations, and export filtered slices to a new DataFrame—while all aggregations run locally in your Python kernel.

# %% [markdown]
# ### Demand per bus line over time
# Let's take a look at the last 90 days of ridership data using `demand_metric` (`remaining_at_stop - remaining_capacity`). Higher values indicate passengers left behind after a bus departs, signaling where additional capacity may be needed. Use the widget's **Add filter** button to isolate specific `bus_line` values or switch to **Split** mode to inspect the underlying rows.

# %%
from google.colabsqlviz.explore_dataframe import explore_dataframe

DAYS_BACK = 90

demand_per_bus_line_df = (
    bigquery_client.query(
        f"""
DECLARE max_ts TIMESTAMP DEFAULT (SELECT MAX(timestamp_at_stop) FROM {BQ_DATASET}.{FEATURES_TABLE_NAME});
SELECT bus_line, timestamp_at_stop, AVG(demand_metric) AS demand_metric
  FROM `{BQ_DATASET}.{FEATURES_TABLE_NAME}`
  WHERE timestamp_at_stop > TIMESTAMP_SUB(max_ts, INTERVAL {DAYS_BACK} DAY)
  GROUP BY bus_line, timestamp_at_stop
  ORDER BY bus_line, timestamp_at_stop;
"""
    )
    .result()
    .to_dataframe()
)

explore_dataframe(demand_per_bus_line_df)

# %% [markdown]
# ### Demand per bus stop over time
# Filter by `bus_stop_id` in the widget toolbar to compare demand spikes across individual stops without sampling away the rest of the network.

# %%
DAYS_BACK = 90

demand_per_bus_stop_df = (
    bigquery_client.query(
        f"""
DECLARE max_ts TIMESTAMP DEFAULT (SELECT MAX(timestamp_at_stop) FROM {BQ_DATASET}.{FEATURES_TABLE_NAME});
SELECT bus_stop_id, timestamp_at_stop, AVG(demand_metric) AS demand_metric
  FROM `{BQ_DATASET}.{FEATURES_TABLE_NAME}`
  WHERE timestamp_at_stop > TIMESTAMP_SUB(max_ts, INTERVAL {DAYS_BACK} DAY)
  GROUP BY bus_stop_id, timestamp_at_stop
  ORDER BY bus_stop_id, timestamp_at_stop;
"""
    )
    .result()
    .to_dataframe()
)

explore_dataframe(demand_per_bus_stop_df)

# %% [markdown]
# ### Distribution of demand per Borough (quantiles & average)
# Switch between **Chart**, **Data**, and **Split** views (or select another chart type in the Setup panel) to compare minimum, quartile (`q1_demand`, `median_demand`, `q3_demand`), `mean_demand`, and maximum demand across boroughs.

# %%
average_demand_per_borough = (
    bigquery_client.query(
        f"""
SELECT
  borough,
  APPROX_QUANTILES(demand_metric, 100)[OFFSET(0)] AS min_demand,
  APPROX_QUANTILES(demand_metric, 100)[OFFSET(25)] AS q1_demand,
  APPROX_QUANTILES(demand_metric, 100)[OFFSET(50)] AS median_demand,
  AVG(demand_metric) AS mean_demand,
  APPROX_QUANTILES(demand_metric, 100)[OFFSET(75)] AS q3_demand,
  APPROX_QUANTILES(demand_metric, 100)[OFFSET(100)] AS max_demand
 FROM `{BQ_DATASET}`.`{FEATURES_TABLE_NAME}`
 GROUP BY borough
 ORDER BY median_demand DESC
"""
    )
    .result()
    .to_dataframe()
)

explore_dataframe(average_demand_per_borough)

# %% [markdown]
# ### Average demand per month

# %%
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
  transit_month
ORDER BY
  transit_month;
"""
    )
    .result()
    .to_dataframe()
)

explore_dataframe(average_demand_per_month)

# %% [markdown]
# ### Average Demand per Day-of-Week

# %%
# Group average demand by day of week (keep day_of_week_num for chronological sorting)
average_demand_per_day_of_week = (
    bigquery_client.query(
        f"""
SELECT
  EXTRACT(DAYOFWEEK FROM timestamp_at_stop) AS day_of_week_num,
  AVG(demand_metric) AS demand_metric
FROM
  `{BQ_DATASET}`.`{FEATURES_TABLE_NAME}`
GROUP BY
  day_of_week_num
ORDER BY
  day_of_week_num;
"""
    )
    .result()
    .to_dataframe()
)

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
    "day_of_week_num"
].map(days_of_week)

explore_dataframe(average_demand_per_day_of_week)


# %% [markdown]
# # Forecast bus ridership
#
# For time-series forecasting, there are 3 main options to choose from.
#
# ### ARIMA_PLUS
# This model is an enhanced version of the traditional ARIMA, offering improved performance and often including automatic parameter selection for better ease of use in forecasting univariate time series. It's a **Univariate** model, meaning, it is built to predict a target value based on only the timestamp variable. You cannot incorporate extra variables that might influence the prediction. For that reason, we will **NOT** use it here in our demo.
#
# ### ARIMA_PLUS_XREG
# Extending `ARIMA_PLUS`, this model incorporates the ability to utilize external regressors (exogenous variables) to enhance the forecasting accuracy by accounting for the influence of other relevant time series. Like the `ARIMA_PLUS`, it still relies on linearity assumptions between regressors and target. It is a multivariate model, in terms of inputs, but still forecasts a single output.
#
# ### TimesFM
# `TimesFM` is a deep learning-based forecasting model that leverages transformer architectures to capture complex temporal dependencies and long-range patterns in time series data, often excelling in multi-variate forecasting tasks. It's considered excellent at capturing complex non-linear patterns and long-range dependencies; naturally handles multivariate inputs and outputs; generally performs well on large datasets. It is known to overfit on small datasets.
#
# ## What's next?
#
# In the rest of this notebook, we will create predictions using `ARIMA_PLUS_XREG` and `TimesFM` and evaluate both models and compare their performance.
#
# We will create a summarized bus_rides table, aggregate the `demand_metric` by the hour, and use the linear time gaps filling strategy to train the ARIMA_PLUS_XREG model.
#

# %%
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


# %%
query = f"""
CREATE OR REPLACE TABLE `{BQ_DATASET}`.`{SUMMARIZED_FEATURES_TABLE}` AS

WITH agg_rides AS (
  SELECT
    TIMESTAMP_BUCKET(timestamp_at_stop, INTERVAL {INTERVALS_MINUTES} MINUTE) AS time,
    bus_line,
    bus_stop_id,
    borough,
    AVG(demand_metric) AS demand
   FROM `{BQ_DATASET}.{FEATURES_TABLE_NAME}`
   GROUP BY time, bus_line, bus_stop_id, borough)

SELECT *
FROM GAP_FILL(
  TABLE agg_rides,
  ts_column => 'time',
  bucket_width => INTERVAL {INTERVALS_MINUTES} MINUTE,
  partitioning_columns => ['bus_line', 'bus_stop_id', 'borough'],
  value_columns => [
    ('demand', 'linear')
  ]
)
ORDER BY time, bus_line, bus_stop_id;
"""
bigquery_client.query(query).result()


# %%
select_top_rows(SUMMARIZED_FEATURES_TABLE)

# %% [markdown]
# ## Multivariate forecasting using the ARIMA_PLUS_XREG model

# %% [markdown]
# ### Train the model
#
#
# The same CREATE MODEL statement is used to train this model Many options, e.g,  `time_series_data_col`, `time_series_timestamp_col`,  `time_series_id_col` have the same meaning as for the ARIMA_PLUS model.
#
# The main difference - the ARIMA_PLUS_XREG model uses all columns besides those identified by the options above as the feature columns and uses linear regression to calculate covariate weights.
#
# For details on the additional options, explanation of the training process, and best practices when training and using the model please refer to BigQuery documentation on [the CREATE MODEL statement for ARIMA_PLUS_XREG models](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-create-multivariate-time-series).
#
# The next query creates a model based on data between January 1st, 2022 and November 30th, 2024. The limitation of date ranges is due to our data size, which can't all fit within the training limits.
#
# We also chose a training dataset in the past, but leave some data "unseen" to the model, to help us evaluate its accuracy.
#
# Note, we are treating each bus line, as a separate time series dataset, using the `TIME_SERIES_ID_COL` parameter.
#

# %%
query = f"""
CREATE OR REPLACE MODEL `{BQ_DATASET}.{ARIMA_PLUS_XREG_MODEL_NAME}`
OPTIONS(
  MODEL_TYPE = 'ARIMA_PLUS_XREG',
  TIME_SERIES_ID_COL = ['bus_line', 'bus_stop_id'],
  TIME_SERIES_DATA_COL = 'demand',
  TIME_SERIES_TIMESTAMP_COL = 'time',
  AUTO_ARIMA=TRUE,
  HORIZON={HORIZON},
  DATA_FREQUENCY='AUTO_FREQUENCY',
  HOLIDAY_REGION = "US"  -- the original dataset is from NY
)
AS SELECT
    time,
    bus_line,
    bus_stop_id,
    borough,
    demand
  FROM `{BQ_DATASET}.{SUMMARIZED_FEATURES_TABLE}`
  WHERE
    time BETWEEN TIMESTAMP("{MIN_TS}") AND TIMESTAMP("{MAX_TS}");
"""
bigquery_client.query(query).result()


# %% [markdown]
# ### Evaluating the ARIMA_PLUS_XREG model

# %%
bigquery_client.query(
    f"SELECT * FROM ML.ARIMA_EVALUATE(MODEL `{BQ_DATASET}.{ARIMA_PLUS_XREG_MODEL_NAME}`);"
).result().to_dataframe()

# %%
query = f"""
SELECT *
FROM ML.EVALUATE(MODEL `{BQ_DATASET}.{ARIMA_PLUS_XREG_MODEL_NAME}`,
  (SELECT * FROM `{BQ_DATASET}.{SUMMARIZED_FEATURES_TABLE}`
    WHERE time BETWEEN TIMESTAMP("{EVAL_MIN_TS}") AND TIMESTAMP("{EVAL_MAX_TS}")
  )
);
"""

bigquery_client.query(query).result().to_dataframe()

# %%
# Let's save the forecast results to a table, so we can compare the 2 models later
query = f"""
CREATE OR REPLACE TABLE `{BQ_DATASET}.{ARIMA_PLUS_XREG_FORECAST_TABLE_NAME}`
AS
SELECT * FROM ML.FORECAST (
    MODEL `{BQ_DATASET}.{ARIMA_PLUS_XREG_MODEL_NAME}`,
    STRUCT (
      {HORIZON} AS horizon,
      0.9 AS confidence_level
    ),
    (
      SELECT
        timestamp_at_stop AS time,
        bus_line,
        bus_stop_id,
        borough
      FROM `{BQ_DATASET}.bus_rides_features`
    )
  )
"""
# print(query)
bigquery_client.query(query).result()
select_top_rows(ARIMA_PLUS_XREG_FORECAST_TABLE_NAME)

# %%
# We can even try to manually compare the forecast results with the actual requests
# let's pick one bus line at random, and compare the forecast results during a given day to the actual results from the same day

query = f"""
WITH forecast AS (
  SELECT * FROM ML.FORECAST (
    MODEL `{BQ_DATASET}.{ARIMA_PLUS_XREG_MODEL_NAME}`,
    STRUCT (
      {HORIZON} AS horizon,
      0.9 AS confidence_level
    ),
    (
      SELECT
        timestamp_at_stop AS time,
        bus_line,
        bus_stop_id,
        borough
      FROM `{BQ_DATASET}.bus_rides_features`
    )
  )
), actual AS (
  SELECT
    timestamp_at_stop AS time,
    bus_line,
    bus_stop_id,
    borough,
    demand_metric as observed_value,
    TIMESTAMP_BUCKET(timestamp_at_stop, INTERVAL {INTERVALS_MINUTES} MINUTE) AS time_bucket
  FROM {BQ_DATASET}.bus_rides_features
)

SELECT
  forecast.bus_line,
  forecast.bus_stop_id,
  forecast.forecast_timestamp,
  actual.time_bucket,
  actual.time AS actual_time,
  forecast.forecast_value,
  actual.observed_value
FROM forecast
INNER JOIN actual ON
  forecast.forecast_timestamp = time_bucket AND
  actual.bus_line = forecast.bus_line AND
  actual.bus_stop_id = forecast.bus_stop_id

  WHERE
  actual.bus_line = '{BUS_LINE_FOR_SAMPLING}' AND
  actual.time_bucket BETWEEN TIMESTAMP('{DAY_FOR_SAMPLING}T00:00:00') AND
    TIMESTAMP('{DAY_FOR_SAMPLING}T23:59:59')
"""

# print(query)
bigquery_client.query(query).result().to_dataframe()


# %% [markdown]
# ## TimesFM Model
#
# BigQuery's `TimesFM` is a pretrained, zero-shot foundation model for time-series forecasting. It's a decoder-only transformer model, similar to those used for language, but adapted for time series data. TimesFM was trained on a massive dataset of billions of real-world time points, which allows it to make accurate predictions on new datasets without needing any specific training on that data. This makes it highly versatile and easy for data analysts to use directly in BigQuery with a simple SQL function like `AI.FORECAST`.
#
# In this section, we will create a table with our forecast, view the results and compare the to the results to the forecasting results from the `ARIMA_PLUS_XREG` model. Although, we don't have a built-in function to evaluate the timesfm model.
#
# For the `ARIMA_PLUS_XREG` has a built-in `ML.EVALUATE` function, we will simulate the same for our `TimesFM` model, and compare the results.

# %% [markdown]
# ### Create a results table

# %%
# Create a table with the timesfm forecast
query = f"""
CREATE OR REPLACE TABLE `{BQ_DATASET}.{TIMESFM_TABLE_NAME}`
AS
SELECT *
FROM
  AI.FORECAST(
    (
      SELECT
        time,
        bus_line,
        bus_stop_id,
        borough,
        demand
      FROM `{BQ_DATASET}.summarized_features`
      WHERE
        time BETWEEN TIMESTAMP('{MIN_TS}') AND TIMESTAMP('{MAX_TS}')
    ),
    horizon => {HORIZON},
    confidence_level => 0.95,
    id_cols => ['bus_line', 'bus_stop_id'],
    timestamp_col => 'time',
    data_col => 'demand');
"""

bigquery_client.query(query).result()
select_top_rows(TIMESFM_TABLE_NAME)

# %% [markdown]
# ### View results against observed results

# %%
# Now we can review the forecast against the actual observed data

query = f"""
WITH forecast AS (
  SELECT
    bus_line,
    bus_stop_id,
    forecast_timestamp,
    forecast_value,
    prediction_interval_lower_bound,
    prediction_interval_upper_bound
  FROM `{BQ_DATASET}.{TIMESFM_TABLE_NAME}`
), actual AS (
  SELECT
    timestamp_at_stop AS time,
    bus_line,
    bus_stop_id,
    borough,
    demand_metric as observed_value,
    TIMESTAMP_BUCKET(timestamp_at_stop, INTERVAL {INTERVALS_MINUTES} MINUTE) AS time_bucket
  FROM {BQ_DATASET}.bus_rides_features
)

SELECT
  forecast.bus_line,
  forecast.bus_stop_id,
  forecast.forecast_timestamp,
  actual.time_bucket,
  actual.time AS actual_time,
  forecast.forecast_value,
  actual.observed_value
FROM forecast
LEFT JOIN actual ON
  forecast.forecast_timestamp = actual.time AND
  actual.bus_line = forecast.bus_line AND
  actual.bus_stop_id = forecast.bus_stop_id
  WHERE actual.bus_line = '{BUS_LINE_FOR_SAMPLING}'
ORDER BY forecast_timestamp
"""
# print(query)
bigquery_client.query(query).result().to_dataframe()

# %% [markdown]
# ## Comparing results from both models

# %%
# Finally, let's compare results of the forecast
# between the arima model and our timesfm model

# We'll start off by joining the 2 forecast tables with the actual data
# and save the results as a new table.
query = f"""
CREATE OR REPLACE TABLE `{BQ_DATASET}.{ACTUAL_VS_FORECAST_TABLE_NAME}` AS

WITH timesfm AS (
  SELECT
    bus_line,
    bus_stop_id,
    forecast_timestamp,
    forecast_value as timesfm_forecast_value,
  FROM `{BQ_DATASET}.{TIMESFM_TABLE_NAME}`
), arima AS (
  SELECT
    bus_line,
    bus_stop_id,
    forecast_timestamp,
    forecast_value as arima_forecast
    FROM `{BQ_DATASET}.{ARIMA_PLUS_XREG_FORECAST_TABLE_NAME}`
), actual AS (
  SELECT
    timestamp_at_stop AS time,
    bus_line,
    bus_stop_id,
    borough,
    demand_metric as observed_value,
    TIMESTAMP_BUCKET(timestamp_at_stop, INTERVAL {INTERVALS_MINUTES} MINUTE) AS time_bucket
  FROM {BQ_DATASET}.bus_rides_features
)
SELECT
  actual.time AS actual_time,
  actual.bus_line,
  actual.bus_stop_id,
  actual.borough,
  actual.observed_value,
  actual.time_bucket,
  timesfm.timesfm_forecast_value,
  arima.arima_forecast,
  ABS(actual.observed_value - timesfm.timesfm_forecast_value) AS timesfm_abs_error,
  ABS(actual.observed_value - arima.arima_forecast) AS arima_abs_error
FROM actual
LEFT JOIN timesfm ON
  actual.time_bucket = timesfm.forecast_timestamp AND
  actual.bus_line = timesfm.bus_line AND
  actual.bus_stop_id = timesfm.bus_stop_id
LEFT JOIN arima ON
  actual.time_bucket = arima.forecast_timestamp AND
  actual.bus_line = arima.bus_line AND
  actual.bus_stop_id = arima.bus_stop_id
WHERE
  arima.arima_forecast IS NOT NULL AND
  timesfm.timesfm_forecast_value IS NOT NULL
ORDER BY actual.time, actual.bus_line, actual.bus_stop_id
"""
# print(query)
bigquery_client.query(query).result()
actual_vs_forecast_df = select_top_rows(ACTUAL_VS_FORECAST_TABLE_NAME)
actual_vs_forecast_df.head()

# %%
from google.colabsqlviz.explore_dataframe import explore_dataframe

# Aggregate the full forecast horizon in BigQuery (instead of only the top-10 preview rows)
plot_df = (
    bigquery_client.query(
        f"""
SELECT
  time_bucket,
  AVG(observed_value) AS observed_value,
  AVG(timesfm_forecast_value) AS timesfm_forecast_value,
  AVG(arima_forecast) AS arima_forecast,
  AVG(timesfm_abs_error) AS timesfm_mae,
  AVG(arima_abs_error) AS arima_mae
FROM `{PROJECT_ID}.{BQ_DATASET}.{ACTUAL_VS_FORECAST_TABLE_NAME}`
GROUP BY time_bucket
ORDER BY time_bucket
"""
    )
    .result()
    .to_dataframe()
)

explore_dataframe(plot_df)

# %% [markdown]
# ## Governance, Data Quality & Downstream Lineage (Knowledge Catalog)
#
# Run an automated **Knowledge Catalog Data Quality** scan on the curated `bus_rides_features` Apache Iceberg table and inspect downstream BigQuery lineage linking `bus_rides` + `bus_stations` + `bus_lines` → `bus_rides_features` → `actual_vs_forecast`.

# %%
import time
from google.api_core import exceptions as api_exceptions
from google.cloud import dataplex_v1
from google.protobuf import field_mask_pb2

datascan_client = dataplex_v1.DataScanServiceClient()

FEATURES_DQ_SCAN_ID = "bus-rides-features-dq-scan"
dq_parent = f"projects/{PROJECT_ID}/locations/{LOCATION}"
features_dq_scan_name = f"{dq_parent}/dataScans/{FEATURES_DQ_SCAN_ID}"
features_dq_results_table = (
    f"//bigquery.googleapis.com/projects/{PROJECT_ID}"
    f"/datasets/{BQ_DATASET}/tables/bus_rides_features_dq_results"
)

features_dq_rules = [
    dataplex_v1.DataQualityRule(
        name="timestamp-at-stop-not-null",
        column="timestamp_at_stop",
        dimension="COMPLETENESS",
        threshold=1.0,
        non_null_expectation=dataplex_v1.DataQualityRule.NonNullExpectation(),
    ),
    dataplex_v1.DataQualityRule(
        name="bus-ride-id-not-null",
        column="bus_ride_id",
        dimension="COMPLETENESS",
        threshold=1.0,
        non_null_expectation=dataplex_v1.DataQualityRule.NonNullExpectation(),
    ),
    dataplex_v1.DataQualityRule(
        name="bus-line-not-null",
        column="bus_line",
        dimension="COMPLETENESS",
        threshold=1.0,
        non_null_expectation=dataplex_v1.DataQualityRule.NonNullExpectation(),
    ),
    dataplex_v1.DataQualityRule(
        name="borough-not-null",
        column="borough",
        dimension="COMPLETENESS",
        threshold=1.0,
        non_null_expectation=dataplex_v1.DataQualityRule.NonNullExpectation(),
    ),
    dataplex_v1.DataQualityRule(
        name="demand-metric-not-null",
        column="demand_metric",
        dimension="COMPLETENESS",
        threshold=1.0,
        non_null_expectation=dataplex_v1.DataQualityRule.NonNullExpectation(),
    ),
    dataplex_v1.DataQualityRule(
        name="remaining-capacity-pct-unit-interval",
        column="remaining_capacity_percentage",
        dimension="VALIDITY",
        threshold=1.0,
        range_expectation=dataplex_v1.DataQualityRule.RangeExpectation(
            min_value="0",
            max_value="1",
            strict_min_enabled=False,
            strict_max_enabled=False,
        ),
    ),
    dataplex_v1.DataQualityRule(
        name="left-behind-pct-unit-interval",
        column="passengers_left_behind_percentage",
        dimension="VALIDITY",
        threshold=1.0,
        range_expectation=dataplex_v1.DataQualityRule.RangeExpectation(
            min_value="0",
            max_value="1",
            strict_min_enabled=False,
            strict_max_enabled=False,
        ),
    ),
]

features_data_scan = dataplex_v1.DataScan(
    description="Automated Knowledge Catalog Data Quality scan for bus_rides_features Iceberg table.",
    data=dataplex_v1.DataSource(
        resource=f"//bigquery.googleapis.com/projects/{PROJECT_ID}/datasets/{BQ_DATASET}/tables/{FEATURES_TABLE_NAME}"
    ),
    data_quality_spec=dataplex_v1.DataQualitySpec(
        rules=features_dq_rules,
        post_scan_actions=dataplex_v1.DataQualitySpec.PostScanActions(
            bigquery_export=dataplex_v1.DataQualitySpec.PostScanActions.BigQueryExport(
                results_table=features_dq_results_table
            )
        ),
    ),
    execution_spec=dataplex_v1.DataScan.ExecutionSpec(
        trigger=dataplex_v1.Trigger(on_demand=dataplex_v1.Trigger.OnDemand())
    ),
)

try:
    datascan_client.create_data_scan(
        request=dataplex_v1.CreateDataScanRequest(
            parent=dq_parent,
            data_scan=features_data_scan,
            data_scan_id=FEATURES_DQ_SCAN_ID,
        )
    ).result()
    print(f"Created Data Quality scan: {features_dq_scan_name}")
except api_exceptions.AlreadyExists:
    features_data_scan.name = features_dq_scan_name
    datascan_client.update_data_scan(
        request=dataplex_v1.UpdateDataScanRequest(
            data_scan=features_data_scan,
            update_mask=field_mask_pb2.FieldMask(
                paths=["description", "data_quality_spec", "execution_spec"]
            ),
        )
    ).result()
    print(f"Updated existing Data Quality scan: {features_dq_scan_name}")

run_response = datascan_client.run_data_scan(
    request=dataplex_v1.RunDataScanRequest(name=features_dq_scan_name)
)
job_name = run_response.job.name
terminal_states = {
    dataplex_v1.DataScanJob.State.SUCCEEDED,
    dataplex_v1.DataScanJob.State.FAILED,
    dataplex_v1.DataScanJob.State.CANCELLED,
}
while True:
    job = datascan_client.get_data_scan_job(
        request=dataplex_v1.GetDataScanJobRequest(
            name=job_name,
            view=dataplex_v1.GetDataScanJobRequest.DataScanJobView.FULL,
        )
    )
    if job.state in terminal_states:
        break
    time.sleep(10)

dq_result = job.data_quality_result
print(
    f"Job state: {job.state.name} | Passed: {dq_result.passed} | Row count: {dq_result.row_count}"
)
print(
    f"DQ Scan URL: https://console.cloud.google.com/dataplex/govern/quality/projects/{PROJECT_ID}/locations/{LOCATION}/dataScans/{FEATURES_DQ_SCAN_ID}?project={PROJECT_ID}"
)
print(
    f"Lineage (bus_rides_features): https://console.cloud.google.com/bigquery?project={PROJECT_ID}&ws=!1m5!1m4!4m3!1s{PROJECT_ID}!2s{BQ_DATASET}!3s{FEATURES_TABLE_NAME}"
)
print(
    f"Lineage (actual_vs_forecast): https://console.cloud.google.com/bigquery?project={PROJECT_ID}&ws=!1m5!1m4!4m3!1s{PROJECT_ID}!2s{BQ_DATASET}!3s{ACTUAL_VS_FORECAST_TABLE_NAME}"
)
pd.DataFrame(
    [
        {
            "rule": r.rule.name,
            "dimension": r.rule.dimension,
            "column": r.rule.column or "(row)",
            "passed": r.passed,
            "pass_ratio": r.pass_ratio,
            "failing_rows_query": r.failing_rows_query,
        }
        for r in dq_result.rules
    ]
)

# %% [markdown]
# # Conclusion

# %% [markdown]
# In this notebook, we used our open data lakehouse with **BigQuery ML** (`ARIMA_PLUS_XREG`) and **BigQuery AI** (`AI.FORECAST` powered by **TimesFM**) to forecast bus stop demand directly inside BigQuery SQL without sacrificing Apache Iceberg openness and portability, and validated the curated feature table with **Knowledge Catalog** Data Quality scans and automatic lineage.
#
# We compared both time-series forecasting approaches side by side. While zero-shot **TimesFM** provides strong out-of-the-box accuracy in this demo, `ARIMA_PLUS_XREG` can still be tuned further for specific routes or exogenous drivers.
#
# In the next chapter (Cloud Run real-time dashboard), we stream bus events through Managed Service for Apache Kafka and **Serverless for Apache Spark** Structured Streaming to monitor live capacity and emit overcrowding alerts.
