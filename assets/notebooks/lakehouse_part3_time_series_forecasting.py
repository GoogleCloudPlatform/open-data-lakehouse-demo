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

# %% id="lvNpj26L7bbY67xWsvugZull"
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

# %% [markdown] id="g4xlhet9alul"
# # Ridership Open Lakehouse Demo (Part 3): Time-series forecasting of ridership data
#
# This notebook will demonstrate a strategy to implement an open lakehouse on GCP, using Apache Iceberg, as an open source standard for managing data, while still leveraging GCP native capabilities. This demo will use BigQuery Manged Iceberg Tables, Managed Apache Kafka and Apache Kafka Connect to ingest streaming data, Vertex AI for Generative AI queries on top of the data and Dataplex to govern tables.
#
# This notebook will use the `bus_rides` data and ML models to generate a time-series forcasting of ridership in the future, in order to alert us when a bus about to become full.
#
# We will evaluate the models accuracy and generate future data to be used in the next chapters for real-time predictions and alerting.

# %% [markdown] id="TJNbPGBEgQKC"
# ## Environment Setup

# %% id="UbBMzAcsgaOW"
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

# %% id="NXsTs1pda7rF"
from google.cloud import bigquery, storage

bigquery_client = bigquery.Client(
    project=PROJECT_ID, location=LOCATION, client_info=ClientInfo(user_agent=USER_AGENT)
)
storage_client = storage.Client(
    project=PROJECT_ID, client_info=ClientInfo(user_agent=USER_AGENT)
)

# %% id="QdvGeeLjgi0N"
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


# %% [markdown] id="4ZeA3dCQPpVE"
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

# %% [markdown] id="OeEJB9VBT7-m"
# ### Create bus rides table with features

# %% id="zYO9_4gfJLXB"
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

# %% id="QnBJKHcuo3Bf"
select_top_rows(FEATURES_TABLE_NAME)

# %% [markdown] id="3yMyeDdVUXHY"
# ## Visualize Projected data
#
# Let's now create a visualization for each of these features, the relationship with the `ridership` column, to see the effects of different features on the target variable.

# %% [markdown] id="ue5rOAnkJ_IE"
# ### Demand per bus line over time
# Let's take a look at the last 90 days of ridership data. We'll specifically look at the `remaining_at_stop` as our metric to determined if we need to alert of high demand. The higher the `remaining_at_stop`, the more demand we have, and we might want to dispatch more buses.

# %% id="GWVIBjf8Z2pK"
import matplotlib.pyplot as plt
import pandas as pd
import random

DAYS_BACK = 90

demand_per_bus_line_df = (
    bigquery_client.query(
        f"""
DECLARE max_ts TIMESTAMP DEFAULT (SELECT MAX(timestamp_at_stop) FROM {BQ_DATASET}.{FEATURES_TABLE_NAME});
SELECT bus_line, timestamp_at_stop as timestamp_at_stop, AVG(demand_metric) AS demand_metric
  FROM `{BQ_DATASET}.{FEATURES_TABLE_NAME}`
  WHERE timestamp_at_stop > TIMESTAMP_SUB(max_ts, INTERVAL {DAYS_BACK} DAY)
  GROUP BY bus_line, timestamp_at_stop
  ORDER BY bus_line, timestamp_at_stop;
"""
    )
    .result()
    .to_dataframe()
)

random.seed(42)

# we'll sample 20 random lines, as displaying all of them is not practical
bus_line_ids = random.sample(list(demand_per_bus_line_df.bus_line.unique()), k=20)

figure = plt.figure(figsize=(20, 6))
plt.xlabel("Timestamp at stop")
# Group data by station ID
for bus_line_id in bus_line_ids:
    station_data = demand_per_bus_line_df[
        demand_per_bus_line_df["bus_line"] == bus_line_id
    ].sort_values(by="timestamp_at_stop")
    # Plot ridership over time for the current bus line
    plt.bar(
        station_data["timestamp_at_stop"],
        station_data["demand_metric"],
        label=bus_line_id,
    )

# Customize the plot
plt.xlabel("Timestamp at stop")
plt.ylabel("Remaining Passengers")
plt.title("Remaining Passengers Over Time by Bus Line")
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# %% id="r9yRT9ZwH5U6"
DAYS_BACK = 90

demand_per_bus_stop_df = (
    bigquery_client.query(
        f"""
DECLARE max_ts TIMESTAMP DEFAULT (SELECT MAX(timestamp_at_stop) FROM {BQ_DATASET}.{FEATURES_TABLE_NAME});
SELECT bus_stop_id, timestamp_at_stop as timestamp_at_stop, AVG(demand_metric) AS demand_metric
  FROM `{BQ_DATASET}.{FEATURES_TABLE_NAME}`
  WHERE timestamp_at_stop > TIMESTAMP_SUB(max_ts, INTERVAL {DAYS_BACK} DAY)
  GROUP BY bus_stop_id, timestamp_at_stop
  ORDER BY bus_stop_id, timestamp_at_stop;
"""
    )
    .result()
    .to_dataframe()
)

random.seed(42)

# we'll sample 20 random lines, as displaying all of them is not practical
bus_stop_ids = random.sample(list(demand_per_bus_stop_df.bus_stop_id.unique()), k=20)

figure = plt.figure(figsize=(20, 6))
plt.xlabel("Timestamp at stop")
# Group data by station ID
for bus_stop_id in bus_stop_ids:
    station_data = demand_per_bus_stop_df[
        demand_per_bus_stop_df["bus_stop_id"] == bus_stop_id
    ].sort_values(by="timestamp_at_stop")
    # Plot ridership over time for the current station
    plt.bar(
        station_data["timestamp_at_stop"],
        station_data["demand_metric"],
        label=bus_stop_id,
    )

# Customize the plot
plt.xlabel("Timestamp at stop")
plt.ylabel("Demand")
plt.title("Demand Over Time by Bus Stop")
plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# %% [markdown] id="tG9QTLedfSnT"
# ### Distribution of demand per Borough (boxplot)

# %% id="ld9h94TofKOZ"

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

# %% [markdown] id="SFph12cCmibo"
# ### Average demand per month

# %% id="G-5DC96y03uE"
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


# %% [markdown] id="siu6JTn9mrQK"
# ### Average Demand per Day-of-Week

# %% id="FBbFMM971nYk"
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


# %% [markdown] id="hfMnFM06jIyJ"
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

# %% id="YDztgKflqBXV"
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


# %% id="X_Y1TjQBsVdT"
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


# %% id="-FcJHjtlu3gJ"
select_top_rows(SUMMARIZED_FEATURES_TABLE)

# %% [markdown] id="6F7BaDwQaECx"
# ## Multivariate forecasting using the ARIMA_PLUS_XREG model

# %% [markdown] id="yzogKPO25C0H"
# ### Train the model
#
#
# The same CREATE MODEL statement is used to train this model Many options, e.g,  `time_series_data_col`, `time_series_timestamp_col`,  `time_series_id_col` have the same meaning as for the ARIMA_PLUS model.
#
# The main difference - the ARIMA_PLUS_XREG model uses all columns besides those identified by the options above as the feature columns and uses linear regression to calculate covariate weights.
#
# For details on the additional options, explanation of the training process, and best practices when training and using the model please refer to BigQuery documentation on [the CREATE MODEL statement for ARIMA_PLUS_XREG models](https://cloud.google.com/bigquery/docs/reference/standard-sql/bigqueryml-syntax-create-multivariate-time-series).
#
# The next query, will create a model based on data between June 1st, 2022 and 30th of November 2024. The limitation of date ranges is due to our data size, which can't all fit to the training limitations.
#
# We also chose a training dataset in the past, but leave some data "unseen" to the model, to help us evaluate its accuracy.
#
# Note, we are treating each bus line, as a separate time series dataset, using the `TIME_SERIES_ID_COL` parameter.
#

# %% id="xM1y3GXljNwn"
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


# %% [markdown] id="O4g_N5GukpKD"
# ### Evaluating the ARIMA_PLUS_XREG model

# %% id="0DFdjosPai9q"
bigquery_client.query(
    f"SELECT * FROM ML.ARIMA_EVALUATE(MODEL `{BQ_DATASET}.{ARIMA_PLUS_XREG_MODEL_NAME}`);"
).result().to_dataframe()

# %% id="xdH7EkBSb9X8"
query = f"""
SELECT *
FROM ML.EVALUATE(MODEL `{BQ_DATASET}.{ARIMA_PLUS_XREG_MODEL_NAME}`,
  (SELECT * FROM `{BQ_DATASET}.{SUMMARIZED_FEATURES_TABLE}`
    WHERE time BETWEEN TIMESTAMP("{EVAL_MIN_TS}") AND TIMESTAMP("{EVAL_MAX_TS}")
  )
);
"""

bigquery_client.query(query).result().to_dataframe()

# %% id="y0TFX-HRkc8x"
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

# %% id="6NjqHE00YquO"
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


# %% [markdown] id="N8Fj_5qDt0CX"
# ## TimesFM Model
#
# BigQuery's `TimesFM` is a pretrained, zero-shot foundation model for time-series forecasting. It's a decoder-only transformer model, similar to those used for language, but adapted for time series data. TimesFM was trained on a massive dataset of billions of real-world time points, which allows it to make accurate predictions on new datasets without needing any specific training on that data. This makes it highly versatile and easy for data analysts to use directly in BigQuery with a simple SQL function like `AI.FORECAST`.
#
# In this section, we will create a table with our forecast, view the results and compare the to the results to the forecasting results from the `ARIMA_PLUS_XREG` model. Although, we don't have a built-in function to evaluate the timesfm model.
#
# For the `ARIMA_PLUS_XREG` has a built-in `ML.EVALUATE` function, we will simulate the same for our `TimesFM` model, and compare the results.

# %% [markdown] id="4AJKzp9zv8LX"
# ### Create a results table

# %% id="PGuYYcBJt4UI"
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

# %% [markdown] id="ck5xk0YcwDab"
# ### View results against observed results

# %% id="-ZrAVATb5BkJ"
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

# %% [markdown] id="xHGV4emLv0I-"
# ## Comparing results from both models

# %% id="pvnzAC59IFsc"
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
bigquery_client.query(query).result().to_dataframe()
actual_vs_forecast_df = select_top_rows(ACTUAL_VS_FORECAST_TABLE_NAME)
actual_vs_forecast_df.head()

# %% id="M53P0GpL-XjL"
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

# %% [markdown] id="6LW15mttL7QA"
# # Conclusion

# %% [markdown] id="CcuS8-5QWgPP"
# In this notebook, we used our open data lakehouse and connected it to BigQueryML and VertexAI to leverage native Google Cloud's capabilities to enhance our workflow, without losing mobility and freedom of our data.
#
# We've created tried 2 time-series forecasting models, and compared the results. We've seen the TimesFM model being the clearly better model for our demo. That should not take away from trying out more options. Even here, the ARIMA_PLUS_XREG model can still be tweaked and changed in order to improve its accuracy.
#
# In the next sections, we will use the TimesFM model to perform near-real-time predictions to anticipate spikes given new data from buses.
