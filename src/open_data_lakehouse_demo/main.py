# Copyright 2024 Google LLC
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
import logging
import os
import threading  # Used to manage the stop signal for the background task

from flask import Flask, render_template, jsonify
from flask_executor import Executor

from open_data_lakehouse_demo.bq_service import BigQueryService
from open_data_lakehouse_demo.kafka_service import KafkaService
from open_data_lakehouse_demo.pyspark_service import PySparkService

templates_dir = os.path.join(os.path.dirname(__file__), "templates")

# Read environment variables
BQ_DATASET = os.environ["BQ_DATASET"]
PROJECT_ID = os.environ["PROJECT_ID"]
GCS_MAIN_BUCKET = os.environ["GCS_MAIN_BUCKET"]
REGION = os.environ["REGION"]
KAFKA_BOOTSTRAP = os.environ["KAFKA_BOOTSTRAP"]
KAFKA_TOPIC = os.environ["KAFKA_TOPIC"]
KAFKA_ALERT_TOPIC = os.environ["KAFKA_ALERT_TOPIC"]
SPARK_TMP_BUCKET = os.environ["SPARK_TMP_BUCKET"]
SPARK_CHECKPOINT_LOCATION = os.environ["SPARK_CHECKPOINT_LOCATION"]
BIGQUERY_TABLE = os.environ["BIGQUERY_TABLE"]
SUBNET_URI = os.environ["SUBNET_URI"]
app = Flask(__name__, template_folder=templates_dir)
executor = Executor(app)

app.config["bq_client"] = BigQueryService(BQ_DATASET)

KAFKA_EVENT_KEY = "kafka_event"
KAFKA_TASK_ID_KEY = "kafka_task_id"

SPARK_EVENT_KEY = "spark_event"
SPARK_TASK_ID_KEY = "spark_task_id"

app.config[KAFKA_EVENT_KEY] = threading.Event()
app.config[KAFKA_TASK_ID_KEY] = None

app.config[SPARK_EVENT_KEY] = threading.Event()
app.config[SPARK_TASK_ID_KEY] = None

spark_service = PySparkService(
    PROJECT_ID,
    REGION,
    GCS_MAIN_BUCKET,
    KAFKA_BOOTSTRAP,
    KAFKA_TOPIC,
    KAFKA_ALERT_TOPIC,
    SPARK_TMP_BUCKET,
    SPARK_CHECKPOINT_LOCATION,
    BQ_DATASET,
    BIGQUERY_TABLE,
    SUBNET_URI
)

@app.route("/spark_status", methods=["GET"])
def spark_status():
    global spark_service
    if spark_service.status.is_running:
        return jsonify({
            **spark_service.status.to_dict(),
            "stats": spark_service.get_stats()
        })
    else:
        return jsonify(spark_service.status.to_dict())

@app.route("/kafka_status", methods=["GET"])
def kafka_status():
    kafka_service = KafkaService()
    if not app.config[KAFKA_TASK_ID_KEY]:
         return jsonify({"status": "inactive", "message": "No kafka producer has been submitted."})

    if app.config[KAFKA_TASK_ID_KEY].running():
        return jsonify(
            {
                "status": "active",
                "message": "Kafka producer job is running.",
                "stats": kafka_service.get_stats(),
            }
        )

    if app.config[KAFKA_TASK_ID_KEY].done():
        try:
            result = app.config[KAFKA_TASK_ID_KEY].result()
            return jsonify(
                {
                    "status": "finished",
                    "message": "Kafka producer job has completed.",
                    "result": str(result),
                    "stats": kafka_service.get_stats(),
                }
            )
        except Exception as e:
             return jsonify({
                "status": "error",
                "message": f"Kafka producer job failed with an exception: {e}"
            })

    return jsonify({"status": "unknown", "message": "Could not determine job status."})

@app.route("/start_spark_simulation", methods=["POST"])
def start_spark_simulation():
    global spark_service
    if spark_service.status.is_running:
        logging.info("Spark streaming app is already running.")
        return jsonify(spark_service.status.to_dict())
    logging.info("Starting spark streaming app...")
    if app.config[SPARK_EVENT_KEY]:
        app.config[SPARK_EVENT_KEY].set()
    if app.config[SPARK_TASK_ID_KEY]:
        app.config[SPARK_TASK_ID_KEY].cancel()
    app.config[SPARK_TASK_ID_KEY] = None
    app.config[SPARK_TASK_ID_KEY] = executor.submit(spark_service.start_pyspark, app.config[SPARK_EVENT_KEY])
    return jsonify(spark_service.status.to_dict())

@app.route("/start_kafka_simulation", methods=["POST"])
def start_kafka_simulation():
    if app.config[KAFKA_TASK_ID_KEY] is not None and not executor.futures.done(app.config[KAFKA_TASK_ID_KEY]):
        return jsonify({"message": "Producer is already running."})

    logging.info("Starting kafka producer...")
    # Reset the stop event and submit the continuous producer task
    app.config[KAFKA_EVENT_KEY].clear()
    kafka_service = KafkaService()
    app.config[KAFKA_TASK_ID_KEY] = executor.submit(
        kafka_service.start_kafka_messages_stream,
        app.config[KAFKA_EVENT_KEY],
        KAFKA_BOOTSTRAP, "bus-updates")
    return jsonify({"message": "Kafka producer started in the background."})

@app.route("/stop_spark_simulation", methods=["POST"])
def stop_spark_simulation():
    global spark_service
    stop_status = spark_service.cancel_job()
    if app.config[SPARK_EVENT_KEY]:
        app.config[SPARK_EVENT_KEY].set()
    if app.config[SPARK_TASK_ID_KEY]:
        app.config[SPARK_TASK_ID_KEY].cancel()
    app.config[SPARK_TASK_ID_KEY] = None
    return jsonify(stop_status)

@app.route("/stop_kafka_simulation", methods=["POST"])
def stop_kafka_simulation():
    if app.config[KAFKA_EVENT_KEY]:
        app.config[KAFKA_EVENT_KEY].set()
    if app.config[KAFKA_TASK_ID_KEY]:
        app.config[KAFKA_TASK_ID_KEY].cancel()
    app.config[KAFKA_TASK_ID_KEY] = None
    return jsonify({"message": "Kafka producer stopped."})

@app.route("/")
@app.route("/index")
def index():
    bus_lines = app.config["bq_client"].get_all_bus_lines()
    spark_link = spark_service.pyspark_main_file.replace("gs://", "https://storage.mtls.cloud.google.com/")
    return render_template("index.html", bus_lines=bus_lines, pyspark_file_link=spark_link)

if __name__ == '__main__':
    import google.cloud.logging
    client = google.cloud.logging.Client()
    client.setup_logging()
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True, host="0.0.0.0", port=8080)