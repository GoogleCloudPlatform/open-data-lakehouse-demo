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
from flask import Flask, render_template, jsonify
from flask_executor import Executor
import threading # Used to manage the stop signal for the background task

from open_data_lakehouse_demo.bq_service import BigQueryService
from open_data_lakehouse_demo.kafka_service import KafkaService
from open_data_lakehouse_demo.pyspark_service import PySparkService

templates_dir = os.path.join(os.path.dirname(__file__), "templates")

BQ_DATASET = os.getenv("BQ_DATASET", "ridership_lakehouse")
PROJECT_ID = os.getenv("PROJECT_ID")
REGION = os.getenv("REGION")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")

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


def get_kafka_status():
    kafka_service = KafkaService()
    if not app.config[KAFKA_TASK_ID_KEY]:
         return {"status": "inactive", "message": "No kafka producer has been submitted."}
    
    if app.config[KAFKA_TASK_ID_KEY].running():
        return {
            "status": "active",
            "message": "Kafka producer job is running.",
            "stats": kafka_service.get_stats(),
        }

    if app.config[KAFKA_TASK_ID_KEY].done():
        try:
            result = app.config[KAFKA_TASK_ID_KEY].result()
            return {
                "status": "finished",
                "message": "Kafka producer job has completed.",
                "result": str(result),
                "stats": kafka_service.get_stats(),
            }
        except Exception as e:
             return {
                "status": "error",
                "message": f"Kafka producer job failed with an exception: {e}"
            }
        
    return {"status": "unknown", "message": "Could not determine job status."}

def get_spark_status():
    spark_service = PySparkService()
    if not app.config[SPARK_TASK_ID_KEY]:
         return {"status": "inactive", "message": "No job has been submitted."}
    
    if app.config[SPARK_TASK_ID_KEY].running():
        if spark_service.get_stats_query() and spark_service.get_stats_query().isActive:
            return {
                "status": "active",
                "message": "Spark job is running and stream is active.",
                "query_status": spark_service.get_stats_query().status,
                "stats": spark_service.get_stats()
            }
        else:
            return {
                "status": "initializing",
                "message": "Spark job is running, but stream is not active yet."
            }

    if app.config[SPARK_TASK_ID_KEY].done():
        try:
            result = app.config[SPARK_TASK_ID_KEY].result()
            return {
                "status": "finished",
                "message": "Spark job has completed.",
                "result": str(result)
                }
        except Exception as e:
             return {
                "status": "error",
                "message": f"Spark job failed with an exception: {e}"
            }
        
    return {"status": "unknown", "message": "Could not determine job status."}

def ensure_spark() -> dict:
    if app.config[SPARK_TASK_ID_KEY] is not None and not executor.futures.done(app.config[SPARK_TASK_ID_KEY]):
        return {"message": "Spark streaming is already running."}
        
    logging.info("Starting spark streaming app...")
    spark_service = PySparkService()
    app.config[SPARK_EVENT_KEY].clear()
    app.config[SPARK_TASK_ID_KEY] = executor.submit(spark_service.start_pyspark, PROJECT_ID, REGION, KAFKA_BOOTSTRAP, 
                                                    "bus-updates")
    return {"message": "Spark streaming started in the background."}

def ensure_kafka() -> dict:
    if app.config[KAFKA_TASK_ID_KEY] is not None and not executor.futures.done(app.config[KAFKA_TASK_ID_KEY]):
        return {"message": "Producer is already running."}
    
    logging.info("Starting kafka producer...")
    # Reset the stop event and submit the continuous producer task
    app.config[KAFKA_EVENT_KEY].clear()
    kafka_service = KafkaService()
    app.config[KAFKA_TASK_ID_KEY] = executor.submit(
        kafka_service.continuous_message_producer,
        app.config[KAFKA_EVENT_KEY],
        KAFKA_BOOTSTRAP, "bus-updates")
    return {"message": "Kafka producer started in the background."}

def stop_spark():
    app.config[SPARK_EVENT_KEY].set()
    app.config[SPARK_TASK_ID_KEY].cancel()
    app.config[SPARK_TASK_ID_KEY] = None

def stop_kafka():
    app.config[KAFKA_EVENT_KEY].set()
    app.config[KAFKA_TASK_ID_KEY].cancel()
    app.config[KAFKA_TASK_ID_KEY] = None


@app.route("/check-for-updates", methods=["GET"])
def check_for_updates():
    return jsonify({
        "spark": get_spark_status(),
        "kafka": get_kafka_status(),
    })

@app.route("/start_simulation", methods=["POST"])
def start_simulation():
    spark_status = ensure_spark()
    kafka_status = ensure_kafka()
    return jsonify({"kafka": kafka_status, "spark": spark_status})

@app.route("/stop_simulation", methods=["POST"])
def stop_simulation():
    stop_spark()
    stop_kafka()
    return jsonify({"message": "Simulation stopped."})


@app.route("/")
@app.route("/index")
def index():
    bus_lines = app.config["bq_client"].get_all_bus_lines()
    return render_template("index.html", bus_lines=bus_lines)

if __name__ == '__main__':
    import google.cloud.logging
    client = google.cloud.logging.Client()
    client.setup_logging()
    logging.basicConfig(level=logging.INFO)
    app.run(debug=True, host="0.0.0.0", port=8080)