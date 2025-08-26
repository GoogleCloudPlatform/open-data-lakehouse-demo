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

from open_data_lakehouse_demo.utils import BigQueryService
from open_data_lakehouse_demo.kafka_service import continuous_message_producer

templates_dir = os.path.join(os.path.dirname(__file__), "templates")

BQ_DATASET = os.getenv("BQ_DATASET", "ridership_lakehouse")
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP")

app = Flask(__name__, template_folder=templates_dir)
executor = Executor(app)

app.config["bq_client"] = BigQueryService(BQ_DATASET)
app.config["producer_stop_event"] = threading.Event()
app.config["producer_task_id"] = None  # Store the task ID


@app.route("/check-for-updates", methods=["GET"])
def check_for_updates():
	return {"foo": "updates"}


@app.route("/start_simulation", methods=["POST"])
def start_simulation():
	logging.info("Starting simulation...")
	if app.config["producer_task_id"] is not None and not executor.futures.done(app.config["producer_task_id"]):
		return jsonify({"message": "Producer is already running."}), 409
	
	# Reset the stop event and submit the continuous producer task
	app.config["producer_stop_event"].clear()
	app.config["producer_task_id"] = executor.submit(continuous_message_producer, app.config["producer_stop_event"],
													 KAFKA_BOOTSTRAP, "bus-updates")

	return jsonify({"message": "Kafka producer started in the background."})
	# get all bus_lines
	# for each bus_line
		# get compareable data from this date and time in 2024
	# pass all data to background worker to send kafka messages
	# update state
	# return {"foo": "bar"}


@app.route("/stop_simulation", methods=["POST"])
def stop_simulation():
	# stop background worker
	# update state
	return {"foo": "bar"}


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