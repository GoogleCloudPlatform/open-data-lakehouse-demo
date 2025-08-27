# kafka_service.py
import os
import random

from kafka import KafkaProducer
import json
import time
import logging
from open_data_lakehouse_demo.bq_service import BigQueryService

producer = None

def get_kafka_producer(bootstrap_servers='localhost:29092'):
    global producer
    if producer is None:
        producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8')
        )
    return producer

def continuous_message_producer(stop_event, bootstrap_servers, topic, interval_seconds=5):
    """Continuously sends Kafka messages until the stop_event is set."""
    import google.cloud.logging
    client = google.cloud.logging.Client()
    client.setup_logging()
    logging.basicConfig(level=logging.INFO)
    
    # Get data from the past, with updated timestamps to simulate new data
    bigquery_client = BigQueryService(os.getenv("BQ_DATASET"))
    rides_data = bigquery_client.get_rides_data()
    
    # create kafka producer instance
    producer_instance = get_kafka_producer(bootstrap_servers)
    logging.info(f"Got {len(rides_data)} rides data. Sending data to kafka (Bootstrap: {bootstrap_servers}, "
                 f"topic: {topic}, connected {producer_instance.bootstrap_connected()})")
    message_count = 0
    while not stop_event.is_set() and message_count < len(rides_data):
        ride_data = rides_data[message_count]
        
        # every once in a while, we will simulate sudden spike of passengers, just to make things interesting.
        sudden_spike = random.random() < 0.05
        if sudden_spike and not ride_data['last_stop']:
            logging.info("Simulating sudden spike of passengers")
            # recalculating the data point, to accommodate new passengers in stop
            on_bus_before_stop = (ride_data["total_passengers"] +
                                  ride_data["passengers_alighting"] -
                                  ride_data["passengers_boarding"])
            on_bus_after_alighting = on_bus_before_stop - ride_data["passengers_alighting"]
            remaining_capacity_after_alighting = ride_data["total_capacity"] - on_bus_after_alighting
            ride_data["passengers_in_stop"] *= 3
            ride_data["passengers_boarding"] = min(ride_data["passengers_in_stop"], remaining_capacity_after_alighting)
            ride_data["remaining_at_stop"] = ride_data["passengers_in_stop"] - ride_data["passengers_boarding"]
            ride_data["total_passengers"] = on_bus_after_alighting + ride_data["passengers_boarding"]
            ride_data["remaining_capacity"] = ride_data["total_capacity"] - ride_data["total_passengers"]
        
        message_count += 1
        message = {"id": message_count, "timestamp": time.time(), "data": ride_data}
        logging.info(f"Sending message {message_count}")
        future = (producer_instance.send(topic, message)
                    .add_errback(lambda e: logging.error(f"from errback: {e}")))
        record_metadata = future.get(timeout=10) # Block until message is sent
        logging.info(f"Message sent to Kafka: Topic={record_metadata.topic}, Partition={record_metadata.partition}, Offset={record_metadata.offset}")
        time.sleep(interval_seconds)
    logging.info("Kafka continuous producer stopped.")
