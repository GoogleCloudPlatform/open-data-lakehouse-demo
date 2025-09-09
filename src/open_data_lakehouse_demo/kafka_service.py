# kafka_service.py
import _struct
import json
import logging
import os
import time
import confluent_kafka
from confluent_kafka import KafkaException
from confluent_kafka.serialization import Serializer, SerializationError

from open_data_lakehouse_demo.bq_service import BigQueryService
from open_data_lakehouse_demo.token_provider import TokenProvider


class JsonSerializer(Serializer):
    def __call__(self, obj, ctx=None):
        if obj is None:
            return None
        try:
            return json.dumps(obj, default=str).encode('utf-8')
        except _struct.error as e:
            raise SerializationError(str(e))


class KafkaService:
    producer: confluent_kafka.SerializingProducer = None
    total_messages = 0
    sent_messages = 0
    token_provider = TokenProvider()


    @classmethod
    def get_kafka_producer(cls, bootstrap_servers='localhost:29092'):
        config = {
            'bootstrap.servers': bootstrap_servers,
            'value.serializer': JsonSerializer()
        }
        if not bootstrap_servers.startswith("localhost"):
            # assume prod env
            config["security.protocol"] = "SASL_SSL"
            config["sasl.mechanism"] = "OAUTHBEARER"
            config["oauth_cb"] = cls.token_provider.get_token

        if cls.producer is None:
            cls.producer = confluent_kafka.SerializingProducer(config)
        return cls.producer

    # Delivery callback function to handle message delivery reports
    @staticmethod
    def delivery_callback(error, message):
        if error is not None:
            logging.error(f"Message delivery failed: {error}")
        else:
            logging.info(f"Message delivered to topic '{message.topic()}' partition [{message.partition()}] offset {message.offset()}")


    @classmethod
    def continuous_message_producer(cls, stop_event, bootstrap_servers, topic, interval_seconds=5):
        """Continuously sends Kafka messages until the stop_event is set."""
        # Get data from the past, with updated timestamps to simulate new data
        bigquery_client = BigQueryService(os.getenv("BQ_DATASET"))
        rides_data = bigquery_client.get_rides_data()
        cls.total_messages = len(rides_data)
        # create kafka producer instance
        producer_instance = cls.get_kafka_producer(bootstrap_servers)
        logging.info(f"Got {len(rides_data)} rides data.")
        cls.sent_messages = 0
        while not stop_event.is_set() and cls.sent_messages < len(rides_data):
            ride_data = rides_data[cls.sent_messages]

            cls.sent_messages += 1
            message = {"id": cls.sent_messages, "timestamp": time.time(), "data": ride_data}
            logging.info(f"Sending message {cls.sent_messages}")
            try:
                producer_instance.produce(topic, value=message, on_delivery=cls.delivery_callback)
                producer_instance.flush(2) # ensure sending message
            except KafkaException as e:
                logging.exception(e)
            except Exception as e:
                logging.exception(e)
            time.sleep(interval_seconds)
        logging.info("Kafka continuous producer stopped.")

    @classmethod
    def get_stats(cls):
        return {
            "total_messages": cls.total_messages,
            "sent_messages": cls.sent_messages,
        }

"""

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
        
"""