import datetime

from google.api_core import exceptions
from google.cloud import bigquery
bigquery_client = None

class BigQueryService():
    client = None
    DAYS_TO_QUERY = 10
    def __init__(self, bq_dataset: str):
        self.client = bigquery.Client()
        self.bq_dataset = bq_dataset

    def get_all_bus_lines(self):
        query = f"""
            SELECT
                bus_line_id,
                bus_line,
                number_of_stops,
                stops,
                frequency_minutes
            FROM `{self.bq_dataset}.bus_lines`
        """
        return [x for x in self.client.query(query).result()]
    
    def get_bus_state(self, table_name: str):
        try:
            table_obj = self.client.get_table(f"{self.bq_dataset}.{table_name}")
        except exceptions.NotFound:
            return []
        query = f"SELECT * FROM {self.bq_dataset}.{table_name}"
        return [dict(x) for x in self.client.query(query).result()]
        
    def get_rides_data(self):
        now = datetime.datetime.now(datetime.UTC)
        start_timestamp = (now - datetime.timedelta(days=self.DAYS_TO_QUERY)).replace(year=2024)
        stop_timestamp = now.replace(year=2024)

        query = f"""
        SELECT
            REGEXP_REPLACE(bus_ride_id, r'^(\\d+)_(\\d{4})-(\\d{2})-(\\d{2})_(\\d{2})-(\\d{2})-(\\d{2})$', 
            '\\\\1_2025-\\\\3-\\\\4_\\\\5-\\\\6-\\\\7') AS bus_ride_id,
            bus_line_id,
            bus_line,
            bus_size,
            seating_capacity,
            standing_capacity,
            total_capacity,
            bus_stop_id,
            bus_stop_index,
            num_of_bus_stops,
            last_stop,
            TIMESTAMP_ADD(timestamp_at_stop, INTERVAL {(now-stop_timestamp).days} DAY) AS timestamp_at_stop,
            passengers_in_stop,
            passengers_alighting,
            passengers_boarding,
            remaining_capacity,
            remaining_at_stop,
            total_passengers
        FROM
            {self.bq_dataset}.bus_rides
        WHERE 
            timestamp_at_stop BETWEEN TIMESTAMP('{start_timestamp.strftime("%Y-%m-%dT%H:%M:%S")}')
                AND TIMESTAMP('{stop_timestamp.strftime("%Y-%m-%dT%H:%M:%S")}')
        """
        return [dict(x) for x in self.client.query(query).result()]

    def clear_table(self, bigquery_table):
        query = f"DELETE FROM {self.bq_dataset}.{bigquery_table} WHERE 1=1;"
        self.client.query(query).result()


