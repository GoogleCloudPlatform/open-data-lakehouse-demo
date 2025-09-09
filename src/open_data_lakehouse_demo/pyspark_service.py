import logging

import pyspark.sql.connect.functions as f
from pyspark.sql.streaming import StreamingQuery
from pyspark.sql.types import (
    StringType,
    StructType,
    TimestampType,
    IntegerType,
    LongType,
    BooleanType,
    StructField,
)
from pyspark.sql import SparkSession
# from google.cloud.dataproc_spark_connect.session import SparkSession as DataprocSparkSession
from google.cloud.dataproc_spark_connect import DataprocSparkSession
from google.cloud.dataproc_v1 import Session


class PySparkService:
    
    @classmethod
    def start_pyspark(cls, 
                      project_id: str, 
                      region: str, 
                      kafka_brokers: str, 
                      kafka_topic: str):
        
        return {"status": "success", "message": "Spark session started successfully."}
    
    @classmethod
    def get_stats(cls):
        if not cls.spark or cls.spark.is_stopped:
            return {"status": "error", "message": "Spark session not running"}
        try:
            # Use the global Spark session to run a SQL query
            stats_data = cls.spark.sql("SELECT * FROM latest_bus_data").collect()
            
            # Convert Spark Rows to a list of dictionaries for JSON response
            results = [row.asDict() for row in stats_data]
            
            return {"status": "success", "data": results}
        except Exception as e:
            # This can happen if the query is not yet initialized
            logging.error(f"Error querying latest_bus_data: {e}")
            return {"status": "error", "message": f"Could not query stats. Is the stream running? Error: {e}"}
