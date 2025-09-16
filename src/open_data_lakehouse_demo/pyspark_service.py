from __future__ import annotations

import enum
import logging
import time
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass

from google.api_core import exceptions
from google.cloud import dataproc_v1 as dataproc, storage

from open_data_lakehouse_demo.bq_service import BigQueryService


class PySparkService:
    def __init__(
            self,
            project_id: str,
            region: str,
            gcs_main_bucket: str,
            kafka_bootstrap: str,
            kafka_topic: str,
            kafka_alert_topic: str,
            spark_tmp_bucket: str,
            spark_checkpoint_location: str,
            bigquery_dataset: str,
            bigquery_table: str,
            subnet_uri: str,
    ):
        self.project_id = project_id
        self.region = region
        self.gcs_main_bucket = gcs_main_bucket
        self.kafka_bootstrap = kafka_bootstrap
        self.kafka_topic = kafka_topic
        self.kafka_alert_topic = kafka_alert_topic
        self.spark_tmp_bucket = spark_tmp_bucket
        self.spark_checkpoint_location = spark_checkpoint_location
        self.bigquery_dataset = bigquery_dataset
        self.bigquery_table = bigquery_table
        self.subnet_uri = subnet_uri

        self.client = dataproc.BatchControllerClient(client_options={
            "api_endpoint": f"{region}-dataproc.googleapis.com:443"
        })
        self.bq_service = BigQueryService(bigquery_dataset)
        self.storage_client = storage.Client()
        self.storage_bucket = self.storage_client.get_bucket(self.spark_tmp_bucket)
        self.__status__ = self.get_job_status()

    @property
    def batch_id(self) -> str:
        return "pyspark-streaming-job"

    @property
    def full_batch_id(self) -> str:
        return f"projects/{self.project_id}/locations/{self.region}/batches/{self.batch_id}"

    @property
    def status(self):
        return self.__status__
    
    def start_pyspark(self, stop_event, retry_count: int = 0):
        self.__status__ = JobStatus(status=PySparkState.PRE_RUN_CLEANUP, message="Cleaning up previous runs.")
        self.clear_bus_state()
        # self.clear_previous_checkpoints()
        batch = dataproc.Batch(
            pyspark_batch=dataproc.PySparkBatch(
                main_python_file_uri=f"gs://{self.gcs_main_bucket}/notebooks_and_code/pyspark-job.py",
                args=[
                    f"--kafka-brokers={self.kafka_bootstrap}",
                    f"--kafka-input-topic={self.kafka_topic}",
                    f"--kafka-alert-topic={self.kafka_alert_topic}",
                    f"--spark-tmp-bucket={self.spark_tmp_bucket}",
                    f"--spark-checkpoint-location={self.spark_checkpoint_location}",
                    f"--bigquery-table={self.bigquery_dataset}.{self.bigquery_table}",
                ],
                file_uris=[f"gs://{self.gcs_main_bucket}/notebooks_and_code/ivySettings.xml"]
            ),
            runtime_config=dataproc.RuntimeConfig(
                version="2.3",
                properties={
                    "spark.jars.ivySettings": "./ivySettings.xml",
                    "spark.jars.packages":
                        "org.apache.spark:spark-streaming-kafka-0-10_2.13:3.5.1,"
                        "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,"
                        "com.google.cloud.hosted.kafka:managed-kafka-auth-login-handler:1.0.5",
                },
            ),
            environment_config=dataproc.EnvironmentConfig(
                execution_config=dataproc.ExecutionConfig(
                    subnetwork_uri=self.subnet_uri
                )
            ),
        )
        try:
            self.__status__ = JobStatus(status=PySparkState.PENDING, message="Submitting job.")
            create_batch_operation = self.client.create_batch(request={
                "parent": f"projects/{self.project_id}/locations/{self.region}",
                "batch": batch,
                "batch_id": self.batch_id
            })
        except exceptions.AlreadyExists:
            if retry_count < 3:
                retry_count += 1
                logging.info(f"Job already exists. Deleting and retrying (Attempt {retry_count})")
                self.cancel_job()
                return self.start_pyspark(stop_event, retry_count)
            self.__status__ = JobStatus(status=PySparkState.ALREADY_EXISTS, message="Job already exists. Check manually.")
        except exceptions.PermissionDenied:
            self.__status__ = JobStatus(PySparkState.PERMISSION_DENIED, message="Permission Denied.")
        except exceptions.ResourceExhausted:
            self.__status__ = JobStatus(PySparkState.RESOURCE_EXHAUSTED, message="Resource exhausted.")
        except exceptions.BadRequest:
            self.__status__ = JobStatus(status=PySparkState.BAD_REQUEST, message="Bad request.")
        except exceptions.InternalServerError:
            self.__status__ = JobStatus(
                status=PySparkState.INTERNAL_SERVER_ERROR,
                message="Internal server error from Dataproc API."
            )
        except Exception as e:
            self.__status__ = JobStatus(status=PySparkState.UNKNOWN_ERROR, message=str(e))
        self.__status__ = JobStatus(status=PySparkState.SUBMITTED, message="Job Submitted")
        while not stop_event.is_set() and self.__status__.is_running:
            time.sleep(1)
            self.__status__ = self.get_job_status()
            if not self.__status__.is_running:
                logging.warning("Job stopped running")

    def get_stats(self):
        return self.bq_service.get_bus_state(self.bigquery_table)

    def cancel_job(self):
        try:
            get_batch_operation = self.client.get_batch(request={
                "name": self.full_batch_id,
            })
        except exceptions.NotFound:
            logging.info("Batch Job not found or not started.")
            self.status = JobStatus(status=PySparkState.NOT_FOUND, message="Job not found.")
            return
        except Exception as e:
            logging.exception(e)
            self.__status__ = JobStatus(status=PySparkState.UNKNOWN_ERROR, message=str(e))
            return
        logging.info("Found existing job. Getting the operation attached")
        try:
            batch_operation = self.client.get_operation(request={"name": get_batch_operation.operation})
        except Exception as e:
            logging.exception(e)
            self.__status__ = JobStatus(status=PySparkState.UNKNOWN_ERROR, message=str(e))
            return
        logging.info("Found existing operation. Canceling the job")
        try:
            logging.info("Cancelling existing operation")
            self.__status__ = JobStatus(status=PySparkState.CANCELLING, message="Cancelling job.")
            self.client.cancel_operation(request={"name": batch_operation.name})
        except Exception as e:
            logging.exception(e)
            self.__status__ = JobStatus(status=PySparkState.UNKNOWN_ERROR, message=str(e))
            return
        logging.info("Cancelled existing operation. Deleting the batch job")
        try:
            logging.info("Waiting for 5 seconds, until cancel state is propagated.")
            time.sleep(5)
            self.client.delete_batch(request={"name": self.full_batch_id})
        except Exception as e:
            logging.exception(e)
            self.__status__ = JobStatus(status=PySparkState.UNKNOWN_ERROR, message=str(e))
            return
        self.__status__ = JobStatus(status=PySparkState.CANCELLED, message="Job cancelled.")

    def get_job_status(self) -> JobStatus:
        try:
            operation = self.client.get_batch(request={
                "name": self.full_batch_id
            })
        except exceptions.NotFound:
            return JobStatus(status=PySparkState.NOT_FOUND, message="Job not found.")
        except Exception as e:
            logging.exception(e)
            return JobStatus(status=PySparkState.UNKNOWN_ERROR, message=str(e))
        match operation.state:
            case dataproc.Batch.State.STATE_UNSPECIFIED:
                return JobStatus(
                    status=PySparkState.STATE_UNSPECIFIED,
                    message="Unknown state - check job manually",
                )
            case dataproc.Batch.State.FAILED:
                return JobStatus(
                    status=PySparkState.FAILED,
                    message=f"Job failed. Error: {operation.state_message}",
                )
            case dataproc.Batch.State.CANCELLED:
                return JobStatus(
                    status=PySparkState.CANCELLED,
                    message="Job not running",
                )
            case dataproc.Batch.State.PENDING:
                return JobStatus(
                    status=PySparkState.PENDING,
                    message="Job is pending",
                )
            case dataproc.Batch.State.RUNNING:
                return JobStatus(
                    status=PySparkState.RUNNING,
                    message="Job is running",
                )
            case dataproc.Batch.State.SUCCEEDED:
                return JobStatus(
                    status=PySparkState.SUCCEEDED,
                    message="Job succeeded",
                )

    def clear_bus_state(self):
        self.bq_service.clear_table(self.bigquery_table)

    def clear_previous_checkpoints(self):
        # List all blobs in the bucket
        blobs = list(self.storage_bucket.list_blobs())

        if not blobs:
            logging.info(f"Bucket '{self.spark_tmp_bucket}' is already empty.")
            return

        # Delete each blob
        logging.info(f"Deleting {len(blobs)} blobs from bucket '{self.spark_tmp_bucket}'.")
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = [executor.submit(self.delete_blob, blob.name) for blob in blobs]
            for future in futures:
                future.result()  # Wait for each deletion to complete (optional, for error handling)

        logging.info(f"All blobs deleted from bucket '{self.spark_tmp_bucket}'.")

    def delete_blob(self, blob_name):
        blob = self.storage_bucket.blob(blob_name)
        self.storage_bucket.delete_blob(blob)
        

class PySparkState(enum.IntEnum):
    LOADING = 0
    PRE_RUN_CLEANUP = 1
    SUBMITTED = 2
    PENDING = 3
    RUNNING = 4

    CANCELLING = 9

    STATE_UNSPECIFIED = 10
    NOT_FOUND = 11
    ALREADY_EXISTS = 12
    PERMISSION_DENIED = 13
    RESOURCE_EXHAUSTED = 14
    BAD_REQUEST = 15
    INTERNAL_SERVER_ERROR = 16
    UNKNOWN_ERROR = 17

    SUCCEEDED = 20
    FAILED = 22
    CANCELLED = 23

@dataclass
class JobStatus:
    status: PySparkState
    message: str

    @property
    def is_running(self):
        return self.status.value < 10

    def to_dict(self):
        return {
            "status": str(self.status.name),
            "message": self.message,
            "is_running": self.is_running
        }
    