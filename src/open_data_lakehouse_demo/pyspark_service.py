import logging

from google.api_core import exceptions
from google.cloud import dataproc_v1 as dataproc

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
        self.batch_id = "pyspark-streaming-job"
        self.bq_service = BigQueryService(bigquery_dataset)


    
    def start_pyspark(self):
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
            c_operation = self.client.create_batch(request={
                "parent": f"projects/{self.project_id}/locations/{self.region}",
                "batch": batch,
                "batch_id": self.batch_id
            })
        except exceptions.AlreadyExists:
            return {"status": "ALREADY_EXISTS", "message": "Job already exists."}
        except exceptions.PermissionDenied:
            return {"status": "PERMISSION_DENIED", "message": "Permission denied."}
        except exceptions.ResourceExhausted:
            return {"status": "RESOURCE_EXHAUSTED", "message": "Resource exhausted."}
        except exceptions.BadRequest:
            return {"status": "BAD_REQUEST", "message": "Bad request."}
        except exceptions.InternalServerError:
            return {"status": "INTERNAL_SERVER_ERROR", "message": "Internal server error."}
        except Exception as e:
            return {"status": "ERROR", "message": str(e)}
        assert c_operation is not None
        return {"status": "SUBMITTED", "message": "Job submitted."}

    def get_stats(self):
        return self.bq_service.get_bus_state(self.bigquery_table)

    def cancel_job(self):
        try:
            get_operation = self.client.get_batch(request={
                "name": f"projects/{self.project_id}/locations/{self.region}/batches/{self.batch_id}"}
            )
        except exceptions.NotFound:
            return {"status": "NOT_FOUND", "message": "Batch Job not found or not started."}
        except Exception as e:
            logging.exception(e)
            return {"status": "ERROR", "message": str(e)}
        
        try:
            self.client.cancel_operation(request={"name": get_operation.operation}
        except Exception as e:
            return {"status": "ERROR", "message": str(e)}
        
        return {"status": "CANCELLED", "message": "Job cancelled."}

    def get_job_status(self):
        try:
            operation = self.client.get_batch(request={
                "name": f"projects/{self.project_id}/locations/{self.region}/batches/{self.batch_id}"}
            )
        except exceptions.NotFound:
            return {"status": "NOT_FOUND", "message": "Job not found or not started."}
        except Exception as e:
            logging.exception(e)
            return {"status": "ERROR", "message": str(e)}
        match operation.state:
            case dataproc.Batch.State.STATE_UNSPECIFIED:
                return {"status": "STATE_UNSPECIFIED", "message": "Unknown state - check job manually"}
            case dataproc.Batch.State.FAILED:
                return {"status": "FAILED", "message": f"Job failed. Error: {operation.state_message}"}
            case dataproc.Batch.State.CANCELLED:
                return {"status": "CANCELLED", "message": "Job cancelled."}
            case dataproc.Batch.State.PENDING:
                return {"status": "PENDING", "message": "Job is pending."}
            case dataproc.Batch.State.RUNNING:
                return {"status": "RUNNING", "message": "Job is running."}
            case dataproc.Batch.State.SUCCEEDED:
                return {"status": "SUCCEEDED", "message": "Job succeeded."}

