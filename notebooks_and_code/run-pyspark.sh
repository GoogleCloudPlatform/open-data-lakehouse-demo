export PROJECT_ID=$(gcloud config get-value project)
export REGION=$(gcloud run services list --format="value(region)" | head -n 1)
export KAFKA_BROKERS=$(gcloud manaaged-kafka clusters list --location us-central1 --format="value(bootstrapAddress)")

gcloud dataproc batches submit pyspark \
  gs://${PROJECT_ID}-ridership-lakehouse/notebooks_and_code/pyspark-job.py \
  --region=$REGION \
  --subnet=projects/${PROJECT_ID}/regions/${REGION}/subnetworks/${REGION}-open-lakehouse-subnet \
  --version=2.3 \
  --properties=^$^'spark.jars.ivySettings=./ivySettings.xml$spark.jars.packages=org.apache.spark:spark-streaming-kafka-0-10_2.13:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,com.google.cloud.hosted.kafka:managed-kafka-auth-login-handler:1.0.5' \
  --files=gs://${PROJECT_ID}-ridership-lakehouse/notebooks_and_code/ivySettings.xml \
  --  --kafka-brokers=$KAFKA_BROKERS \
      --kafka-input-topic=bus-updates \
      --kafka-alert-topic=capacity-alerts \
      --spark-tmp-bucket=${PROJECT_ID}-dataproc-serverless \
      --spark-checkpoint-location=${PROJECT_ID}-dataproc-serverless/checkpoint \
      --bigquery-table=ridership_lakehouse.bus_state
