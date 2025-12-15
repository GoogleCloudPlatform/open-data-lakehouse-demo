
export const ENV = {
  PROJECT_ID: process.env.PROJECT_ID || '',
  REGION: process.env.REGION || 'us-central1',
  BQ_DATASET: process.env.BQ_DATASET || '',
  GCS_MAIN_BUCKET: process.env.GCS_MAIN_BUCKET || '',
  KAFKA_BOOTSTRAP: process.env.KAFKA_BOOTSTRAP || 'localhost:9092',
  KAFKA_TOPIC: process.env.KAFKA_TOPIC || 'bus-updates',
  KAFKA_ALERT_TOPIC: process.env.KAFKA_ALERT_TOPIC || 'capacity-alerts',
  SPARK_TMP_BUCKET: process.env.SPARK_TMP_BUCKET || '',
  SPARK_CHECKPOINT_LOCATION: process.env.SPARK_CHECKPOINT_LOCATION || '',
  BIGQUERY_TABLE: process.env.BIGQUERY_TABLE || 'buses_state',
  SUBNET_URI: process.env.SUBNET_URI || '',
  SERVICE_ACCOUNT: process.env.SERVICE_ACCOUNT || '',
  USE_MOCK: process.env.USE_MOCK === 'true',
};
