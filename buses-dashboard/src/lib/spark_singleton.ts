import { PySparkService } from './pyspark_service';
import { ENV } from './env';

// Singleton instance
let sparkServiceInstance: PySparkService | null = null;

export function getSparkService(): PySparkService {
  if (!sparkServiceInstance) {
    sparkServiceInstance = new PySparkService(
      ENV.PROJECT_ID,
      ENV.REGION,
      ENV.GCS_MAIN_BUCKET,
      ENV.KAFKA_BOOTSTRAP,
      ENV.KAFKA_TOPIC,
      ENV.KAFKA_ALERT_TOPIC,
      ENV.SPARK_TMP_BUCKET,
      ENV.SPARK_CHECKPOINT_LOCATION,
      ENV.BQ_DATASET,
      ENV.BIGQUERY_TABLE,
      ENV.SUBNET_URI,
      ENV.SERVICE_ACCOUNT
    );
  }
  return sparkServiceInstance;
}
