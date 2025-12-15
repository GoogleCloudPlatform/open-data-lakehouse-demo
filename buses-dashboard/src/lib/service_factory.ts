import { ENV } from './env';
import { BigQueryService } from './bq_service';
import { MockBigQueryService, MockPySparkService, MockKafkaService } from './mocks';
import { PySparkService } from './pyspark_service';
import { KafkaService } from './kafka_service';

// Singleton instances for stateful services
let sparkServiceInstance: PySparkService | MockPySparkService | null = null;

export function getBigQueryService() {
  if (ENV.USE_MOCK) {
    return new MockBigQueryService(ENV.BQ_DATASET);
  }
  return new BigQueryService(ENV.BQ_DATASET);
}

export function getSparkService() {
  if (!sparkServiceInstance) {
    if (ENV.USE_MOCK) {
      sparkServiceInstance = new MockPySparkService();
    } else {
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
  }
  return sparkServiceInstance;
}

export const KafkaServiceFactory = {
  startStreaming: async (bootstrapServers: string, topic: string, bqDataset: string) => {
    if (ENV.USE_MOCK) {
      return MockKafkaService.startStreaming();
    }
    return KafkaService.startStreaming(bootstrapServers, topic, bqDataset);
  },
  stopStreaming: () => {
    if (ENV.USE_MOCK) {
      return MockKafkaService.stopStreaming();
    }
    return KafkaService.stopStreaming();
  },
  getStatus: () => {
    if (ENV.USE_MOCK) {
      return MockKafkaService.getStatus();
    }
    return KafkaService.getStatus();
  }
};
