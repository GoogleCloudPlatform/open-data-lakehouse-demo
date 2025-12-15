import { BatchControllerClient } from '@google-cloud/dataproc';
import { Storage } from '@google-cloud/storage';
import { BigQueryService } from './bq_service';

export enum PySparkState {
  LOADING = 0,
  PRE_RUN_CLEANUP = 1,
  SUBMITTED = 2,
  PENDING = 3,
  RUNNING = 4,
  CANCELLING = 9,
  STATE_UNSPECIFIED = 10,
  NOT_STARTED = 11,
  ALREADY_EXISTS = 12,
  PERMISSION_DENIED = 13,
  RESOURCE_EXHAUSTED = 14,
  BAD_REQUEST = 15,
  INTERNAL_SERVER_ERROR = 16,
  UNKNOWN_ERROR = 17,
  SUCCEEDED = 20,
  FAILED = 22,
  CANCELLED = 23,
}

export interface JobStatus {
  status: string; // Using string key of enum for JSON compatibility
  message: string;
  is_running: boolean;
}

export class PySparkService {
  private client: BatchControllerClient;
  private storage: Storage;
  private bqService: BigQueryService;
  private _status: JobStatus;

  constructor(
    private projectId: string,
    private region: string,
    private gcsMainBucket: string,
    private kafkaBootstrap: string,
    private kafkaTopic: string,
    private kafkaAlertTopic: string,
    private sparkTmpBucket: string,
    private sparkCheckpointLocation: string,
    private bigqueryDataset: string,
    private bigqueryTable: string,
    private subnetUri: string,
    private serviceAccount: string
  ) {
    this.client = new BatchControllerClient({
      apiEndpoint: `${region}-dataproc.googleapis.com`,
    });
    this.storage = new Storage();
    this.bqService = new BigQueryService(bigqueryDataset);
    this._status = {
      status: 'NOT_STARTED',
      message: 'Job not started.',
      is_running: false,
    };
    // Initialize status check
    this.getJobStatus().then(s => this._status = s);
  }

  get batchId(): string {
    return 'pyspark-streaming-job';
  }

  get fullBatchId(): string {
    return `projects/${this.projectId}/locations/${this.region}/batches/${this.batchId}`;
  }

  get status(): JobStatus {
    return this._status;
  }

  get pysparkMainFile(): string {
    return `gs://${this.gcsMainBucket}/code/pyspark-job.py`;
  }

  async startPySpark(retryCount = 0): Promise<void> {
    this._status = {
      status: 'PRE_RUN_CLEANUP',
      message: 'Cleaning up previous runs.',
      is_running: true,
    };

    await this.clearBusState();
    // await this.clearPreviousCheckpoints(); // Commented out in original

    const batch = {
      pysparkBatch: {
        mainPythonFileUri: this.pysparkMainFile,
        args: [
          `--kafka-brokers=${this.kafkaBootstrap}`,
          `--kafka-input-topic=${this.kafkaTopic}`,
          `--kafka-alert-topic=${this.kafkaAlertTopic}`,
          `--spark-tmp-bucket=${this.sparkTmpBucket}`,
          `--spark-checkpoint-location=${this.sparkCheckpointLocation}`,
          `--bigquery-table=${this.bigqueryDataset}.${this.bigqueryTable}`,
        ],
        fileUris: [`gs://${this.gcsMainBucket}/code/ivySettings.xml`],
      },
      runtimeConfig: {
        version: '2.3',
        properties: {
          'spark.jars.ivySettings': './ivySettings.xml',
          'spark.jars.packages':
            'org.apache.spark:spark-streaming-kafka-0-10_2.13:3.5.1,org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.1,com.google.cloud.hosted.kafka:managed-kafka-auth-login-handler:1.0.5',
        },
      },
      environmentConfig: {
        executionConfig: {
          subnetworkUri: this.subnetUri,
          serviceAccount: this.serviceAccount,
        },
      },
    };

    try {
      this._status = {
        status: 'PENDING',
        message: 'Submitting job.',
        is_running: true,
      };

      await this.client.createBatch({
        parent: `projects/${this.projectId}/locations/${this.region}`,
        batch,
        batchId: this.batchId,
      });

      this._status = {
        status: 'SUBMITTED',
        message: 'Job Submitted',
        is_running: true,
      };
    } catch (error: any) {
      if (error.code === 6) { // ALREADY_EXISTS
        if (retryCount < 3) {
          console.log(`Job already exists. Deleting and retrying (Attempt ${retryCount + 1})`);
          await this.cancelJob();
          return this.startPySpark(retryCount + 1);
        }
        this._status = {
          status: 'ALREADY_EXISTS',
          message: 'Job already exists. Check manually.',
          is_running: false,
        };
      } else {
        console.error('Error submitting job:', error);
        this._status = {
          status: 'UNKNOWN_ERROR',
          message: error.message || 'Unknown error',
          is_running: false,
        };
      }
    }
  }

  async getStats(): Promise<any> {
    return this.bqService.getBusState(this.bigqueryTable);
  }

  async cancelJob(): Promise<JobStatus> {
    try {
      // Check if batch exists
      try {
        await this.client.getBatch({ name: this.fullBatchId });
      } catch (e: any) {
        if (e.code === 5) { // NOT_FOUND
          this._status = { status: 'NOT_STARTED', message: 'Job not started.', is_running: false };
          return this._status;
        }
        throw e;
      }

      console.log('Found existing job. Deleting the batch job');
      this._status = { status: 'CANCELLING', message: 'Cancelling job.', is_running: true };

      await this.client.deleteBatch({ name: this.fullBatchId });

      // Wait a bit?
      await new Promise(resolve => setTimeout(resolve, 5000));

      this._status = { status: 'CANCELLED', message: 'Job cancelled.', is_running: false };
    } catch (error: any) {
      console.error('Error cancelling job:', error);
      this._status = { status: 'UNKNOWN_ERROR', message: error.message, is_running: false };
    }
    return this._status;
  }

  async getJobStatus(): Promise<JobStatus> {
    try {
      const [batch] = await this.client.getBatch({ name: this.fullBatchId });

      let statusStr = 'STATE_UNSPECIFIED';
      let isRunning = false;
      let message = 'Unknown state';

      // Map Dataproc state to our status
      // Note: We need to check the actual enum values from the proto or library
      // The library returns string or number.
      const state = batch.state;

      switch (state) {
        case 'FAILED':
          statusStr = 'FAILED';
          message = `Job failed. Error: ${batch.stateMessage}`;
          break;
        case 'CANCELLED':
          statusStr = 'CANCELLED';
          message = 'Job not running';
          break;
        case 'PENDING':
          statusStr = 'PENDING';
          message = 'Job is pending';
          isRunning = true;
          break;
        case 'RUNNING':
          statusStr = 'RUNNING';
          message = 'Job is running';
          isRunning = true;
          break;
        case 'SUCCEEDED':
          statusStr = 'SUCCEEDED';
          message = 'Job succeeded';
          break;
        default:
          statusStr = 'STATE_UNSPECIFIED';
          message = `Unknown state (${state}) - check job manually`;
      }

      this._status = {
        status: statusStr,
        message,
        is_running: isRunning,
      };
    } catch (error: any) {
      if (error.code === 5) { // NOT_FOUND
        this._status = { status: 'NOT_STARTED', message: 'Job not started.', is_running: false };
      } else {
        console.error('Error getting job status:', error);
        this._status = { status: 'UNKNOWN_ERROR', message: error.message, is_running: false };
      }
    }
    return this._status;
  }

  async clearBusState(): Promise<void> {
    await this.bqService.clearTable(this.bigqueryTable);
  }
}
