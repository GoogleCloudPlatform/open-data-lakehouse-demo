import { Kafka, Producer, SASLOptions } from 'kafkajs';
import { BigQueryService, BusRideData } from './bq_service';
import { TokenProvider } from './token_provider';

export interface KafkaStats {
  total_messages: number;
  sent_messages: number;
}

export class KafkaService {
  private static producer: Producer | null = null;
  private static totalMessages = 0;
  private static sentMessages = 0;
  private static isRunning = false;
  private static shouldStop = false;

  static async getProducer(bootstrapServers: string): Promise<Producer> {
    if (this.producer) return this.producer;

    const isLocal = bootstrapServers.startsWith('localhost');
    let sasl: SASLOptions | undefined = undefined;
    let ssl = false;

    if (!isLocal) {
      const tokenProvider = new TokenProvider();
      ssl = true;
      sasl = {
        mechanism: 'oauthbearer',
        oauthBearerProvider: async () => {
          const { token } = await tokenProvider.getGoogleKafkaToken();
          return { value: token };
        },
      };
    }

    const kafka = new Kafka({
      clientId: 'buses-dashboard',
      brokers: bootstrapServers.split(','),
      ssl,
      sasl,
    });

    this.producer = kafka.producer();
    await this.producer.connect();
    return this.producer;
  }

  static async startStreaming(
    bootstrapServers: string,
    topic: string,
    bqDataset: string,
    intervalMs = 1000
  ) {
    if (this.isRunning) {
      console.log('Kafka producer already running.');
      return;
    }

    this.isRunning = true;
    this.shouldStop = false;
    this.sentMessages = 0;

    try {
      const bqService = new BigQueryService(bqDataset);
      const ridesData = await bqService.getRidesData();
      this.totalMessages = ridesData.length;
      console.log(`Got ${this.totalMessages} rides data.`);

      const producer = await this.getProducer(bootstrapServers);

      for (const ride of ridesData) {
        if (this.shouldStop) break;

        this.sentMessages++;
        const message = {
          id: this.sentMessages,
          timestamp: Date.now() / 1000,
          data: ride,
        };

        console.log(`Sending message ${this.sentMessages}`);
        await producer.send({
          topic,
          messages: [{ value: JSON.stringify(message) }],
        });

        await new Promise((resolve) => setTimeout(resolve, intervalMs));
      }
    } catch (error) {
      console.error('Error in Kafka streaming:', error);
    } finally {
      this.isRunning = false;
      console.log('Kafka continuous producer stopped.');
    }
  }

  static stopStreaming() {
    this.shouldStop = true;
  }

  static getStats(): KafkaStats {
    return {
      total_messages: this.totalMessages,
      sent_messages: this.sentMessages,
    };
  }
  
  static getStatus() {
    if (this.isRunning) {
      return {
        status: 'active',
        message: 'Kafka producer job is running.',
        stats: this.getStats(),
      };
    }
    if (this.sentMessages > 0 && this.sentMessages >= this.totalMessages) {
       return {
         status: 'finished',
         message: 'Kafka producer job has completed.',
         stats: this.getStats(),
       };
    }
    return {
      status: 'inactive',
      message: 'No kafka producer has been submitted.',
      stats: this.getStats(),
    };
  }
}
