import { BusLine, BusRideData } from './bq_service';
import { JobStatus } from './pyspark_service';
import { KafkaStats } from './kafka_service';

export class MockBigQueryService {
  constructor(private dataset: string) { }

  async getAllBusLines(): Promise<BusLine[]> {
    return Array.from({ length: 25 }, (_, i) => ({
      bus_line_id: `${i + 1}`,
      bus_line: `Line ${i + 1}`,
      number_of_stops: 10 + Math.floor(Math.random() * 10),
      stops: '[]',
      frequency_minutes: 5 + Math.floor(Math.random() * 25),
    }));
  }

  async getBusState(tableName: string): Promise<any[]> {
    // Simulate random bus states for a subset of buses
    const now = new Date().toISOString();
    const activeBusCount = 15; // Simulate ~15 active buses
    const allBusIds = Array.from({ length: 25 }, (_, i) => `${i + 1}`);

    // Shuffle and pick first 15
    const shuffled = allBusIds.sort(() => 0.5 - Math.random());
    const activeIds = shuffled.slice(0, activeBusCount);

    return activeIds.map(id => ({
      bus_line_id: id,
      total_passengers: Math.floor(Math.random() * 60),
      total_capacity: 50,
      remaining_at_stop: Math.random() > 0.8 ? Math.floor(Math.random() * 10) : 0,
      update_timestamp: now,
    }));
  }

  async getRidesData(): Promise<BusRideData[]> {
    return Array.from({ length: 100 }, (_, i) => ({
      bus_ride_id: `ride_${i}`,
      bus_line_id: `${(i % 3) + 1}`,
      bus_line: `Line ${(i % 3) + 1}`,
      bus_size: 1,
      seating_capacity: 30,
      standing_capacity: 20,
      total_capacity: 50,
      bus_stop_id: `stop_${i}`,
      bus_stop_index: i,
      num_of_bus_stops: 10,
      last_stop: false,
      timestamp_at_stop: new Date().toISOString(),
      passengers_in_stop: 5,
      passengers_alighting: 2,
      passengers_boarding: 3,
      remaining_capacity: 20,
      remaining_at_stop: 0,
      total_passengers: 30,
    }));
  }

  async clearTable(tableName: string): Promise<void> {
    console.log(`[Mock] Clearing table ${tableName}`);
  }
}

export class MockPySparkService {
  private _status: JobStatus = {
    status: 'NOT_STARTED',
    message: 'Job not started.',
    is_running: false,
  };

  async startPySpark(): Promise<void> {
    console.log('[Mock] Starting PySpark job...');
    this._status = { status: 'PENDING', message: 'Starting...', is_running: true };
    setTimeout(() => {
      this._status = { status: 'RUNNING', message: 'Job is running', is_running: true };
    }, 2000);
  }

  async getStats(): Promise<any> {
    return new MockBigQueryService('').getBusState('');
  }

  async cancelJob(): Promise<JobStatus> {
    console.log('[Mock] Cancelling PySpark job...');
    this._status = { status: 'CANCELLED', message: 'Job cancelled.', is_running: false };
    return this._status;
  }

  async getJobStatus(): Promise<JobStatus> {
    return this._status;
  }
}

export class MockKafkaService {
  private static isRunning = false;
  private static sentMessages = 0;
  private static totalMessages = 100;

  static async startStreaming() {
    if (this.isRunning) return;
    this.isRunning = true;
    this.sentMessages = 0;
    console.log('[Mock] Starting Kafka streaming...');

    const interval = setInterval(() => {
      if (!this.isRunning || this.sentMessages >= this.totalMessages) {
        clearInterval(interval);
        this.isRunning = false;
        return;
      }
      this.sentMessages += 5;
    }, 1000);
  }

  static stopStreaming() {
    console.log('[Mock] Stopping Kafka streaming...');
    this.isRunning = false;
  }

  static getStatus() {
    if (this.isRunning) {
      return {
        status: 'active',
        message: 'Kafka producer job is running.',
        stats: { total_messages: this.totalMessages, sent_messages: this.sentMessages },
      };
    }
    return {
      status: 'inactive',
      message: 'No kafka producer has been submitted.',
      stats: { total_messages: this.totalMessages, sent_messages: this.sentMessages },
    };
  }
}
