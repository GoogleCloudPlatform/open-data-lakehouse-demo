import { BigQuery } from '@google-cloud/bigquery';

export interface BusLine {
  bus_line_id: string;
  bus_line: string;
  number_of_stops: number;
  stops: string; // JSON string or array? In python it seems to be just selected.
  frequency_minutes: number;
}

export interface BusRideData {
  bus_ride_id: string;
  bus_line_id: string;
  bus_line: string;
  bus_size: number;
  seating_capacity: number;
  standing_capacity: number;
  total_capacity: number;
  bus_stop_id: string;
  bus_stop_index: number;
  num_of_bus_stops: number;
  last_stop: boolean;
  timestamp_at_stop: string; // BigQuery Timestamp
  passengers_in_stop: number;
  passengers_alighting: number;
  passengers_boarding: number;
  remaining_capacity: number;
  remaining_at_stop: number;
  total_passengers: number;
}

export class BigQueryService {
  private client: BigQuery;
  private dataset: string;
  private readonly DAYS_TO_QUERY = 10;

  constructor(dataset: string) {
    this.client = new BigQuery();
    this.dataset = dataset;
  }

  async getAllBusLines(): Promise<BusLine[]> {
    const query = `
      SELECT
        bus_line_id,
        bus_line,
        number_of_stops,
        stops,
        frequency_minutes
      FROM \`${this.dataset}.bus_lines\`
    `;
    const [rows] = await this.client.query(query);
    return rows as BusLine[];
  }

  async getBusState(tableName: string): Promise<any[]> {
    try {
      // Check if table exists implicitly by querying or explicit check.
      // Python code did get_table first.
      const [exists] = await this.client.dataset(this.dataset).table(tableName).exists();
      if (!exists) return [];
      
      const query = `SELECT * FROM \`${this.dataset}.${tableName}\``;
      const [rows] = await this.client.query(query);
      return rows;
    } catch (error) {
      console.error('Error getting bus state:', error);
      return [];
    }
  }

  async getRidesData(): Promise<BusRideData[]> {
    const now = new Date();
    const startTimestamp = new Date(now);
    startTimestamp.setDate(now.getDate() - this.DAYS_TO_QUERY);
    startTimestamp.setFullYear(2024);
    
    const stopTimestamp = new Date(now);
    stopTimestamp.setFullYear(2024);

    // Format dates for BigQuery: YYYY-MM-DDTHH:mm:ss
    const formatBQDate = (d: Date) => d.toISOString().slice(0, 19);

    const diffDays = Math.floor((now.getTime() - stopTimestamp.getTime()) / (1000 * 60 * 60 * 24));

    const query = `
      SELECT
        REGEXP_REPLACE(bus_ride_id, r'^(\\d+)_(\\d{4})-(\\d{2})-(\\d{2})_(\\d{2})-(\\d{2})-(\\d{2})$', 
        '\\\\1_2025-\\\\3-\\\\4_\\\\5-\\\\6-\\\\7') AS bus_ride_id,
        bus_line_id,
        bus_line,
        bus_size,
        seating_capacity,
        standing_capacity,
        total_capacity,
        bus_stop_id,
        bus_stop_index,
        num_of_bus_stops,
        last_stop,
        TIMESTAMP_ADD(timestamp_at_stop, INTERVAL ${diffDays} DAY) AS timestamp_at_stop,
        passengers_in_stop,
        passengers_alighting,
        passengers_boarding,
        remaining_capacity,
        remaining_at_stop,
        total_passengers
      FROM
        \`${this.dataset}.bus_rides\`
      WHERE 
        timestamp_at_stop BETWEEN TIMESTAMP('${formatBQDate(startTimestamp)}')
            AND TIMESTAMP('${formatBQDate(stopTimestamp)}')
    `;
    
    const [rows] = await this.client.query(query);
    return rows as BusRideData[];
  }

  async clearTable(tableName: string): Promise<void> {
    try {
      const [exists] = await this.client.dataset(this.dataset).table(tableName).exists();
      if (!exists) {
        console.log(`Drop operation - Table ${this.dataset}.${tableName} not found. Skipping`);
        return;
      }
      const query = `DELETE FROM \`${this.dataset}.${tableName}\` WHERE 1=1;`;
      await this.client.query(query);
    } catch (error) {
      console.error('Error clearing table:', error);
    }
  }
}
