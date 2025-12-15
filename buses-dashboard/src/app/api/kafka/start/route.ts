import { NextResponse } from 'next/server';
import { KafkaServiceFactory } from '@/lib/service_factory';
import { ENV } from '@/lib/env';

export async function POST() {
  const status = KafkaServiceFactory.getStatus();
  if (status.status === 'active') {
    return NextResponse.json({ message: 'Producer is already running.' });
  }

  // Start in background
  KafkaServiceFactory.startStreaming(
    ENV.KAFKA_BOOTSTRAP,
    ENV.KAFKA_TOPIC,
    ENV.BQ_DATASET
  ).catch(err => console.error('Kafka start error:', err));

  return NextResponse.json({ message: 'Kafka producer started in the background.' });
}
