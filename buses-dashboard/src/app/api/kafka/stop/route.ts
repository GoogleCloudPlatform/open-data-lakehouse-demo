import { NextResponse } from 'next/server';
import { KafkaServiceFactory } from '@/lib/service_factory';

export async function POST() {
  KafkaServiceFactory.stopStreaming();
  return NextResponse.json({ message: 'Kafka producer stopped.' });
}
