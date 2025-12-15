import { NextResponse } from 'next/server';
import { KafkaServiceFactory } from '@/lib/service_factory';

export async function GET() {
  const status = KafkaServiceFactory.getStatus();
  return NextResponse.json(status);
}
