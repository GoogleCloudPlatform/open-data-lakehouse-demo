import { NextResponse } from 'next/server';
import { getBigQueryService } from '@/lib/service_factory';

export async function GET() {
  const bqService = getBigQueryService();
  const busLines = await bqService.getAllBusLines();
  return NextResponse.json(busLines);
}
