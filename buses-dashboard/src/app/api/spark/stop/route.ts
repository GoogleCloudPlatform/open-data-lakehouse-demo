import { NextResponse } from 'next/server';
import { getSparkService } from '@/lib/service_factory';

export async function POST() {
  const sparkService = getSparkService();
  const status = await sparkService.cancelJob();
  return NextResponse.json(status);
}
