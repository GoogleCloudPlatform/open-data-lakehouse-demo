import { NextResponse } from 'next/server';
import { getSparkService } from '@/lib/service_factory';

export async function GET() {
  const sparkService = getSparkService();
  const status = await sparkService.getJobStatus();

  if (status.is_running) {
    const stats = await sparkService.getStats();
    return NextResponse.json({ ...status, stats });
  } else {
    return NextResponse.json(status);
  }
}
