import { NextResponse } from 'next/server';
import { getSparkService } from '@/lib/service_factory';

export async function POST() {
  const sparkService = getSparkService();
  const status = await sparkService.getJobStatus();

  if (status.is_running) {
    return NextResponse.json(status);
  }

  // Start in background (don't await)
  sparkService.startPySpark().catch(err => console.error('Spark start error:', err));

  // Return immediately with updated status (it might still be PENDING/LOADING locally)
  return NextResponse.json({
    status: 'PENDING',
    message: 'Starting spark streaming app...',
    is_running: true
  });
}
