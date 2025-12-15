'use client';

import React, { useState } from 'react';
import useSWR from 'swr';
import { Play, Square, Activity, Bus, AlertCircle, CheckCircle2, Clock } from 'lucide-react';
import { clsx } from 'clsx';
import { twMerge } from 'tailwind-merge';

// Utility for tailwind class merging
function cn(...inputs: (string | undefined | null | false)[]) {
  return twMerge(clsx(inputs));
}

const fetcher = (url: string) => fetch(url).then((res) => res.json());

export default function Dashboard() {
  const { data: sparkStatus, mutate: mutateSpark } = useSWR('/api/spark/status', fetcher, { refreshInterval: 3000 });
  const { data: kafkaStatus, mutate: mutateKafka } = useSWR('/api/kafka/status', fetcher, { refreshInterval: 3000 });

  // We also need the initial bus lines. 
  // In the original app, it was passed from server. Here we can fetch it or pass it as prop if we use Server Component.
  // Let's fetch it via a new API route or just use a server component wrapper.
  // For simplicity, let's assume we fetch it client side or pass it.
  // I'll create a separate component for the table that fetches its own data or receives it.
  // But wait, the original app passed `bus_lines` in `index()`.
  // I should probably create an API for bus lines too.

  const [isSparkLoading, setIsSparkLoading] = useState(false);
  const [isKafkaLoading, setIsKafkaLoading] = useState(false);

  const startSpark = async () => {
    setIsSparkLoading(true);
    try {
      await fetch('/api/spark/start', { method: 'POST' });
      mutateSpark();
    } finally {
      setIsSparkLoading(false);
    }
  };

  const stopSpark = async () => {
    setIsSparkLoading(true);
    try {
      await fetch('/api/spark/stop', { method: 'POST' });
      mutateSpark();
    } finally {
      setIsSparkLoading(false);
    }
  };

  const startKafka = async () => {
    setIsKafkaLoading(true);
    try {
      await fetch('/api/kafka/start', { method: 'POST' });
      mutateKafka();
    } finally {
      setIsKafkaLoading(false);
    }
  };

  const stopKafka = async () => {
    setIsKafkaLoading(true);
    try {
      await fetch('/api/kafka/stop', { method: 'POST' });
      mutateKafka();
    } finally {
      setIsKafkaLoading(false);
    }
  };

  // Bus lines data
  const { data: busLines } = useSWR('/api/bus-lines', fetcher);

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 text-gray-900 dark:text-gray-100 font-sans">
      <main className="container mx-auto px-4 py-8 max-w-7xl">
        {/* Header Section */}
        <header className="mb-12 text-center space-y-4">
          <h1 className="text-5xl font-extrabold tracking-tight text-transparent bg-clip-text bg-gradient-to-r from-blue-600 to-indigo-600 dark:from-blue-400 dark:to-indigo-400">
            Real-Time Bus Capacity
          </h1>
          <p className="text-xl text-gray-600 dark:text-gray-400 max-w-3xl mx-auto leading-relaxed">
            Monitor city bus lines in real-time. Simulate passenger flow and track capacity alerts with our advanced data lakehouse solution.
          </p>
        </header>

        {/* Control Panel Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 gap-8 mb-12">
          {/* Spark Control Card */}
          <div className="bg-white dark:bg-gray-800 rounded-2xl shadow-xl p-6 border border-gray-100 dark:border-gray-700 transition-all hover:shadow-2xl">
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-3">
                <div className="p-3 bg-blue-100 dark:bg-blue-900/30 rounded-xl">
                  <Activity className="w-6 h-6 text-blue-600 dark:text-blue-400" />
                </div>
                <h2 className="text-2xl font-bold">Spark Processing</h2>
              </div>
              <StatusBadge status={sparkStatus?.status} />
            </div>

            <p className="text-gray-600 dark:text-gray-400 mb-6 h-12">
              {sparkStatus?.message || 'Initializing...'}
            </p>

            <div className="flex gap-3">
              <button
                onClick={startSpark}
                disabled={isSparkLoading || sparkStatus?.is_running}
                className="flex-1 flex items-center justify-center gap-2 bg-blue-600 hover:bg-blue-700 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-3 px-6 rounded-xl transition-colors shadow-lg shadow-blue-600/20"
              >
                <Play className="w-5 h-5" /> Start
              </button>
              <button
                onClick={stopSpark}
                disabled={isSparkLoading || !sparkStatus?.is_running}
                className="flex-1 flex items-center justify-center gap-2 bg-red-500 hover:bg-red-600 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-3 px-6 rounded-xl transition-colors shadow-lg shadow-red-500/20"
              >
                <Square className="w-5 h-5 fill-current" /> Stop
              </button>
            </div>
          </div>

          {/* Kafka Control Card */}
          <div className="bg-white dark:bg-gray-800 rounded-2xl shadow-xl p-6 border border-gray-100 dark:border-gray-700 transition-all hover:shadow-2xl">
            <div className="flex items-center justify-between mb-6">
              <div className="flex items-center gap-3">
                <div className="p-3 bg-purple-100 dark:bg-purple-900/30 rounded-xl">
                  <Bus className="w-6 h-6 text-purple-600 dark:text-purple-400" />
                </div>
                <h2 className="text-2xl font-bold">Kafka Simulation</h2>
              </div>
              <StatusBadge status={kafkaStatus?.status?.toUpperCase()} />
            </div>

            <p className="text-gray-600 dark:text-gray-400 mb-6 h-12">
              {kafkaStatus?.message || 'Initializing...'}
              {kafkaStatus?.status === 'active' && (
                <span className="block text-sm mt-1 opacity-75">
                  Sent {kafkaStatus.stats.sent_messages} / {kafkaStatus.stats.total_messages} messages
                </span>
              )}
            </p>

            <div className="flex gap-3">
              <button
                onClick={startKafka}
                disabled={isKafkaLoading || kafkaStatus?.status === 'active'}
                className="flex-1 flex items-center justify-center gap-2 bg-purple-600 hover:bg-purple-700 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-3 px-6 rounded-xl transition-colors shadow-lg shadow-purple-600/20"
              >
                <Play className="w-5 h-5" /> Start
              </button>
              <button
                onClick={stopKafka}
                disabled={isKafkaLoading || kafkaStatus?.status !== 'active'}
                className="flex-1 flex items-center justify-center gap-2 bg-red-500 hover:bg-red-600 disabled:opacity-50 disabled:cursor-not-allowed text-white font-semibold py-3 px-6 rounded-xl transition-colors shadow-lg shadow-red-500/20"
              >
                <Square className="w-5 h-5 fill-current" /> Stop
              </button>
            </div>
          </div>
        </div>

        {/* Active Bus Lines Grid */}
        {busLines && (
          <div className="mb-12">
            <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-6 flex items-center gap-2">
              <Activity className="w-5 h-5 text-green-500" />
              Active Fleet
            </h3>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-6">
              {busLines
                .filter((line: any) => sparkStatus?.stats?.some((s: any) => s.bus_line_id === line.bus_line_id))
                .map((line: any) => {
                  const stats = sparkStatus?.stats?.find((s: any) => s.bus_line_id === line.bus_line_id);
                  return (
                    <BusCard key={line.bus_line_id} line={line} stats={stats} isRunning={sparkStatus?.is_running} />
                  );
                })}
              {busLines.filter((line: any) => sparkStatus?.stats?.some((s: any) => s.bus_line_id === line.bus_line_id)).length === 0 && (
                <div className="col-span-full text-center py-8 text-gray-500 bg-gray-50 dark:bg-gray-800/50 rounded-2xl border border-dashed border-gray-200 dark:border-gray-700">
                  No active buses currently. Start the simulation to see live data.
                </div>
              )}
            </div>
          </div>
        )}

        {/* Inactive Bus Lines Grid */}
        {busLines && (
          <div>
            <h3 className="text-xl font-bold text-gray-900 dark:text-white mb-6 flex items-center gap-2">
              <Bus className="w-5 h-5 text-gray-400" />
              Inactive Fleet
            </h3>
            <div className="grid grid-cols-2 sm:grid-cols-3 md:grid-cols-4 lg:grid-cols-5 xl:grid-cols-6 gap-4">
              {busLines
                .filter((line: any) => !sparkStatus?.stats?.some((s: any) => s.bus_line_id === line.bus_line_id))
                .map((line: any) => (
                  <InactiveBusCard key={line.bus_line_id} line={line} />
                ))}
            </div>
          </div>
        )}

        {!busLines && (
          <div className="text-center py-12 text-gray-500">Loading bus lines...</div>
        )}
      </main>
    </div>
  );
}

function StatusBadge({ status }: { status?: string }) {
  if (!status) return <span className="px-3 py-1 rounded-full bg-gray-100 text-gray-500 text-sm font-medium">Unknown</span>;

  const styles: Record<string, string> = {
    RUNNING: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400',
    ACTIVE: 'bg-green-100 text-green-700 dark:bg-green-900/30 dark:text-green-400',
    PENDING: 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900/30 dark:text-yellow-400',
    SUBMITTED: 'bg-blue-100 text-blue-700 dark:bg-blue-900/30 dark:text-blue-400',
    FAILED: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400',
    ERROR: 'bg-red-100 text-red-700 dark:bg-red-900/30 dark:text-red-400',
    FINISHED: 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300',
  };

  const style = styles[status] || styles.FINISHED;

  return (
    <span className={cn("px-3 py-1 rounded-full text-xs font-bold uppercase tracking-wide", style)}>
      {status}
    </span>
  );
}

function InactiveBusCard({ line }: { line: any }) {
  return (
    <div className="rounded-xl p-4 border border-gray-200 dark:border-gray-700 bg-gray-50 dark:bg-gray-800/50 opacity-75 hover:opacity-100 transition-opacity">
      <div className="flex justify-between items-center mb-2">
        <h3 className="font-bold text-gray-700 dark:text-gray-300">{line.bus_line}</h3>
        <Bus className="w-4 h-4 text-gray-400" />
      </div>
      <div className="text-xs text-gray-500 flex flex-col gap-1">
        <span>ID: {line.bus_line_id}</span>
        <span className="flex items-center gap-1"><Clock className="w-3 h-3" /> {line.frequency_minutes} min</span>
        <span>{line.number_of_stops} stops</span>
      </div>
    </div>
  );
}

function BusCard({ line, stats, isRunning }: { line: any, stats: any, isRunning: boolean }) {
  let cardColorClass = 'bg-gray-100 dark:bg-gray-800 border-gray-200 dark:border-gray-700';
  let statusText = 'Inactive';
  let statusIcon = <Bus className="w-5 h-5 text-gray-400" />;
  let textColor = 'text-gray-500';

  if (isRunning && stats) {
    const { total_passengers, total_capacity, remaining_at_stop } = stats;
    const loadPercentage = total_passengers / total_capacity;
    const isOverfilled = remaining_at_stop > 0;

    if (isOverfilled) {
      cardColorClass = 'bg-red-50 dark:bg-red-900/20 border-red-200 dark:border-red-800';
      statusText = `Overcrowded (+${remaining_at_stop})`;
      statusIcon = <AlertCircle className="w-5 h-5 text-red-500" />;
      textColor = 'text-red-700 dark:text-red-300';
    } else if (loadPercentage > 0.8) {
      cardColorClass = 'bg-yellow-50 dark:bg-yellow-900/20 border-yellow-200 dark:border-yellow-800';
      statusText = 'High Capacity';
      statusIcon = <Activity className="w-5 h-5 text-yellow-500" />;
      textColor = 'text-yellow-700 dark:text-yellow-300';
    } else {
      cardColorClass = 'bg-green-50 dark:bg-green-900/20 border-green-200 dark:border-green-800';
      statusText = 'Running Smoothly';
      statusIcon = <CheckCircle2 className="w-5 h-5 text-green-500" />;
      textColor = 'text-green-700 dark:text-green-300';
    }
  }

  return (
    <div className={cn("rounded-2xl p-6 border shadow-sm transition-all hover:shadow-md", cardColorClass)}>
      <div className="flex justify-between items-start mb-4">
        <div>
          <h3 className="text-xl font-bold text-gray-900 dark:text-white">{line.bus_line}</h3>
          <p className="text-xs text-gray-500">ID: {line.bus_line_id}</p>
        </div>
        {statusIcon}
      </div>

      <div className="space-y-3">
        <div className="flex items-center justify-between text-sm">
          <span className="text-gray-500 dark:text-gray-400">Status</span>
          <span className={cn("font-medium", textColor)}>{statusText}</span>
        </div>

        {isRunning && stats && (
          <div className="space-y-1">
            <div className="flex justify-between text-xs text-gray-500">
              <span>Capacity</span>
              <span>{stats.total_passengers} / {stats.total_capacity}</span>
            </div>
            <div className="w-full bg-gray-200 dark:bg-gray-700 rounded-full h-2 overflow-hidden">
              <div
                className={cn("h-full rounded-full transition-all duration-500",
                  stats.remaining_at_stop > 0 ? "bg-red-500" :
                    (stats.total_passengers / stats.total_capacity > 0.8) ? "bg-yellow-500" : "bg-green-500"
                )}
                style={{ width: `${Math.min((stats.total_passengers / stats.total_capacity) * 100, 100)}%` }}
              />
            </div>
          </div>
        )}

        <div className="pt-4 border-t border-gray-200/50 dark:border-gray-700/50 flex justify-between text-xs text-gray-500">
          <span className="flex items-center gap-1"><Clock className="w-3 h-3" /> {line.frequency_minutes} min freq</span>
          <span>{line.number_of_stops} stops</span>
        </div>
      </div>
    </div>
  );
}
