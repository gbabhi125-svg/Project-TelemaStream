"use client";

import { BentoGrid, BentoTile } from "@/components/BentoGrid";
import { LiveChartCard } from "@/components/LiveChartCard";
import { AlertTerminal } from "@/components/AlertTerminal";
import { InfraHealthGrid } from "@/components/InfraHealthGrid";
import { useTelemetry } from "@/hooks/useTelemetry";

const modeLabel: Record<string, string> = {
  live: "LIVE",
  demo: "DEMO DATA",
  connecting: "CONNECTING",
};

const modeColor: Record<string, string> = {
  live: "text-accent border-accent/40 bg-accent/10",
  demo: "text-warn border-warn/40 bg-warn/10",
  connecting: "text-muted border-border bg-surfaceAlt",
};

export default function DashboardPage() {
  const { mode, throughput, alerts, infra } = useTelemetry();

  return (
    <main className="min-h-screen px-6 py-8 lg:px-10">
      <header className="mb-8 flex flex-wrap items-center justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold tracking-tight text-zinc-50">TelemaStream</h1>
          <p className="text-sm text-muted">High-throughput IoT telemetry pipeline — live analytics</p>
        </div>
        <span className={`rounded-full border px-3 py-1 text-xs font-mono tracking-wide ${modeColor[mode]}`}>
          ● {modeLabel[mode]}
        </span>
      </header>

      <BentoGrid>
        <BentoTile className="md:col-span-2 lg:col-span-3 md:row-span-2">
          <LiveChartCard points={throughput} />
        </BentoTile>

        <BentoTile className="lg:row-span-2">
          <AlertTerminal alerts={alerts} />
        </BentoTile>

        <BentoTile className="md:col-span-2 lg:col-span-3">
          <p className="mb-3 text-xs uppercase tracking-wider text-muted">Infrastructure Health</p>
          <InfraHealthGrid metrics={infra} />
        </BentoTile>

        <BentoTile>
          <p className="text-xs uppercase tracking-wider text-muted">Architecture</p>
          <p className="mt-2 text-sm text-zinc-400 leading-relaxed">
            Go ingestion (backpressure) → Redpanda/Kafka → sliding-window stream processor →
            TimescaleDB (hot) + MinIO/Parquet (cold).
          </p>
        </BentoTile>
      </BentoGrid>

      <footer className="mt-8 text-center text-xs text-muted">
        {mode === "demo" ? "Rendering synthetic data — connect NEXT_PUBLIC_WS_URL to a live backend to go live." : "Connected to live backend."}
      </footer>
    </main>
  );
}
