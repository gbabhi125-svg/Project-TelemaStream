import type { InfraMetric } from "@/lib/types";

const statusDot: Record<InfraMetric["status"], string> = {
  ok: "bg-accent",
  warn: "bg-warn",
  critical: "bg-danger",
};

export function InfraHealthGrid({ metrics }: { metrics: InfraMetric[] }) {
  return (
    <div className="grid h-full grid-cols-2 gap-3">
      {metrics.map((m) => (
        <div key={m.label} className="rounded-xl border border-border bg-surfaceAlt/50 p-3">
          <div className="flex items-center gap-2">
            <span className={`h-1.5 w-1.5 rounded-full ${statusDot[m.status]}`} />
            <p className="text-[11px] uppercase tracking-wide text-muted">{m.label}</p>
          </div>
          <p className="mt-1 font-mono text-lg font-semibold text-zinc-50">{m.value}</p>
          <p className="text-[11px] text-muted">{m.hint}</p>
        </div>
      ))}
    </div>
  );
}
