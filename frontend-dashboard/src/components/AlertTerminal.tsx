import type { Alert } from "@/lib/types";

const severityColor: Record<string, string> = {
  info: "text-accent",
  warning: "text-warn",
  critical: "text-danger",
};

function timeAgo(iso?: string) {
  if (!iso) return "now";
  const diff = Date.now() - new Date(iso).getTime();
  const seconds = Math.max(0, Math.round(diff / 1000));
  if (seconds < 60) return `${seconds}s ago`;
  return `${Math.round(seconds / 60)}m ago`;
}

export function AlertTerminal({ alerts }: { alerts: Alert[] }) {
  return (
    <div className="flex h-full flex-col">
      <p className="text-xs uppercase tracking-wider text-muted">Alert Feed</p>
      <p className="mt-1 text-sm text-zinc-400">Threshold violations from the stream processor</p>
      <div className="scrollbar-thin mt-4 flex-1 space-y-2 overflow-y-auto font-mono text-xs">
        {alerts.length === 0 && <p className="text-muted">No active alerts. All sensors nominal.</p>}
        {alerts.map((alert, i) => (
          <div key={i} className="rounded-lg border border-border bg-surfaceAlt/60 px-3 py-2">
            <div className="flex items-center justify-between">
              <span className={`font-semibold ${severityColor[alert.severity ?? "warning"]}`}>
                {alert.sensor_id}
              </span>
              <span className="text-muted">{timeAgo(alert.time)}</span>
            </div>
            <p className="mt-1 text-zinc-400">{alert.message}</p>
          </div>
        ))}
      </div>
    </div>
  );
}
