export type Severity = "info" | "warning" | "critical";

export interface ThroughputPoint {
  t: number;
  messagesPerSecond: number;
}

export interface Alert {
  sensor_id: string;
  message?: string;
  avg?: number;
  threshold?: number;
  severity?: Severity;
  time?: string;
}

export interface InfraMetric {
  label: string;
  value: string;
  status: "ok" | "warn" | "critical";
  hint: string;
}

export interface TelemetrySnapshot {
  connected: boolean;
  mode: "live" | "demo";
  throughput: ThroughputPoint[];
  alerts: Alert[];
  infra: InfraMetric[];
}
