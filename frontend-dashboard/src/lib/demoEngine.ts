import type { Alert, InfraMetric } from "./types";

// DemoEngine stands in for the live backend when no WebSocket URL is
// reachable — e.g. the dashboard deployed standalone for a preview, with
// no Kafka/Timescale cluster behind it. It reproduces the same event shape
// the real backend emits so every component is identical in both modes.
const SENSOR_IDS = Array.from({ length: 12 }, (_, i) => {
  const types = ["soil_moisture", "soil_temperature", "fleet_speed", "fleet_fuel_level"];
  const type = types[i % types.length];
  return `${type}-${String(i).padStart(4, "0")}`;
});

let throughputBaseline = 4200;
let spikeTicksRemaining = 0;

export function nextThroughputValue(): number {
  if (spikeTicksRemaining > 0) {
    spikeTicksRemaining -= 1;
  } else if (Math.random() < 0.02) {
    spikeTicksRemaining = 15 + Math.floor(Math.random() * 20);
  }

  const target = spikeTicksRemaining > 0 ? throughputBaseline * 2.6 : throughputBaseline;
  const noise = (Math.random() - 0.5) * 300;
  throughputBaseline += (target - throughputBaseline) * 0.08;
  return Math.max(0, Math.round(throughputBaseline + noise));
}

export function maybeAlert(): Alert | null {
  if (Math.random() > 0.06) return null;
  const sensorId = SENSOR_IDS.filter((s) => s.startsWith("soil_moisture"))[
    Math.floor(Math.random() * 3)
  ];
  const avg = 8 + Math.random() * 10;
  return {
    sensor_id: sensorId,
    message: `sensor ${sensorId} averaged ${avg.toFixed(1)}% over the last window, below threshold 20.0%`,
    avg,
    threshold: 20,
    severity: "warning",
    time: new Date().toISOString(),
  };
}

export function infraSnapshot(throughput: number): InfraMetric[] {
  const kafkaLag = Math.max(0, Math.round((throughput - 4200) / 20 + Math.random() * 15));
  const bufferUtil = Math.min(97, Math.round(35 + (throughput / 11000) * 60 + Math.random() * 5));
  const dbPoolUsed = Math.min(20, Math.round(4 + (throughput / 11000) * 14));

  return [
    {
      label: "Kafka Consumer Lag",
      value: `${kafkaLag} msgs`,
      status: kafkaLag > 400 ? "critical" : kafkaLag > 150 ? "warn" : "ok",
      hint: "sensor.readings topic",
    },
    {
      label: "Ingestion Buffer",
      value: `${bufferUtil}%`,
      status: bufferUtil > 85 ? "critical" : bufferUtil > 65 ? "warn" : "ok",
      hint: "ring buffer utilization",
    },
    {
      label: "DB Pool",
      value: `${dbPoolUsed}/20`,
      status: dbPoolUsed >= 18 ? "critical" : dbPoolUsed >= 14 ? "warn" : "ok",
      hint: "TimescaleDB connections",
    },
    {
      label: "Active Sensors",
      value: `${SENSOR_IDS.length.toLocaleString()}`,
      status: "ok",
      hint: "reporting in last 5 min",
    },
  ];
}
