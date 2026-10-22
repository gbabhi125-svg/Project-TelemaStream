"use client";

import { useEffect, useRef, useState } from "react";
import { infraSnapshot, maybeAlert, nextThroughputValue } from "@/lib/demoEngine";
import type { Alert, InfraMetric, ThroughputPoint } from "@/lib/types";

const MAX_POINTS = 120;
const MAX_ALERTS = 30;

// useTelemetry owns the single source of truth for the whole dashboard. It
// tries the real backend WebSocket first; if no URL is configured or the
// connection never opens within the timeout, it falls back to the demo
// engine so the deployed preview is never a blank page. Either way,
// consumers only ever see the same TelemetrySnapshot shape.
export function useTelemetry() {
  const [mode, setMode] = useState<"live" | "demo" | "connecting">("connecting");
  const [throughput, setThroughput] = useState<ThroughputPoint[]>([]);
  const [alerts, setAlerts] = useState<Alert[]>([]);
  const [infra, setInfra] = useState<InfraMetric[]>([]);
  const latestThroughputRef = useRef(4200);

  useEffect(() => {
    const wsUrl = process.env.NEXT_PUBLIC_WS_URL;
    let socket: WebSocket | null = null;
    let demoTimer: ReturnType<typeof setInterval> | null = null;
    let fallbackTimer: ReturnType<typeof setTimeout> | null = null;
    let cancelled = false;

    function startDemo() {
      if (cancelled || demoTimer) return;
      setMode("demo");
      demoTimer = setInterval(() => {
        const value = nextThroughputValue();
        latestThroughputRef.current = value;
        setThroughput((prev) => [...prev.slice(-MAX_POINTS + 1), { t: Date.now(), messagesPerSecond: value }]);
        setInfra(infraSnapshot(value));

        const alert = maybeAlert();
        if (alert) {
          setAlerts((prev) => [alert, ...prev].slice(0, MAX_ALERTS));
        }
      }, 250);
    }

    if (wsUrl) {
      try {
        socket = new WebSocket(wsUrl);
        fallbackTimer = setTimeout(() => {
          if (socket && socket.readyState !== WebSocket.OPEN) {
            socket.close();
            startDemo();
          }
        }, 3000);

        socket.onopen = () => {
          if (fallbackTimer) clearTimeout(fallbackTimer);
          setMode("live");
        };

        socket.onmessage = (event) => {
          try {
            const { type, payload } = JSON.parse(event.data);
            if (type === "throughput") {
              setThroughput((prev) => [
                ...prev.slice(-MAX_POINTS + 1),
                { t: Date.now(), messagesPerSecond: payload.messagesPerSecond },
              ]);
            } else if (type === "alert") {
              setAlerts((prev) => [payload, ...prev].slice(0, MAX_ALERTS));
            } else if (type === "infra") {
              setInfra(payload);
            }
          } catch {
            // ignore malformed frames
          }
        };

        socket.onerror = () => startDemo();
        socket.onclose = () => {
          if (!cancelled) startDemo();
        };
      } catch {
        startDemo();
      }
    } else {
      startDemo();
    }

    return () => {
      cancelled = true;
      if (fallbackTimer) clearTimeout(fallbackTimer);
      if (demoTimer) clearInterval(demoTimer);
      socket?.close();
    };
  }, []);

  return { mode, throughput, alerts, infra };
}
