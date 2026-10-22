"use client";

import { useEffect, useRef } from "react";
import type { ThroughputPoint } from "@/lib/types";

// Renders the throughput series directly to a <canvas>. A canvas is used
// instead of a DOM/SVG chart so redraws stay O(points) with no per-point
// reflow — the whole point of the "buffer updates, flush on an interval"
// strategy described in the architecture brief. The effect redraws once
// per incoming batch of points (already throttled upstream), not per frame.
export function LiveChartCard({ points }: { points: ThroughputPoint[] }) {
  const canvasRef = useRef<HTMLCanvasElement>(null);

  useEffect(() => {
    const canvas = canvasRef.current;
    if (!canvas) return;
    const ctx = canvas.getContext("2d");
    if (!ctx) return;

    const dpr = window.devicePixelRatio || 1;
    const width = canvas.clientWidth;
    const height = canvas.clientHeight;
    canvas.width = width * dpr;
    canvas.height = height * dpr;
    ctx.scale(dpr, dpr);
    ctx.clearRect(0, 0, width, height);

    if (points.length < 2) return;

    const values = points.map((p) => p.messagesPerSecond);
    const max = Math.max(...values, 1) * 1.15;
    const min = 0;

    ctx.strokeStyle = "#232733";
    ctx.lineWidth = 1;
    for (let i = 1; i <= 3; i++) {
      const y = (height / 4) * i;
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(width, y);
      ctx.stroke();
    }

    const stepX = width / (points.length - 1);
    const toY = (v: number) => height - ((v - min) / (max - min)) * height;

    const gradient = ctx.createLinearGradient(0, 0, 0, height);
    gradient.addColorStop(0, "rgba(94, 234, 212, 0.25)");
    gradient.addColorStop(1, "rgba(94, 234, 212, 0)");

    ctx.beginPath();
    ctx.moveTo(0, toY(values[0]));
    points.forEach((p, i) => ctx.lineTo(i * stepX, toY(p.messagesPerSecond)));
    ctx.lineTo(width, height);
    ctx.lineTo(0, height);
    ctx.closePath();
    ctx.fillStyle = gradient;
    ctx.fill();

    ctx.beginPath();
    ctx.moveTo(0, toY(values[0]));
    points.forEach((p, i) => ctx.lineTo(i * stepX, toY(p.messagesPerSecond)));
    ctx.strokeStyle = "#5eead4";
    ctx.lineWidth = 2;
    ctx.lineJoin = "round";
    ctx.stroke();
  }, [points]);

  const current = points.at(-1)?.messagesPerSecond ?? 0;
  const peak = Math.max(0, ...points.map((p) => p.messagesPerSecond));

  return (
    <div className="flex h-full flex-col">
      <div className="flex items-start justify-between">
        <div>
          <p className="text-xs uppercase tracking-wider text-muted">System Throughput</p>
          <p className="mt-1 font-mono text-3xl font-semibold text-zinc-50">
            {current.toLocaleString()}
            <span className="ml-1 text-sm text-muted">msg/s</span>
          </p>
        </div>
        <div className="text-right text-xs text-muted">
          <p>peak</p>
          <p className="font-mono text-accent">{peak.toLocaleString()}</p>
        </div>
      </div>
      <div className="mt-4 flex-1">
        <canvas ref={canvasRef} className="h-full w-full" />
      </div>
    </div>
  );
}
