import type { FastifyInstance } from "fastify";
import { getRecentAlerts, getRecentWindows, getSensorHealth, getThroughputSeries } from "../db.js";

export async function registerApiRoutes(app: FastifyInstance) {
  app.get("/api/windows", async (req) => {
    const limit = Number((req.query as Record<string, string>).limit ?? 200);
    return getRecentWindows(limit);
  });

  app.get("/api/alerts", async (req) => {
    const limit = Number((req.query as Record<string, string>).limit ?? 50);
    return getRecentAlerts(limit);
  });

  app.get("/api/throughput-history", async (req) => {
    const minutes = Number((req.query as Record<string, string>).minutes ?? 10);
    return getThroughputSeries(minutes);
  });

  app.get("/api/health", async () => {
    const health = await getSensorHealth();
    return { status: "ok", ...health };
  });
}
