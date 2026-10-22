import cors from "@fastify/cors";
import websocket from "@fastify/websocket";
import Fastify from "fastify";
import { config } from "./config.js";
import { registerApiRoutes } from "./controllers/api.js";
import { startKafkaConsumers } from "./kafkaConsumers.js";
import { hub } from "./websockets/hub.js";

const app = Fastify({ logger: true });

await app.register(cors, { origin: true });
await app.register(websocket);
await registerApiRoutes(app);

app.register(async (fastify) => {
  fastify.get("/ws", { websocket: true }, (socket) => {
    hub.add(socket);
    socket.send(JSON.stringify({ type: "connected", payload: { ok: true }, ts: Date.now() }));
  });
});

app.get("/healthz", async () => ({ status: "ok" }));

try {
  await startKafkaConsumers();
} catch (err) {
  app.log.error({ err }, "kafka consumers failed to start; retrying in background is left to the orchestrator");
}

await app.listen({ port: config.port, host: "0.0.0.0" });
console.log(`backend-api listening on :${config.port}`);
