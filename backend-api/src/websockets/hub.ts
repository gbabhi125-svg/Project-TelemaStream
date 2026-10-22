import type { WebSocket } from "ws";

// Hub tracks every connected dashboard client and fans out server-side
// events (throughput ticks, alerts, infra health) to all of them. Clients
// never push state here — this is a one-way broadcast, which is what lets
// the frontend buffer and flush updates on its own render cadence instead
// of reacting to every individual message.
class Hub {
  private clients = new Set<WebSocket>();

  add(socket: WebSocket) {
    this.clients.add(socket);
    socket.on("close", () => this.clients.delete(socket));
  }

  broadcast(type: string, payload: unknown) {
    const message = JSON.stringify({ type, payload, ts: Date.now() });
    for (const client of this.clients) {
      if (client.readyState === client.OPEN) {
        client.send(message);
      }
    }
  }

  get size() {
    return this.clients.size;
  }
}

export const hub = new Hub();
