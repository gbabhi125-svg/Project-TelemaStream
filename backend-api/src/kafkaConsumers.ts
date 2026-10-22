import { Kafka } from "kafkajs";
import { config } from "./config.js";
import { hub } from "./websockets/hub.js";

const kafka = new Kafka({ clientId: "telemastream-backend", brokers: config.kafkaBrokers });

// A rolling counter of raw readings observed in the last second. The
// dashboard's throughput tile reads this via the "throughput" WS event
// rather than hitting Postgres on every tick, so it stays responsive even
// when the hot-path writer is backed up.
let messagesThisSecond = 0;

export async function startKafkaConsumers() {
  const readingsConsumer = kafka.consumer({ groupId: "backend-api-throughput" });
  await readingsConsumer.connect();
  await readingsConsumer.subscribe({ topic: config.readingsTopic, fromBeginning: false });

  await readingsConsumer.run({
    eachMessage: async () => {
      messagesThisSecond += 1;
    },
  });

  setInterval(() => {
    hub.broadcast("throughput", { messagesPerSecond: messagesThisSecond, connectedClients: hub.size });
    messagesThisSecond = 0;
  }, 1000);

  const alertsConsumer = kafka.consumer({ groupId: "backend-api-alerts" });
  await alertsConsumer.connect();
  await alertsConsumer.subscribe({ topic: config.alertsTopic, fromBeginning: false });

  await alertsConsumer.run({
    eachMessage: async ({ message }) => {
      if (!message.value) return;
      try {
        const alert = JSON.parse(message.value.toString());
        hub.broadcast("alert", alert);
      } catch {
        // malformed alert payload — drop it rather than crash the consumer loop
      }
    },
  });

  console.log("backend-api: kafka consumers running");
}
