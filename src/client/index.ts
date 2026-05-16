import amqp from "amqplib";
import { clientWelcome } from "../internal/gamelogic/gamelogic.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { declareAndBind, SimpleQueueType } from "../internal/pubsub/declareAndBind.js";

async function main() {
  console.log("Starting Peril client...");

  const username = await clientWelcome();
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ successfully.");

  const queueName = `pause.${username}`;
  await declareAndBind(
    conn,
    ExchangePerilDirect,
    queueName,
    PauseKey,
    SimpleQueueType.Transient,
  );
  console.log(`Declared and bound queue ${queueName} to ${ExchangePerilDirect} with routing key ${PauseKey}.`);

  const shutdown = async (signal: string) => {
    console.log(`Received ${signal}, shutting down...`);
    await conn.close();
    process.exit(0);
  };

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));

  console.log("Client is waiting for shutdown signal...");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
