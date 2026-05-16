import amqp from "amqplib";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { publishJSON } from "../internal/pubsub/publish.js";

async function main() {
  console.log("Starting Peril server...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ successfully.");

  const ch = await conn.createConfirmChannel();

  const pauseEvent: PlayingState = {
    isPaused: true,
  };

  await publishJSON(ch, ExchangePerilDirect, PauseKey, pauseEvent);
  console.log(`Published pause event to exchange ${ExchangePerilDirect} with routing key ${PauseKey}.`);

  const shutdown = async (signal: string) => {
    console.log(`Received ${signal}, shutting down...`);
    await conn.close();
    process.exit(0);
  };

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));

  console.log("Server is waiting for shutdown signal...");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
