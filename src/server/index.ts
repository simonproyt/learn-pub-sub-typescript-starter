import amqp from "amqplib";
import type { PlayingState } from "../internal/gamelogic/gamestate.js";
import {
  printServerHelp,
  getInput,
} from "../internal/gamelogic/gamelogic.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { publishJSON } from "../internal/pubsub/publish.js";

async function main() {
  console.log("Starting Peril server...");
  printServerHelp();

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ successfully.");

  const ch = await conn.createConfirmChannel();

  const shutdown = async (signal: string) => {
    console.log(`Received ${signal}, shutting down...`);
    await conn.close();
    process.exit(0);
  };

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));

  while (true) {
    const words = await getInput("Server> ");
    if (words.length === 0) {
      continue;
    }

    const command = words[0].toLowerCase();
    if (command === "pause") {
      console.log("Sending pause message...");
      const pauseEvent: PlayingState = { isPaused: true };
      await publishJSON(ch, ExchangePerilDirect, PauseKey, pauseEvent);
      console.log("Pause message sent.");
    } else if (command === "resume") {
      console.log("Sending resume message...");
      const resumeEvent: PlayingState = { isPaused: false };
      await publishJSON(ch, ExchangePerilDirect, PauseKey, resumeEvent);
      console.log("Resume message sent.");
    } else if (command === "quit") {
      console.log("Exiting server...");
      break;
    } else {
      console.log(`Unrecognized command: ${command}`);
    }
  }

  await conn.close();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
