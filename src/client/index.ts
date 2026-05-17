import amqp from "amqplib";
import { GameState } from "../internal/gamelogic/gamestate.js";
import {
  clientWelcome,
  commandStatus,
  getInput,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { ExchangePerilDirect, PauseKey } from "../internal/routing/routing.js";
import { subscribeJSON, SimpleQueueType } from "../internal/pubsub/subscribeJSON.js";
import { handlerPause } from "./handlers.js";

async function main() {
  console.log("Starting Peril client...");

  const username = await clientWelcome();
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ successfully.");

  const gs = new GameState(username);

  const queueName = `pause.${username}`;
  await subscribeJSON(
    conn,
    ExchangePerilDirect,
    queueName,
    PauseKey,
    SimpleQueueType.Transient,
    handlerPause(gs),
  );
  console.log(`Declared and bound queue ${queueName} to ${ExchangePerilDirect} with routing key ${PauseKey}.`);

  let shuttingDown = false;
  const shutdown = async (signal: string) => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    console.log(`Received ${signal}, shutting down...`);
    try {
      await conn.close();
    } catch (err) {
      console.error("Error closing RabbitMQ connection:", err);
    }
    process.exit(0);
  };

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));

  while (true) {
    const words = await getInput("Client> ");
    if (words.length === 0) {
      continue;
    }

    const firstWord = words[0];
    if (!firstWord) {
      continue;
    }
    const command = firstWord.toLowerCase();
    try {
      if (command === "spawn") {
        commandSpawn(gs, words);
      } else if (command === "move") {
        commandMove(gs, words);
        console.log("Move command completed.");
      } else if (command === "status") {
        await commandStatus(gs);
      } else if (command === "help") {
        printClientHelp();
      } else if (command === "spam") {
        console.log("Spamming not allowed yet!");
      } else if (command === "quit") {
        printQuit();
        break;
      } else {
        console.log(`Unrecognized command: ${command}`);
      }
    } catch (err) {
      if (err instanceof Error) {
        console.error(err.message);
      } else {
        console.error("Error:", err);
      }
    }
  }

  await conn.close();
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
