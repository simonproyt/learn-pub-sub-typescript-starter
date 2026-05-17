import amqp from "amqplib";
import { GameState } from "../internal/gamelogic/gamestate.js";
import {
  clientWelcome,
  commandStatus,
  getInput,
  getMaliciousLog,
  printClientHelp,
  printQuit,
} from "../internal/gamelogic/gamelogic.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { ArmyMovesPrefix, ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, WarRecognitionsPrefix } from "../internal/routing/routing.js";
import { publishJSON, publishMsgPack } from "../internal/pubsub/publish.js";
import { subscribeJSON, SimpleQueueType } from "../internal/pubsub/subscribeJSON.js";
import type { GameLog } from "../internal/gamelogic/logs.js";
import { handlerPause, handlerMove, handlerWar } from "./handlers.js";

async function main() {
  console.log("Starting Peril client...");

  const username = await clientWelcome();
  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ successfully.");

  const gs = new GameState(username);

  const pauseQueueName = `pause.${username}`;
  await subscribeJSON(
    conn,
    ExchangePerilDirect,
    pauseQueueName,
    PauseKey,
    SimpleQueueType.Transient,
    handlerPause(gs),
  );
  console.log(`Declared and bound queue ${pauseQueueName} to ${ExchangePerilDirect} with routing key ${PauseKey}.`);

  const publishChannel = await conn.createConfirmChannel();

  const moveQueueName = `${ArmyMovesPrefix}.${username}`;
  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    moveQueueName,
    `${ArmyMovesPrefix}.*`,
    SimpleQueueType.Transient,
    handlerMove(gs, await conn.createConfirmChannel()),
    "topic",
  );
  console.log(`Declared and bound queue ${moveQueueName} to ${ExchangePerilTopic} with routing key ${ArmyMovesPrefix}.*.`);

  const warQueueName = "war";
  await subscribeJSON(
    conn,
    ExchangePerilTopic,
    warQueueName,
    `${WarRecognitionsPrefix}.*`,
    SimpleQueueType.Durable,
    handlerWar(gs, publishGameLog),
    "topic",
  );
  console.log(`Declared and bound queue ${warQueueName} to ${ExchangePerilTopic} with routing key ${WarRecognitionsPrefix}.*.`);

  async function publishGameLog(
    username: string,
    message: string,
  ): Promise<void> {
    const gameLog: GameLog = {
      username,
      message,
      currentTime: new Date(),
    };

    const routingKey = `${GameLogSlug}.${username}`;
    await publishMsgPack(publishChannel, ExchangePerilTopic, routingKey, gameLog);
  }

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
        const move = commandMove(gs, words);
        const routingKey = `${ArmyMovesPrefix}.${username}`;
        await publishJSON(publishChannel, ExchangePerilTopic, routingKey, move);
        console.log(`Published move to ${routingKey}`);
      } else if (command === "status") {
        await commandStatus(gs);
      } else if (command === "help") {
        printClientHelp();
      } else if (command === "spam") {
        if (words.length < 2) {
          console.log("Usage: spam <count>");
          continue;
        }

        const count = Number(words[1]);
        if (!Number.isInteger(count) || count <= 0) {
          console.log("Usage: spam <positive integer>");
          continue;
        }

        const routingKey = `${GameLogSlug}.${username}`;
        for (let i = 0; i < count; i += 1) {
          const message = getMaliciousLog();
          const gameLog: GameLog = {
            username,
            message,
            currentTime: new Date(),
          };
          await publishMsgPack(publishChannel, ExchangePerilTopic, routingKey, gameLog);
        }
        console.log(`Published ${count} malicious logs to ${routingKey}.`);
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
