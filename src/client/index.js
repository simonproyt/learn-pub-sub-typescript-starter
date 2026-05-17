import amqp from "amqplib";
import { GameState } from "../internal/gamelogic/gamestate.js";
import { clientWelcome, commandStatus, getInput, printClientHelp, printQuit, } from "../internal/gamelogic/gamelogic.js";
import { commandSpawn } from "../internal/gamelogic/spawn.js";
import { commandMove } from "../internal/gamelogic/move.js";
import { ExchangePerilDirect, ExchangePerilTopic, PauseKey } from "../internal/routing/routing.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { subscribeJSON, SimpleQueueType } from "../internal/pubsub/subscribeJSON.js";
import { handlerPause, handlerMove } from "./handlers.js";
async function main() {
    console.log("Starting Peril client...");
    const username = await clientWelcome();
    const rabbitConnString = "amqp://guest:guest@localhost:5672/";
    const conn = await amqp.connect(rabbitConnString);
    console.log("Connected to RabbitMQ successfully.");
    const gs = new GameState(username);
    const pauseQueueName = `pause.${username}`;
    await subscribeJSON(conn, ExchangePerilDirect, pauseQueueName, PauseKey, SimpleQueueType.Transient, handlerPause(gs));
    console.log(`Declared and bound queue ${pauseQueueName} to ${ExchangePerilDirect} with routing key ${PauseKey}.`);
    const moveQueueName = `army_moves.${username}`;
    await subscribeJSON(conn, ExchangePerilTopic, moveQueueName, "army_moves.*", SimpleQueueType.Transient, handlerMove(gs), "topic");
    console.log(`Declared and bound queue ${moveQueueName} to ${ExchangePerilTopic} with routing key army_moves.*.`);
    const publishChannel = await conn.createConfirmChannel();
    let shuttingDown = false;
    const shutdown = async (signal) => {
        if (shuttingDown) {
            return;
        }
        shuttingDown = true;
        console.log(`Received ${signal}, shutting down...`);
        try {
            await conn.close();
        }
        catch (err) {
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
            }
            else if (command === "move") {
                const move = commandMove(gs, words);
                const routingKey = `army_moves.${username}`;
                await publishJSON(publishChannel, ExchangePerilTopic, routingKey, move);
                console.log(`Published move to ${routingKey}`);
            }
            else if (command === "status") {
                await commandStatus(gs);
            }
            else if (command === "help") {
                printClientHelp();
            }
            else if (command === "spam") {
                console.log("Spamming not allowed yet!");
            }
            else if (command === "quit") {
                printQuit();
                break;
            }
            else {
                console.log(`Unrecognized command: ${command}`);
            }
        }
        catch (err) {
            if (err instanceof Error) {
                console.error(err.message);
            }
            else {
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
