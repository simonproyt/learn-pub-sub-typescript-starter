import amqp from "amqplib";
import { writeLog } from "../internal/gamelogic/logs.js";
import { printServerHelp, getInput, } from "../internal/gamelogic/gamelogic.js";
import { ExchangePerilDirect, ExchangePerilTopic, GameLogSlug, PauseKey, } from "../internal/routing/routing.js";
import { publishJSON } from "../internal/pubsub/publish.js";
import { subscribeMsgPack } from "../internal/pubsub/consume.js";
import { AckType } from "../internal/pubsub/subscribeJSON.js";
import { SimpleQueueType } from "../internal/pubsub/declareAndBind.js";
async function main() {
    console.log("Starting Peril server...");
    printServerHelp();
    const rabbitConnString = "amqp://guest:guest@localhost:5672/";
    const conn = await amqp.connect(rabbitConnString);
    console.log("Connected to RabbitMQ successfully.");
    const ch = await conn.createConfirmChannel();
    await ch.assertExchange(ExchangePerilDirect, "direct");
    await subscribeMsgPack(conn, ExchangePerilTopic, GameLogSlug, `${GameLogSlug}.*`, SimpleQueueType.Durable, async (gameLog) => {
        try {
            await writeLog(gameLog);
            process.stdout.write("> ");
            return AckType.Ack;
        }
        catch (err) {
            console.error("Failed to write game log:", err);
            process.stdout.write("> ");
            return AckType.NackDiscard;
        }
    }, "topic");
    console.log(`Subscribed to durable queue ${GameLogSlug} on ${ExchangePerilTopic} with routing key ${GameLogSlug}.*.`);
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
        const words = await getInput("Server> ");
        if (words.length === 0) {
            continue;
        }
        const firstWord = words[0];
        if (!firstWord) {
            continue;
        }
        const command = firstWord.toLowerCase();
        if (command === "pause") {
            console.log("Sending pause message...");
            const pauseEvent = { isPaused: true };
            await publishJSON(ch, ExchangePerilDirect, PauseKey, pauseEvent);
            console.log("Pause message sent.");
        }
        else if (command === "resume") {
            console.log("Sending resume message...");
            const resumeEvent = { isPaused: false };
            await publishJSON(ch, ExchangePerilDirect, PauseKey, resumeEvent);
            console.log("Resume message sent.");
        }
        else if (command === "quit") {
            console.log("Exiting server...");
            break;
        }
        else {
            console.log(`Unrecognized command: ${command}`);
        }
    }
    await conn.close();
}
main().catch((err) => {
    console.error("Fatal error:", err);
    process.exit(1);
});
