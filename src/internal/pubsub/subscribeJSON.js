import { declareAndBind } from "./declareAndBind.js";
export { SimpleQueueType } from "./declareAndBind.js";
export async function subscribeJSON(conn, exchange, queueName, key, queueType, handler) {
    const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);
    await ch.consume(queue.queue, (message) => {
        if (!message) {
            return;
        }
        try {
            const payload = JSON.parse(message.content.toString("utf8"));
            handler(payload);
            ch.ack(message);
        }
        catch (err) {
            console.error("Failed to process message:", err);
            ch.ack(message);
        }
    });
}
