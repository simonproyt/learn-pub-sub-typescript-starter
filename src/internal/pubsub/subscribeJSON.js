import { declareAndBind } from "./declareAndBind.js";
export { SimpleQueueType } from "./declareAndBind.js";
export var AckType;
(function (AckType) {
    AckType[AckType["Ack"] = 0] = "Ack";
    AckType[AckType["NackRequeue"] = 1] = "NackRequeue";
    AckType[AckType["NackDiscard"] = 2] = "NackDiscard";
})(AckType = AckType || (AckType = {}));
export async function subscribeJSON(conn, exchange, queueName, key, queueType, handler, exchangeType = "direct") {
    const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType, exchangeType);
    await ch.consume(queue.queue, async (message) => {
        if (!message) {
            return;
        }
        try {
            const payload = JSON.parse(message.content.toString("utf8"));
            const ackType = await handler(payload);
            if (ackType === AckType.Ack) {
                ch.ack(message);
            }
            else if (ackType === AckType.NackRequeue) {
                ch.nack(message, false, true);
            }
            else {
                ch.nack(message, false, false);
            }
        }
        catch (err) {
            console.error("Failed to process message:", err);
            console.log("Message processing failed: NACK discard");
            ch.nack(message, false, false);
        }
    });
}
