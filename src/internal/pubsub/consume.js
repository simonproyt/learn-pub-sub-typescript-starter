import { decode } from "@msgpack/msgpack";
import { declareAndBind } from "./declareAndBind.js";
export var AckType;
(function (AckType) {
    AckType[AckType["Ack"] = 0] = "Ack";
    AckType[AckType["NackRequeue"] = 1] = "NackRequeue";
    AckType[AckType["NackDiscard"] = 2] = "NackDiscard";
})(AckType = AckType || (AckType = {}));
export async function subscribe(conn, exchange, queueName, routingKey, simpleQueueType, handler, deserializer, exchangeType = "direct") {
    const [ch, queue] = await declareAndBind(conn, exchange, queueName, routingKey, simpleQueueType, exchangeType);
    await ch.consume(queue.queue, async (message) => {
        if (!message) {
            return;
        }
        try {
            const payload = deserializer(message.content);
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
            ch.nack(message, false, false);
        }
    });
}
export async function subscribeMsgPack(conn, exchange, queueName, routingKey, simpleQueueType, handler, exchangeType = "direct") {
    return subscribe(conn, exchange, queueName, routingKey, simpleQueueType, handler, (data) => decode(data), exchangeType);
}
