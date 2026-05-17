import type * as amqp from "amqplib";
import { declareAndBind, SimpleQueueType } from "./declareAndBind.js";

export { SimpleQueueType } from "./declareAndBind.js";

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard,
}

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  exchangeType: "direct" | "topic" | "fanout" | "headers" = "direct",
): Promise<void> {
  const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType, exchangeType);

  await ch.consume(queue.queue, async (message) => {
    if (!message) {
      return;
    }

    try {
      const payload = JSON.parse(message.content.toString("utf8")) as T;
      const ackType = await handler(payload);
      if (ackType === AckType.Ack) {
        ch.ack(message);
      } else if (ackType === AckType.NackRequeue) {
        ch.nack(message, false, true);
      } else {
        ch.nack(message, false, false);
      }
    } catch (err) {
      console.error("Failed to process message:", err);
      console.log("Message processing failed: NACK discard");
      ch.nack(message, false, false);
    }
  });
}
