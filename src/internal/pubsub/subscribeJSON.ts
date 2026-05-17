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
  handler: (data: T) => AckType,
  exchangeType: "direct" | "topic" | "fanout" | "headers" = "direct",
): Promise<void> {
  const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType, exchangeType);

  await ch.consume(queue.queue, (message) => {
    if (!message) {
      return;
    }

    try {
      const payload = JSON.parse(message.content.toString("utf8")) as T;
      const ackType = handler(payload);
      if (ackType === AckType.Ack) {
        console.log("Message processed successfully: ACK");
        ch.ack(message);
      } else if (ackType === AckType.NackRequeue) {
        console.log("Message processing failed: NACK requeue");
        ch.nack(message, false, true);
      } else {
        console.log("Message processing failed: NACK discard");
        ch.nack(message, false, false);
      }
    } catch (err) {
      console.error("Failed to process message:", err);
      console.log("Message processing failed: NACK discard");
      ch.nack(message, false, false);
    }
  });
}
