import type * as amqp from "amqplib";
import { declareAndBind, SimpleQueueType } from "./declareAndBind.js";

export { SimpleQueueType } from "./declareAndBind.js";

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => void,
): Promise<void> {
  const [ch, queue] = await declareAndBind(conn, exchange, queueName, key, queueType);

  await ch.consume(queue.queue, (message) => {
    if (!message) {
      return;
    }

    try {
      const payload = JSON.parse(message.content.toString("utf8")) as T;
      handler(payload);
      ch.ack(message);
    } catch (err) {
      console.error("Failed to process message:", err);
      ch.ack(message);
    }
  });
}
