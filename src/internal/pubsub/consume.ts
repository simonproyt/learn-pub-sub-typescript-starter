import type * as amqp from "amqplib";
import { decode } from "@msgpack/msgpack";
import { declareAndBind, SimpleQueueType } from "./declareAndBind.js";

export enum AckType {
  Ack,
  NackRequeue,
  NackDiscard,
}

export async function subscribe<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  simpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  deserializer: (data: Buffer) => T,
  exchangeType: "direct" | "topic" | "fanout" | "headers" = "direct",
): Promise<void> {
  const [ch, queue] = await declareAndBind(
    conn,
    exchange,
    queueName,
    routingKey,
    simpleQueueType,
    exchangeType,
  );

  await ch.consume(queue.queue, async (message) => {
    if (!message) {
      return;
    }

    try {
      const payload = deserializer(message.content);
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
      ch.nack(message, false, false);
    }
  });
}

export async function subscribeMsgPack<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  routingKey: string,
  simpleQueueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  exchangeType: "direct" | "topic" | "fanout" | "headers" = "direct",
): Promise<void> {
  return subscribe(
    conn,
    exchange,
    queueName,
    routingKey,
    simpleQueueType,
    handler,
    (data) => decode(data) as T,
    exchangeType,
  );
}
