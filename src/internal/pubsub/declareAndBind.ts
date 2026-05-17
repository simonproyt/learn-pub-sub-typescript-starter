import type * as amqp from "amqplib";

export enum SimpleQueueType {
  Durable,
  Transient,
}

export async function declareAndBind(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  exchangeType: "direct" | "topic" | "fanout" | "headers" = "direct",
): Promise<[amqp.Channel, amqp.Replies.AssertQueue]> {
  const ch = await conn.createChannel();
  await ch.assertExchange(exchange, exchangeType);
  const queue = await ch.assertQueue(queueName, {
    durable: queueType === SimpleQueueType.Durable,
    exclusive: queueType === SimpleQueueType.Transient,
    autoDelete: queueType === SimpleQueueType.Transient,
  });
  await ch.bindQueue(queueName, exchange, key);
  return [ch, queue];
}
