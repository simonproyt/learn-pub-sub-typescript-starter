import type * as amqp from "amqplib";
import { subscribe, AckType } from "./consume.js";
import { SimpleQueueType } from "./declareAndBind.js";

export { SimpleQueueType } from "./declareAndBind.js";
export { AckType } from "./consume.js";

export async function subscribeJSON<T>(
  conn: amqp.ChannelModel,
  exchange: string,
  queueName: string,
  key: string,
  queueType: SimpleQueueType,
  handler: (data: T) => Promise<AckType> | AckType,
  exchangeType: "direct" | "topic" | "fanout" | "headers" = "direct",
): Promise<void> {
  return subscribe(
    conn,
    exchange,
    queueName,
    key,
    queueType,
    handler,
    (data) => JSON.parse(data.toString("utf8")) as T,
    exchangeType,
  );
}
