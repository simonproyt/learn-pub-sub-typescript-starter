import { subscribe } from "./consume.js";
export { SimpleQueueType } from "./declareAndBind.js";
export { AckType } from "./consume.js";
export async function subscribeJSON(conn, exchange, queueName, key, queueType, handler, exchangeType = "direct") {
    return subscribe(conn, exchange, queueName, key, queueType, handler, (data) => JSON.parse(data.toString("utf8")), exchangeType);
}
