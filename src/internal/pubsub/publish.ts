import type { ConfirmChannel } from "amqplib";
import { encode } from "@msgpack/msgpack";

export async function publishJSON<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const content = Buffer.from(JSON.stringify(value), "utf8");

  await new Promise<void>((resolve, reject) => {
    ch.publish(
      exchange,
      routingKey,
      content,
      { contentType: "application/json" },
      (err) => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      },
    );
  });
}

export async function publishMsgPack<T>(
  ch: ConfirmChannel,
  exchange: string,
  routingKey: string,
  value: T,
): Promise<void> {
  const content = Buffer.from(encode(value));

  await new Promise<void>((resolve, reject) => {
    ch.publish(
      exchange,
      routingKey,
      content,
      { contentType: "application/x-msgpack" },
      (err) => {
        if (err) {
          reject(err);
          return;
        }
        resolve();
      },
    );
  });
}
