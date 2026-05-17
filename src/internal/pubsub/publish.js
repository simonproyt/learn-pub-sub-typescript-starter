import { encode } from "@msgpack/msgpack";
export async function publishJSON(ch, exchange, routingKey, value) {
    const content = Buffer.from(JSON.stringify(value), "utf8");
    await new Promise((resolve, reject) => {
        ch.publish(exchange, routingKey, content, { contentType: "application/json" }, (err) => {
            if (err) {
                reject(err);
                return;
            }
            resolve();
        });
    });
}
export async function publishMsgPack(ch, exchange, routingKey, value) {
    const content = Buffer.from(encode(value));
    await new Promise((resolve, reject) => {
        ch.publish(exchange, routingKey, content, { contentType: "application/x-msgpack" }, (err) => {
            if (err) {
                reject(err);
                return;
            }
            resolve();
        });
    });
}
