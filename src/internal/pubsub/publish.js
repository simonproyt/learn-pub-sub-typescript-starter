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
