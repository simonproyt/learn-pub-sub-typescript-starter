import amqp from "amqplib";

async function main() {
  console.log("Starting Peril server...");

  const rabbitConnString = "amqp://guest:guest@localhost:5672/";
  const conn = await amqp.connect(rabbitConnString);
  console.log("Connected to RabbitMQ successfully.");

  const shutdown = async (signal: string) => {
    console.log(`Received ${signal}, shutting down...`);
    await conn.close();
    process.exit(0);
  };

  process.on("SIGINT", () => void shutdown("SIGINT"));
  process.on("SIGTERM", () => void shutdown("SIGTERM"));

  console.log("Server is waiting for shutdown signal...");
}

main().catch((err) => {
  console.error("Fatal error:", err);
  process.exit(1);
});
