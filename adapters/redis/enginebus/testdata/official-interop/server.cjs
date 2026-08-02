"use strict";

const { createServer } = require("node:http");
const { RedisEngine } = require("@socket.io/cluster-engine");

async function main() {
  const mode = process.env.CLUSTER_ENGINE_REDIS_CLIENT;
  const redisUrl = process.env.CLUSTER_ENGINE_REDIS_URL;
  const channelPrefix = process.env.CLUSTER_ENGINE_CHANNEL_PREFIX || "engine.io";
  const port = Number(process.env.CLUSTER_ENGINE_NODE_PORT);
  if (!mode || !redisUrl || !Number.isInteger(port)) {
    throw new Error("missing cluster-engine interop configuration");
  }

  let pubClient;
  let subClient;
  let disconnect;
  if (mode === "redis") {
    const { createClient } = require("redis");
    pubClient = createClient({ url: redisUrl });
    subClient = pubClient.duplicate();
    await Promise.all([pubClient.connect(), subClient.connect()]);
    disconnect = async () => {
      await Promise.all([pubClient.disconnect(), subClient.disconnect()]);
    };
  } else if (mode === "ioredis") {
    const Redis = require("ioredis");
    pubClient = new Redis(redisUrl);
    subClient = pubClient.duplicate();
    await Promise.all([
      new Promise((resolve, reject) => {
        pubClient.once("ready", resolve);
        pubClient.once("error", reject);
      }),
      new Promise((resolve, reject) => {
        subClient.once("ready", resolve);
        subClient.once("error", reject);
      }),
    ]);
    disconnect = async () => {
      pubClient.disconnect();
      subClient.disconnect();
    };
  } else {
    throw new Error(`unknown Redis client mode: ${mode}`);
  }

  const httpServer = createServer();
  const engine = new RedisEngine(pubClient, subClient, {
    pingInterval: 50,
    channelPrefix,
  });
  engine.on("connection", (socket) => {
    socket.on("message", (value) => socket.send(value));
  });
  engine.attach(httpServer);

  const generalChannel = `${channelPrefix}#`;
  const directedChannel = `${channelPrefix}#${engine._nodeId}#`;
  let subscriptionsReady = false;
  for (let attempt = 0; attempt < 100; attempt++) {
    if (mode === "redis") {
      const counts = await pubClient.pubSubNumSub([
        generalChannel,
        directedChannel,
      ]);
      subscriptionsReady =
        Number(counts[generalChannel]) >= 1 &&
        Number(counts[directedChannel]) >= 1;
    } else {
      const counts = await pubClient.pubsub(
        "numsub",
        generalChannel,
        directedChannel,
      );
      subscriptionsReady =
        Number(counts[1]) >= 1 && Number(counts[3]) >= 1;
    }
    if (subscriptionsReady) break;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  if (!subscriptionsReady) {
    throw new Error("RedisEngine subscriptions did not become ready");
  }

  let stopping = false;
  const stop = async () => {
    if (stopping) return;
    stopping = true;
    engine.close();
    await new Promise((resolve) => httpServer.close(resolve));
    await disconnect();
    process.exit(0);
  };
  process.on("SIGTERM", () => void stop());
  process.on("SIGINT", () => void stop());

  httpServer.listen(port, "127.0.0.1", () => {
    process.stdout.write(JSON.stringify({ ready: true, port }) + "\n");
  });
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
