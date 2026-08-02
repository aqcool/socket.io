"use strict";

const { createServer } = require("node:http");
const { Server } = require("socket.io");
const adapterGeneration = process.argv[4] || "current";
const redisAdapterPackage = adapterGeneration === "previous"
  ? "@socket.io/redis-adapter-previous"
  : "@socket.io/redis-adapter";
const streamsAdapterPackage = adapterGeneration === "previous"
  ? "@socket.io/redis-streams-adapter-previous"
  : "@socket.io/redis-streams-adapter";
const {
  createAdapter: createPubSubAdapter,
  createShardedAdapter,
} = require(redisAdapterPackage);
const {
  createAdapter: createStreamsAdapter,
} = require(streamsAdapterPackage);
const { createClient: createRedisClient } = require("redis");

const redisURL = process.argv[2];
const adapterMode = process.argv[3];
if (!redisURL || !["pubsub", "streams", "sharded"].includes(adapterMode) ||
    !["current", "previous"].includes(adapterGeneration)) {
  throw new Error("usage: node worker.cjs <redis-url> <pubsub|streams|sharded> [current|previous]");
}

async function main() {
  const pubClient = createRedisClient({ url: redisURL });
  const subClient = pubClient.duplicate();
  pubClient.on("error", () => {});
  subClient.on("error", () => {});
  await Promise.all([pubClient.connect(), subClient.connect()]);

  const httpServer = createServer();
  const io = new Server(httpServer, { transports: ["websocket"] });
  if (adapterMode === "streams") {
    io.adapter(createStreamsAdapter(pubClient, { blockTimeInMs: 100 }));
  } else if (adapterMode === "sharded") {
    io.adapter(createShardedAdapter(pubClient, subClient, { subscriptionMode: "dynamic" }));
  } else {
    io.adapter(createPubSubAdapter(pubClient, subClient, {
      publishOnSpecificResponseChannel: true,
    }));
  }
  io.on("connection", (socket) => {
    socket.join("worker-room");
    socket.on("control-worker-broadcast", (value, ack) => {
      io.emit("from-worker", adapterGeneration, value);
      ack();
    });
  });
  io.on("from-node-server", (value, ack) => {
    ack(`worker-${adapterGeneration}:${value}`);
  });
  io.on("from-go-server", (value, ack) => {
    ack(`worker-${adapterGeneration}:${value}`);
  });

  await new Promise((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
  const address = httpServer.address();
  if (process.send) process.send({ port: address.port, generation: adapterGeneration });
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
