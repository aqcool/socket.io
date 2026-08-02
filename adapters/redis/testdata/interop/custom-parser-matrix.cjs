"use strict";

const assert = require("node:assert/strict");
const { createServer } = require("node:http");
const { Server } = require("socket.io");
const { io: createClient } = require("socket.io-client");
const { createClient: createRedisClient } = require("redis");
const { createAdapter } = require("@socket.io/redis-adapter");
const { Emitter } = require("@socket.io/redis-emitter");

const [goURL, redisURL, key] = process.argv.slice(2);
if (!goURL || !redisURL || !key) {
  throw new Error(
    "usage: node custom-parser-matrix.cjs <go-url> <redis-url> <key>",
  );
}

const timeoutMs = 10_000;
const delay = (duration) =>
  new Promise((resolve) => setTimeout(resolve, duration));
const parser = {
  encode: (value) => JSON.stringify(value),
  decode: (value) => JSON.parse(value),
};

function waitFor(socket, event) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.off(event, onEvent);
      reject(new Error(`timeout waiting for ${event}`));
    }, timeoutMs);
    const onEvent = (...args) => {
      clearTimeout(timer);
      resolve(args);
    };
    socket.once(event, onEvent);
  });
}

function emitAck(socket, event, ...args) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`ACK timeout for ${event}`)),
      timeoutMs,
    );
    socket.emit(event, ...args, (...values) => {
      clearTimeout(timer);
      resolve(values);
    });
  });
}

async function connect(url) {
  const socket = createClient(url, {
    forceNew: true,
    reconnection: false,
    transports: ["websocket"],
    timeout: timeoutMs,
  });
  await waitFor(socket, "connect");
  return socket;
}

async function closeSocket(socket) {
  if (!socket) return;
  socket.removeAllListeners();
  socket.close();
  await delay(20);
}

async function expectBoth(goSocket, nodeSocket, event, trigger) {
  const goEvent = waitFor(goSocket, event);
  const nodeEvent = waitFor(nodeSocket, event);
  await trigger();
  const [goArgs, nodeArgs] = await Promise.all([goEvent, nodeEvent]);
  for (const args of [goArgs, nodeArgs]) {
    assert.equal(args[0], 1);
    assert.equal(args[1], "2");
    assert.deepEqual(args[2], [3]);
  }
}

async function main() {
  const pubClient = createRedisClient({ url: redisURL });
  const subClient = pubClient.duplicate();
  pubClient.on("error", () => {});
  subClient.on("error", () => {});
  await Promise.all([pubClient.connect(), subClient.connect()]);

  const httpServer = createServer();
  const io = new Server(httpServer, { transports: ["websocket"] });
  io.adapter(createAdapter(pubClient, subClient, { key, parser }));
  await new Promise((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
  const nodeURL = `http://127.0.0.1:${httpServer.address().port}`;
  const emitter = new Emitter(pubClient, {
    key,
    parser: { encode: parser.encode },
  });
  let goSocket;
  let nodeSocket;
  try {
    [goSocket, nodeSocket] = await Promise.all([
      connect(goURL),
      connect(nodeURL),
    ]);
    await delay(300);
    await expectBoth(goSocket, nodeSocket, "official-custom-payload", () => {
      emitter.emit("official-custom-payload", 1, "2", [3]);
    });
    await expectBoth(goSocket, nodeSocket, "go-custom-payload", async () => {
      const [error] = await emitAck(
        goSocket,
        "interop-go-custom-parser-emitter",
      );
      assert.equal(error, null);
    });
    process.stdout.write(
      `${JSON.stringify({
        emitter: "@socket.io/redis-emitter@5.1.0",
        officialCustomParserBehavior: "1/1",
        officialEmitterToGoAdapter: "pass",
        goEmitterToOfficialAdapter: "pass",
      })}\n`,
    );
  } finally {
    await closeSocket(goSocket);
    await closeSocket(nodeSocket);
    await new Promise((resolve) => io.close(resolve));
    if (httpServer.listening)
      await new Promise((resolve) => httpServer.close(resolve));
    await Promise.allSettled([pubClient.quit(), subClient.quit()]);
  }
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : error);
  process.exitCode = 1;
});
