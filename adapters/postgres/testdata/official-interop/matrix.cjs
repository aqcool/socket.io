"use strict";

const assert = require("node:assert/strict");
const { createServer } = require("node:http");
const { Server } = require("socket.io");
const { io: createClient } = require("socket.io-client");
const { Pool } = require("pg");
const { createAdapter } = require("@socket.io/postgres-adapter");

const goURL = process.argv[2];
const postgresURI = process.argv[3];
const channelPrefix = process.argv[4];
const tableName = process.argv[5];
if (!goURL || !postgresURI || !channelPrefix || !tableName) {
  throw new Error("usage: node matrix.cjs <go-url> <postgres-uri> <channel-prefix> <table-name>");
}

const timeoutMs = 10000;
const delay = (duration) => new Promise((resolve) => setTimeout(resolve, duration));

function toBuffer(value) {
  if (Buffer.isBuffer(value)) return value;
  if (value instanceof Uint8Array || Array.isArray(value)) return Buffer.from(value);
  if (value && Array.isArray(value.data)) return Buffer.from(value.data);
  throw new TypeError(`expected binary ACK, got ${JSON.stringify(value)}`);
}

function waitFor(socket, event, timeout = timeoutMs) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`timeout waiting for ${event}`)), timeout);
    socket.once(event, (...args) => {
      clearTimeout(timer);
      resolve(args);
    });
  });
}

async function connect(url) {
  const socket = createClient(url, {
    autoConnect: false,
    forceNew: true,
    reconnection: false,
    timeout: timeoutMs,
  });
  const connected = waitFor(socket, "connect");
  const failed = waitFor(socket, "connect_error").then(([error]) => {
    throw error;
  });
  socket.connect();
  await Promise.race([connected, failed]);
  return socket;
}

function emitAck(socket, event, ...args) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`ACK timeout for ${event}`)), timeoutMs);
    socket.emit(event, ...args, (...values) => {
      clearTimeout(timer);
      resolve(values);
    });
  });
}

async function closeSocket(socket) {
  if (!socket) return;
  socket.removeAllListeners();
  socket.close();
  await delay(20);
}

async function startNodeServer() {
  const pool = new Pool({ connectionString: postgresURI });
  await pool.query(`
    CREATE TABLE IF NOT EXISTS ${tableName} (
      id bigserial UNIQUE,
      created_at timestamptz DEFAULT NOW(),
      payload bytea
    )
  `);
  const httpServer = createServer();
  const io = new Server(httpServer);
  io.adapter(createAdapter(pool, {
    channelPrefix,
    tableName,
    payloadThreshold: 8000,
    cleanupInterval: 30000,
    heartbeatInterval: 200,
    heartbeatTimeout: 600,
  }));
  io.on("connection", (socket) => {
    socket.on("interop-join", (room, ack) => {
      socket.join(room);
      ack("joined");
    });
  });
  io.on("interop-go-cluster", (value, ack) => ack(`node:${value}`));
  io.of("/").adapter.init();
  await new Promise((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
  const address = httpServer.address();
  return { io, httpServer, pool, url: `http://127.0.0.1:${address.port}` };
}

async function stopNodeServer(peer) {
  if (!peer) return;
  await new Promise((resolve) => peer.io.close(resolve));
  if (peer.httpServer.listening) {
    await new Promise((resolve) => peer.httpServer.close(resolve));
  }
  await peer.pool.end();
}

async function expectOnBoth(goSocket, nodeSocket, event, trigger, expected) {
  const goEvent = waitFor(goSocket, event).catch((error) => {
    throw new Error(`Go client ${event}: ${error.message}`);
  });
  const nodeEvent = waitFor(nodeSocket, event).catch((error) => {
    throw new Error(`Node client ${event}: ${error.message}`);
  });
  await trigger();
  const [goArgs, nodeArgs] = await Promise.all([goEvent, nodeEvent]);
  expected(goArgs);
  expected(nodeArgs);
}

async function main() {
  let peer = await startNodeServer();
  let goSocket;
  let nodeSocket;
  try {
    goSocket = await connect(goURL);
    nodeSocket = await connect(peer.url);
    await delay(1000);

    await expectOnBoth(goSocket, nodeSocket, "interop-from-go", async () => {
      await emitAck(goSocket, "interop-trigger-broadcast");
    }, ([label, binary]) => {
      assert.equal(label, "go");
      assert.deepEqual(Buffer.from(binary), Buffer.from([1, 2, 3]));
    });
    await expectOnBoth(goSocket, nodeSocket, "interop-from-node", async () => {
      peer.io.emit("interop-from-node", "node", Buffer.from([4, 5, 6]));
    }, ([label, binary]) => {
      assert.equal(label, "node");
      assert.deepEqual(Buffer.from(binary), Buffer.from([4, 5, 6]));
    });

    goSocket.once("interop-ack-from-node", (_label, ack) => ack(Buffer.from([7])));
    nodeSocket.once("interop-ack-from-node", (_label, ack) => ack(Buffer.from([8])));
    const nodeAckResponses = await peer.io
      .timeout(2000)
      .emitWithAck("interop-ack-from-node", "node");
    assert.equal(nodeAckResponses.length, 2);
    assert.deepEqual(
      nodeAckResponses.map((value) => toBuffer(value).toString("hex")).sort(),
      ["07", "08"],
    );

    goSocket.once("interop-ack-from-go", (_label, ack) => ack(Buffer.from([9])));
    nodeSocket.once("interop-ack-from-go", (_label, ack) => ack(Buffer.from([10])));
    const [goAckError, goAckResponses] = await emitAck(
      goSocket,
      "interop-trigger-broadcast-ack",
    );
    assert.equal(goAckError, null);
    assert.equal(goAckResponses.length, 2);
    assert.deepEqual(
      goAckResponses.map((value) => toBuffer(value).toString("hex")).sort(),
      ["09", "0a"],
    );

    const large = "x".repeat(20000);
    await expectOnBoth(goSocket, nodeSocket, "interop-large-from-go", async () => {
      await emitAck(goSocket, "interop-trigger-large");
    }, ([payload]) => assert.equal(payload, large));
    await expectOnBoth(goSocket, nodeSocket, "interop-large-from-node", async () => {
      peer.io.emit("interop-large-from-node", large);
    }, ([payload]) => assert.equal(payload, large));

    await Promise.all([
      emitAck(goSocket, "interop-join", "shared-room"),
      emitAck(nodeSocket, "interop-join", "shared-room"),
    ]);
    await expectOnBoth(goSocket, nodeSocket, "interop-room-from-go", async () => {
      await emitAck(goSocket, "interop-trigger-room");
    }, (args) => assert.deepEqual(args, ["go-room"]));
    await expectOnBoth(goSocket, nodeSocket, "interop-room-from-node", async () => {
      peer.io.to("shared-room").emit("interop-room-from-node", "node-room");
    }, (args) => assert.deepEqual(args, ["node-room"]));

    assert.equal((await peer.io.fetchSockets()).length, 2);
    const [goFetchCount, goFetchError] = await emitAck(goSocket, "interop-fetch");
    assert.equal(goFetchError, null);
    assert.equal(goFetchCount, 2);

    const nodeResponses = await peer.io.serverSideEmitWithAck("interop-node-cluster", "value");
    assert.deepEqual(nodeResponses, ["go:value"]);
    const [goClusterError, goClusterResponses] = await emitAck(goSocket, "interop-trigger-server-side");
    assert.equal(goClusterError, null);
    assert.deepEqual(goClusterResponses, ["node:value"]);

    peer.io.in(goSocket.id).socketsJoin("node-remote-room");
    await delay(300);
    const remotelyJoinedGo = waitFor(goSocket, "interop-node-remote-room");
    peer.io.to("node-remote-room").emit("interop-node-remote-room", "joined");
    assert.deepEqual(await remotelyJoinedGo, ["joined"]);

    await emitAck(goSocket, "interop-remote-join", nodeSocket.id, "go-remote-room");
    const remotelyJoinedNode = waitFor(nodeSocket, "interop-go-remote-room");
    await emitAck(goSocket, "interop-trigger-remote-room");
    assert.deepEqual(await remotelyJoinedNode, ["joined"]);

    await closeSocket(nodeSocket);
    nodeSocket = null;
    await stopNodeServer(peer);
    peer = null;
    await delay(1200);
    const [remainingCount, remainingError] = await emitAck(goSocket, "interop-fetch");
    assert.equal(remainingError, null);
    assert.equal(remainingCount, 1);

    peer = await startNodeServer();
    nodeSocket = await connect(peer.url);
    await delay(1000);
    await expectOnBoth(goSocket, nodeSocket, "interop-after-roll", async () => {
      await emitAck(goSocket, "interop-trigger-after-roll");
    }, (args) => assert.deepEqual(args, ["go-roll"]));
    await expectOnBoth(goSocket, nodeSocket, "interop-node-after-roll", async () => {
      peer.io.emit("interop-node-after-roll", "node-roll");
    }, (args) => assert.deepEqual(args, ["node-roll"]));

    process.stdout.write(JSON.stringify({
      adapter: "@socket.io/postgres-adapter@0.5.0",
      socketIO: "4.8.3",
      bilateralBroadcast: "pass",
      binaryAttachment: "pass",
      broadcastAck: "pass",
      largeAttachment: "pass",
      rooms: "pass",
      fetchSockets: "pass",
      serverSideEmit: "pass",
      remoteJoin: "pass",
      nodeExit: "pass",
      rollingRestart: "pass",
      recovery: "not-supported-by-official-adapter"
    }) + "\n");
  } finally {
    await closeSocket(goSocket);
    await closeSocket(nodeSocket);
    await stopNodeServer(peer);
  }
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : error);
  process.exitCode = 1;
});
