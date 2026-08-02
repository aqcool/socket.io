"use strict";

const assert = require("node:assert/strict");
const { createServer } = require("node:http");
const { Server } = require("socket.io");
const { io: createClient } = require("socket.io-client");
const { MongoClient, ObjectId } = require("mongodb");
const { createAdapter } = require("@socket.io/mongo-adapter");

const goURL = process.argv[2];
const mongoURI = process.argv[3];
const databaseName = process.argv[4];
if (!goURL || !mongoURI || !databaseName) {
  throw new Error("usage: node matrix.cjs <go-url> <mongo-uri> <database-name>");
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
  const mongoClient = new MongoClient(mongoURI);
  await mongoClient.connect();
  const collection = mongoClient.db(databaseName).collection("events");
  await collection.createIndex({ createdAt: 1 }, { expireAfterSeconds: 60 });

  const httpServer = createServer();
  const io = new Server(httpServer, {
    connectionStateRecovery: { maxDisconnectionDuration: 5000 },
  });
  io.adapter(createAdapter(collection, {
    addCreatedAtField: true,
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

  await new Promise((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
  const address = httpServer.address();
  return {
    io,
    httpServer,
    mongoClient,
    collection,
    url: `http://127.0.0.1:${address.port}`,
  };
}

async function waitForDocument(collection, filter, label) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await collection.findOne(filter)) return;
    await delay(25);
  }
  const recent = await collection.find({}).sort({ _id: -1 }).limit(8).toArray();
  throw new Error(`timeout waiting for MongoDB document: ${label}; filter=${JSON.stringify(filter)}; recent=${JSON.stringify(recent)}`);
}

async function stopNodeServer(peer) {
  if (!peer) return;
  await new Promise((resolve) => peer.io.close(resolve));
  if (peer.httpServer.listening) {
    await new Promise((resolve) => peer.httpServer.close(resolve));
  }
  await peer.mongoClient.close();
}

async function expectOnBoth(goSocket, nodeSocket, event, trigger, expected) {
  const goEvent = waitFor(goSocket, event);
  const nodeEvent = waitFor(nodeSocket, event);
  await trigger();
  const [goArgs, nodeArgs] = await Promise.all([goEvent, nodeEvent]);
  expected(goArgs);
  expected(nodeArgs);
}

async function recoverAcross(label, collection, sourceURL, targetURL, baseline, emitMissed) {
  const source = await connect(sourceURL);
  let replacement;
  try {
    const baselineEvent = waitFor(source, "interop-recovery-baseline");
    await baseline();
    await baselineEvent;
    const credentials = {
      sid: source.id,
      pid: source._pid,
      offset: source._lastOffset,
    };
    assert.ok(credentials.pid, "missing private session ID");
    assert.ok(credentials.offset, "missing recovery offset");

    const disconnected = waitFor(source, "disconnect");
    source.io.engine.close();
    await disconnected;
    await waitForDocument(collection, {
      type: 13,
      "data.pid": credentials.pid,
    }, `${label} session`);
    // MongoDB ObjectIds are only timestamp-ordered to the second; different
    // Node and Go processes have unrelated random suffixes. Crossing the next
    // ObjectId second makes the official adapter's `_id > offset` recovery
    // query deterministic during bilateral tests.
    const baselineSecond = Math.floor(new ObjectId(credentials.offset).getTimestamp().getTime() / 1000);
    while (Math.floor(Date.now() / 1000) <= baselineSecond) {
      await delay(25);
    }
    await emitMissed();
    await waitForDocument(collection, {
      type: 3,
      _id: { $gt: new ObjectId(credentials.offset) },
    }, `${label} missed broadcast`);

    replacement = createClient(targetURL, {
      autoConnect: false,
      forceNew: true,
      reconnection: false,
      timeout: timeoutMs,
    });
    replacement._pid = credentials.pid;
    replacement._lastOffset = credentials.offset;
    const connected = waitFor(replacement, "connect");
    const missed = waitFor(replacement, "interop-recovered-event").catch((error) => {
      throw new Error(`${label}: ${error.message}`);
    });
    replacement.connect();
    await connected;
    assert.equal(replacement.recovered, true);
    assert.equal(replacement.id, credentials.sid);
    assert.equal((await missed)[0], "missed");
  } finally {
    await closeSocket(source);
    await closeSocket(replacement);
  }
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

    await Promise.all([
      emitAck(goSocket, "interop-join", "shared-room"),
      emitAck(nodeSocket, "interop-join", "shared-room"),
    ]);
    await expectOnBoth(goSocket, nodeSocket, "interop-room-from-go", async () => {
      await emitAck(goSocket, "interop-trigger-room");
    }, (args) => assert.equal(args[0], "go-room"));
    await expectOnBoth(goSocket, nodeSocket, "interop-room-from-node", async () => {
      peer.io.to("shared-room").emit("interop-room-from-node", "node-room");
    }, (args) => assert.equal(args[0], "node-room"));

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
    assert.equal((await remotelyJoinedGo)[0], "joined");

    await emitAck(goSocket, "interop-remote-join", nodeSocket.id, "go-remote-room");
    const remotelyJoinedNode = waitFor(nodeSocket, "interop-go-remote-room");
    await emitAck(goSocket, "interop-trigger-remote-room");
    assert.equal((await remotelyJoinedNode)[0], "joined");

    await recoverAcross("Go to Node recovery", peer.collection, goURL, peer.url, async () => {
      await emitAck(goSocket, "interop-trigger-recovery-baseline");
    }, async () => {
      peer.io.emit("interop-recovered-event", "missed");
      await delay(100);
    });
    await recoverAcross("Node to Go recovery", peer.collection, peer.url, goURL, async () => {
      peer.io.emit("interop-recovery-baseline", "node");
      await delay(100);
    }, async () => {
      await emitAck(goSocket, "interop-trigger-recovered-event");
    });

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
    }, (args) => assert.equal(args[0], "go-roll"));
    await expectOnBoth(goSocket, nodeSocket, "interop-node-after-roll", async () => {
      peer.io.emit("interop-node-after-roll", "node-roll");
    }, (args) => assert.equal(args[0], "node-roll"));

    process.stdout.write(JSON.stringify({
      adapter: "@socket.io/mongo-adapter@0.4.0",
      socketIO: "4.8.3",
      bilateralBroadcast: "pass",
      binary: "pass",
      broadcastAck: "pass",
      rooms: "pass",
      fetchSockets: "pass",
      serverSideEmit: "pass",
      remoteJoin: "pass",
      recoveryGoToNode: "pass",
      recoveryNodeToGo: "pass",
      nodeExit: "pass",
      rollingRestart: "pass"
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
