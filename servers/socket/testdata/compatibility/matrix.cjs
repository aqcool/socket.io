"use strict";

const assert = require("node:assert/strict");
const http = require("node:http");

function clientFactory(packageName) {
  const client = require(packageName);
  return client.io || client.default || client;
}

const allClients = [
  { major: 2, io: clientFactory("socket.io-client-v2") },
  { major: 3, io: clientFactory("socket.io-client-v3") },
  { major: 4, io: clientFactory("socket.io-client-v4") },
];

const strictURL = process.argv[2];
const compatibilityURL = process.argv[3];
const recoveryURL = process.argv[4];
const recoveryWithMiddlewareURL = process.argv[5];
const cleanupURL = process.argv[6];
const largePayloadURL = process.argv[7];
const lifecycleURL = process.argv[8];
const selectedMajor = process.argv[9];
if (!strictURL || !compatibilityURL || !recoveryURL || !recoveryWithMiddlewareURL || !cleanupURL || !largePayloadURL || !lifecycleURL) {
  throw new Error("usage: node matrix.cjs <strict-server-url> <EIO3-compatible-server-url> <recovery-server-url> <recovery-with-middleware-server-url> <cleanup-server-url> <large-payload-server-url> <lifecycle-server-url> [client-major]");
}
const clients = selectedMajor
  ? allClients.filter((client) => String(client.major) === selectedMajor)
  : allClients;
if (clients.length === 0) {
  throw new Error(`unsupported client major: ${selectedMajor}`);
}

// The Go race detector significantly slows down the transport stack in CI,
// especially while several official clients connect concurrently.
const timeoutMs = 20000;
const results = [];

function socketOptions(extra = {}) {
  return {
    forceNew: true,
    reconnection: false,
    timeout: timeoutMs,
    ...extra,
  };
}

function waitFor(socket, event, timeout = timeoutMs) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.off(event, onEvent);
      reject(new Error(`timed out waiting for "${event}"`));
    }, timeout);
    const onEvent = (...args) => {
      clearTimeout(timer);
      resolve(args);
    };
    socket.once(event, onEvent);
  });
}

function waitForConnect(socket, timeout = timeoutMs) {
  return new Promise((resolve, reject) => {
    const cleanup = () => {
      clearTimeout(timer);
      socket.off("connect", onConnect);
      socket.off("connect_error", onError);
    };
    const onConnect = (...args) => {
      cleanup();
      resolve(args);
    };
    const onError = (error) => {
      cleanup();
      const message = error && error.message ? error.message : String(error);
      reject(new Error(`connection failed: ${message}`, { cause: error }));
    };
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`timed out waiting for "connect": ${socketState(socket)}`));
    }, timeout);
    socket.once("connect", onConnect);
    socket.once("connect_error", onError);
  });
}

function socketState(socket) {
  const engine = socket.io && socket.io.engine;
  return JSON.stringify({
    socketConnected: socket.connected,
    socketDisconnected: socket.disconnected,
    managerState: socket.io && socket.io._readyState,
    engineState: engine && engine.readyState,
    transport: engine && engine.transport && engine.transport.name,
    engineTrace: socket.__matrixEngineTrace,
  });
}

function attachEngineTrace(socket) {
  const engine = socket.io && socket.io.engine;
  if (!engine) return;
  const trace = [];
  socket.__matrixEngineTrace = trace;
  const record = (direction) => (packet) => {
    const data = packet && packet.data;
    trace.push({
      direction,
      type: packet && packet.type,
      data: typeof data === "string" ? data.slice(0, 120) : typeof data,
    });
    if (trace.length > 20) trace.shift();
  };
  engine.on("packet", record("in"));
  engine.on("packetCreate", record("out"));
}

function delay(duration) {
  return new Promise((resolve) => setTimeout(resolve, duration));
}

function httpGet(url) {
  return new Promise((resolve, reject) => {
    const request = http.get(url, (response) => {
      const chunks = [];
      response.on("data", (chunk) => chunks.push(chunk));
      response.on("end", () => resolve({
        statusCode: response.statusCode,
        headers: response.headers,
        body: Buffer.concat(chunks),
      }));
    });
    request.on("error", reject);
  });
}

async function closeSocket(socket) {
  const manager = socket.io;
  const engine = manager && manager.engine;
  let disconnected = Promise.resolve();
  if (socket.connected) {
    disconnected = new Promise((resolve) => {
      const timer = setTimeout(resolve, 1000);
      socket.once("disconnect", () => {
        clearTimeout(timer);
        resolve();
      });
    });
  }

  let engineClosed = Promise.resolve();
  if (engine && engine.readyState !== "closed") {
    engineClosed = new Promise((resolve) => {
      const timer = setTimeout(resolve, 1000);
      engine.once("close", () => {
        clearTimeout(timer);
        resolve();
      });
    });
  }

  socket.close();
  await Promise.all([disconnected, engineClosed]);
}

async function connect(io, url, options = {}) {
  const socket = io(url, socketOptions({ ...options, autoConnect: false }));
  try {
    const connected = waitForConnect(socket);
    socket.connect();
    attachEngineTrace(socket);
    await connected;
    return socket;
  } catch (error) {
    await closeSocket(socket);
    throw error;
  }
}

async function expectConnectError(io, url, options = {}, legacyErrorEvent = false) {
  const socket = io(url, socketOptions({ ...options, autoConnect: false }));
  try {
    const eventNames = legacyErrorEvent ? ["connect_error", "error"] : ["connect_error"];
    const failure = new Promise((resolve, reject) => {
      const timer = setTimeout(() => reject(new Error(
        `timed out waiting for ${eventNames.join(" or ")}; ${socketState(socket)}`,
      )), timeoutMs);
      const onError = (value) => {
        clearTimeout(timer);
        socket.off("connect", onUnexpectedConnect);
        for (const eventName of eventNames) {
          socket.off(eventName, onError);
        }
        resolve(value);
      };
      const onUnexpectedConnect = () => {
        clearTimeout(timer);
        for (const eventName of eventNames) {
          socket.off(eventName, onError);
        }
        reject(new Error("connection unexpectedly succeeded with invalid credentials"));
      };
      socket.once("connect", onUnexpectedConnect);
      for (const eventName of eventNames) {
        socket.once(eventName, onError);
      }
    });
    socket.connect();
    attachEngineTrace(socket);
    const error = await failure;
    assert.ok(error, "connection failure event must include an error");
  } finally {
    await closeSocket(socket);
  }
}

function emitWithAck(socket, event, ...args) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(
      `timed out waiting for ACK of "${event}"; ${socketState(socket)}`,
    )), timeoutMs);
    socket.emit(event, ...args, (...ackArgs) => {
      clearTimeout(timer);
      resolve(ackArgs);
    });
  });
}

async function waitUntil(predicate, description) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (predicate()) {
      return;
    }
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  throw new Error(`timed out waiting for ${description}`);
}

async function checkTransport(client, transports, expected, upgrade) {
  const socket = client.io(compatibilityURL, socketOptions({
    autoConnect: false,
    transports,
    upgrade,
  }));
  try {
    const connected = waitForConnect(socket);
    socket.connect();
    attachEngineTrace(socket);
    const engine = socket.io.engine;
    const initialTransport = engine.transport.name;
    await connected;
    if (expected === "websocket" && initialTransport === "polling") {
      await waitUntil(() => engine.transport.name === "websocket", "WebSocket upgrade");
    }
    assert.equal(engine.transport.name, expected);
    if (upgrade) {
      assert.equal(initialTransport, "polling");
    }
  } finally {
    await closeSocket(socket);
  }
}

async function checkFeatures(client) {
  const socket = await connect(client.io, compatibilityURL, {
    transports: ["websocket"],
  });
  try {
    const [serverSocketID] = await emitWithAck(socket, "server-socket-id");
    assert.equal(
      serverSocketID,
      socket.id,
      "the public Socket.IO ID must match on the client and server",
    );

    const [echo] = await emitWithAck(socket, "echo", `v${client.major}`);
    assert.equal(echo, `v${client.major}`);

    const binary = Buffer.from([0, 1, 2, 127, 255]);
    const [binaryEcho] = await emitWithAck(socket, "binary", binary);
    assert.deepEqual(Buffer.from(binaryEcho), binary);

	const utf8 = "你好，Socket.IO — Привет — مرحبًا";
	const [utf8Echo] = await emitWithAck(socket, "echo", utf8);
	assert.equal(utf8Echo, utf8);

	const firstBinary = Buffer.from([1, 2, 3]);
	const secondBinary = Buffer.from([4, 5, 6]);
	const [numberValue, stringValue, echoedFirst, echoedNested] = await emitWithAck(
	  socket,
	  "multi-args",
	  1,
	  "two",
	  firstBinary,
	  [secondBinary, "nested"],
	);
	assert.equal(numberValue, 1);
	assert.equal(stringValue, "two");
	assert.deepEqual(Buffer.from(echoedFirst), firstBinary);
	assert.deepEqual(Buffer.from(echoedNested[0]), secondBinary);
	assert.equal(echoedNested[1], "nested");

	const [message] = await new Promise((resolve, reject) => {
	  const timer = setTimeout(() => reject(new Error('timed out waiting for ACK of "message"')), timeoutMs);
	  socket.send("hello through send", (...ackArgs) => {
		clearTimeout(timer);
		resolve(ackArgs);
	  });
	});
	assert.equal(message, "hello through send");
	const [nullMessage] = await new Promise((resolve, reject) => {
	  const timer = setTimeout(() => reject(new Error('timed out waiting for ACK of null "message"')), timeoutMs);
	  socket.send(null, (...ackArgs) => {
		clearTimeout(timer);
		resolve(ackArgs);
	  });
	});
	assert.equal(nullMessage, null);

    const serverAckResult = waitFor(socket, "server-ack-result");
    socket.once("server-ack", (value, ack) => {
      assert.equal(value, "server-value");
      ack("client-value");
    });
    socket.emit("trigger-server-ack");
    const [ackValue] = await serverAckResult;
    assert.equal(ackValue, "client-value");

	const serverBinaryAckResult = waitFor(socket, "server-binary-ack-result");
	socket.once("server-binary-ack", (value, ack) => {
	  assert.deepEqual(Buffer.from(value), Buffer.from([1, 2, 3]));
	  ack(Buffer.from([4, 5, 6]));
	});
	socket.emit("trigger-server-binary-ack");
	const [binaryAckValue] = await serverBinaryAckResult;
	assert.deepEqual(Buffer.from(binaryAckValue), Buffer.from([4, 5, 6]));

    const disconnected = waitFor(socket, "disconnect");
    socket.emit("request-server-disconnect");
    const [disconnectReason] = await disconnected;
    assert.equal(disconnectReason, "io server disconnect", "namespace disconnect must surface the official client reason");
    assert.equal(socket.connected, false);
  } finally {
    await closeSocket(socket);
  }
}

async function checkBufferedNamespaceEventOrder(client) {
  const socket = client.io(`${compatibilityURL}/ordered`, socketOptions({
    transports: ["websocket"],
  }));
  try {
    const result = waitFor(socket, "ordered-result");
    socket.emit("ordered", "a");
    await delay(50);
    socket.emit("ordered", "b");
    const [values] = await result;
    assert.deepEqual(values, ["a", "b"]);
  } finally {
    await closeSocket(socket);
  }
}

async function checkLargePayloads(client) {
  const socket = await connect(client.io, largePayloadURL, {
    transports: ["websocket"],
    perMessageDeflate: false,
  });
  try {
    const jsonPayload = "socket.io-large-json-" + "x".repeat(6_100_000);
    const [jsonEcho] = await emitWithAck(socket, "echo", jsonPayload);
    assert.equal(typeof jsonEcho, "string");
    assert.equal(jsonEcho.length, jsonPayload.length);
    assert.equal(jsonEcho, jsonPayload);

    const binaryPayload = Buffer.alloc(4_700_000);
    for (let index = 0; index < binaryPayload.length; index += 4096) {
      binaryPayload[index] = (index / 4096) & 0xff;
    }
    const [binaryEcho] = await emitWithAck(socket, "binary", binaryPayload);
    assert.ok(Buffer.isBuffer(binaryEcho));
    assert.deepEqual(binaryEcho, binaryPayload);
  } finally {
    await closeSocket(socket);
  }
}

async function checkManualReconnect(client) {
  const socket = client.io(compatibilityURL, socketOptions({
    autoConnect: false,
    transports: ["websocket"],
  }));
  let connectCount = 0;
  socket.on("connect", () => connectCount++);
  try {
	const connected = waitForConnect(socket);
    socket.connect();
	await connected;
    const reconnected = waitFor(socket, "connect");
    socket.io.engine.close();
    await delay(0);
    socket.connect();
    await reconnected;
    await delay(100);
    assert.equal(connectCount, 2, "manual reconnect must emit connect exactly once per connection");
  } finally {
    await closeSocket(socket);
  }
}

async function checkServerRestart(client) {
  const socket = await connect(client.io, lifecycleURL, {
    transports: ["websocket"],
    reconnection: true,
    reconnectionAttempts: 10,
    reconnectionDelay: 100,
    reconnectionDelayMax: 100,
  });
  try {
    const disconnected = waitFor(socket, "disconnect");
    const reconnected = waitFor(socket, "connect");
    const [status] = await emitWithAck(socket, "request-server-restart");
    assert.equal(status, "restarting");
    const [disconnectReason] = await disconnected;
    assert.equal(disconnectReason, "transport close", "server close must surface the official client disconnect reason");
    await reconnected;
    const [echo] = await emitWithAck(socket, "echo", "payload");
    assert.equal(echo, "payload");
  } finally {
    await closeSocket(socket);
  }
}

async function checkReconnectFailedOnce(client) {
  const socket = await connect(client.io, lifecycleURL, {
    transports: ["websocket"],
    reconnection: true,
    reconnectionAttempts: 3,
    reconnectionDelay: 100,
    reconnectionDelayMax: 100,
  });
  let reconnectFailedCount = 0;
  socket.io.on("reconnect_failed", () => reconnectFailedCount++);
  try {
    const failed = waitFor(socket.io, "reconnect_failed");
    const [status] = await emitWithAck(socket, "request-server-shutdown");
    assert.equal(status, "shutting-down");
    await failed;
    await delay(300);
    assert.equal(reconnectFailedCount, 1, "reconnect_failed must be emitted exactly once");
  } finally {
    await closeSocket(socket);
  }
}

async function checkNamespaceAuth(client) {
  const namespaceURL = `${compatibilityURL}/secure`;
  const authOptions = client.major === 2
    ? { query: { token: "matrix-secret", clientMajor: String(client.major) } }
    : { auth: { token: "matrix-secret", clientMajor: String(client.major) } };
  const socket = await connect(client.io, namespaceURL, {
    transports: ["websocket"],
    ...authOptions,
  });
  try {
    const [identity] = await emitWithAck(socket, "identity");
    assert.equal(identity, `v${client.major}-authorized`);
  } finally {
    await closeSocket(socket);
  }

  const rejectedOptions = client.major === 2
    ? { query: { token: "wrong", clientMajor: String(client.major) } }
    : { auth: { token: "wrong", clientMajor: String(client.major) } };
  await expectConnectError(client.io, namespaceURL, {
    transports: ["websocket"],
    ...rejectedOptions,
  }, client.major === 2);
}

async function checkRoomBroadcastSemantics(client) {
  const first = await connect(client.io, compatibilityURL, { transports: ["websocket"] });
  const second = await connect(client.io, compatibilityURL, { transports: ["websocket"] });
  const overlap = await connect(client.io, compatibilityURL, { transports: ["websocket"] });
  const sockets = [first, second, overlap];
  try {
    await Promise.all([
      emitWithAck(first, "join-matrix-rooms", "matrix-room-a", "matrix-excluded"),
      emitWithAck(second, "join-matrix-rooms", "matrix-room-b"),
      emitWithAck(overlap, "join-matrix-rooms", "matrix-room-a", "matrix-room-b"),
    ]);

    const unionCounts = [0, 0, 0];
    sockets.forEach((socket, index) => socket.on("union-broadcast", (value) => {
      assert.equal(value, "union-value");
      unionCounts[index]++;
    }));
    const unionEvents = sockets.map((socket) => waitFor(socket, "union-broadcast"));
    await emitWithAck(first, "trigger-union-broadcast", "union-value");
    await Promise.all(unionEvents);
    await delay(100);
    assert.deepEqual(unionCounts, [1, 1, 1], "room union must deliver once per socket");

    const binaryEvents = sockets.map((socket) => waitFor(socket, "binary-room-broadcast"));
    await emitWithAck(first, "trigger-binary-room-broadcast");
    const binaryPayloads = await Promise.all(binaryEvents);
    binaryPayloads.forEach(([value]) => assert.deepEqual(Buffer.from(value), Buffer.from([1, 2, 3])));

    const exceptCounts = [0, 0, 0];
    sockets.forEach((socket, index) => socket.on("except-broadcast", (value) => {
      assert.equal(value, "except-value");
      exceptCounts[index]++;
    }));
    const secondExcept = waitFor(second, "except-broadcast");
    const overlapExcept = waitFor(overlap, "except-broadcast");
    await emitWithAck(first, "trigger-except-broadcast", "except-value");
    await Promise.all([secondExcept, overlapExcept]);
    await delay(100);
    assert.deepEqual(exceptCounts, [0, 1, 1], "except room must filter matching sockets");

    const socketBroadcastCounts = [0, 0, 0];
    sockets.forEach((socket, index) => socket.on("socket-broadcast", (value) => {
      assert.equal(value, "socket-value");
      socketBroadcastCounts[index]++;
    }));
    const overlapBroadcast = waitFor(overlap, "socket-broadcast");
    await emitWithAck(first, "trigger-socket-broadcast", "socket-value");
    await overlapBroadcast;
    await delay(100);
    assert.deepEqual(socketBroadcastCounts, [0, 0, 1], "socket broadcast must exclude its sender");

    await emitWithAck(overlap, "leave-matrix-room", "matrix-room-a");
    await emitWithAck(first, "trigger-socket-broadcast", "after-leave");
    await delay(150);
    assert.deepEqual(socketBroadcastCounts, [0, 0, 1], "a socket that left the room must not receive later broadcasts");
  } finally {
    await Promise.all(sockets.map(closeSocket));
  }
}

async function checkNamespaceBroadcastIsolation(client) {
  const first = await connect(client.io, compatibilityURL, { transports: ["websocket"] });
  const second = await connect(client.io, compatibilityURL, { transports: ["websocket"] });
  const isolated = await connect(client.io, `${compatibilityURL}/dynamic-messaging`, { transports: ["websocket"] });
  try {
    let isolatedCount = 0;
    isolated.on("namespace-broadcast", () => isolatedCount++);
    const firstEvent = waitFor(first, "namespace-broadcast");
    const secondEvent = waitFor(second, "namespace-broadcast");
    await emitWithAck(first, "trigger-namespace-broadcast", Buffer.from([4, 5, 6]));
    const [[firstValue], [secondValue]] = await Promise.all([firstEvent, secondEvent]);
    assert.deepEqual(Buffer.from(firstValue), Buffer.from([4, 5, 6]));
    assert.deepEqual(Buffer.from(secondValue), Buffer.from([4, 5, 6]));
    await delay(100);
    assert.equal(isolatedCount, 0, "namespace broadcast must not leak into another namespace");
  } finally {
    await Promise.all([first, second, isolated].map(closeSocket));
  }
}

// The official uws.ts file repeats core Socket.IO behavior against the
// Node-specific uWebSockets.js engine. Go cannot expose attachApp(), but its
// deployment engine must satisfy the same observable contract over Polling,
// WebSocket and an upgrading connection.
async function checkDeploymentEngineParity(client) {
  const defaultSocket = await connect(client.io, compatibilityURL, {
    transports: ["polling", "websocket"],
  });
  const websocketOnly = await connect(client.io, compatibilityURL, {
    transports: ["websocket"],
  });
  const pollingOnly = await connect(client.io, compatibilityURL, {
    transports: ["polling"],
    upgrade: false,
  });
  const custom = await connect(client.io, `${compatibilityURL}/custom`, {
    transports: ["polling", "websocket"],
  });
  const rootSockets = [defaultSocket, websocketOnly, pollingOnly];
  let dynamic;

  try {
    assert.equal(websocketOnly.io.engine.transport.name, "websocket");
    assert.equal(pollingOnly.io.engine.transport.name, "polling");

    let customLeakCount = 0;
    custom.on("deployment-broadcast", () => customLeakCount++);
    const broadcastEvents = rootSockets.map((socket) => waitFor(socket, "deployment-broadcast"));
    await emitWithAck(defaultSocket, "deployment-trigger-broadcast", "hello");
    const broadcastPayloads = await Promise.all(broadcastEvents);
    assert.deepEqual(broadcastPayloads, [["hello"], ["hello"], ["hello"]]);
    await delay(50);
    assert.equal(customLeakCount, 0, "default namespace broadcast leaked into /custom");

    const namespaceLeakCounts = [0, 0, 0];
    rootSockets.forEach((socket, index) => {
      socket.on("deployment-namespace-broadcast", () => namespaceLeakCounts[index]++);
    });
    const customEvent = waitFor(custom, "deployment-namespace-broadcast");
    await emitWithAck(custom, "deployment-trigger-namespace-broadcast", "custom");
    assert.deepEqual(await customEvent, ["custom"]);
    await delay(50);
    assert.deepEqual(namespaceLeakCounts, [0, 0, 0], "custom namespace broadcast leaked into /");

    dynamic = await connect(client.io, `${compatibilityURL}/dynamic-101`, {
      transports: ["websocket"],
    });
    const dynamicEvent = waitFor(dynamic, "dynamic-broadcast");
    await emitWithAck(dynamic, "trigger-dynamic-broadcast", "dynamic");
    assert.deepEqual(await dynamicEvent, ["dynamic"]);

    const binaryEvents = rootSockets.map((socket) => waitFor(socket, "deployment-broadcast"));
    await emitWithAck(defaultSocket, "deployment-trigger-broadcast", Buffer.from([1, 2, 3]));
    for (const [payload] of await Promise.all(binaryEvents)) {
      assert.deepEqual(Buffer.from(payload), Buffer.from([1, 2, 3]));
    }

    await delay(20);
    const volatileEvents = rootSockets.map((socket) => waitFor(socket, "deployment-volatile-binary"));
    await emitWithAck(defaultSocket, "deployment-trigger-volatile-binary");
    for (const [payload] of await Promise.all(volatileEvents)) {
      assert.deepEqual(Buffer.from(payload), Buffer.from([1, 2, 3]));
    }

    await Promise.all([
      emitWithAck(websocketOnly, "join-matrix-rooms", "deployment-room-1"),
      emitWithAck(pollingOnly, "join-matrix-rooms", "deployment-room-1"),
    ]);
    let defaultRoomCount = 0;
    defaultSocket.on("deployment-room-broadcast", () => defaultRoomCount++);
    const roomEvents = [websocketOnly, pollingOnly].map((socket) => waitFor(socket, "deployment-room-broadcast"));
    await emitWithAck(defaultSocket, "deployment-trigger-room-broadcast");
    await Promise.all(roomEvents);
    await delay(50);
    assert.equal(defaultRoomCount, 0, "room broadcast reached a socket outside the room");

    await Promise.all([
      emitWithAck(websocketOnly, "join-matrix-rooms", "deployment-room-a"),
      emitWithAck(pollingOnly, "join-matrix-rooms", "deployment-room-b"),
    ]);
    const multipleRoomEvents = [websocketOnly, pollingOnly].map((socket) => waitFor(socket, "deployment-multiple-rooms"));
    await emitWithAck(defaultSocket, "deployment-trigger-multiple-rooms");
    await Promise.all(multipleRoomEvents);

    await emitWithAck(pollingOnly, "join-matrix-rooms", "deployment-excluded-room");
    let excludedCount = 0;
    pollingOnly.on("deployment-except-room", () => excludedCount++);
    const exceptEvents = [defaultSocket, websocketOnly].map((socket) => waitFor(socket, "deployment-except-room"));
    await emitWithAck(defaultSocket, "deployment-trigger-except-room");
    await Promise.all(exceptEvents);
    await delay(50);
    assert.equal(excludedCount, 0, "except(room) reached an excluded socket");

    const middlewareRoomEvents = rootSockets.map((socket) => waitFor(socket, "deployment-middleware-room"));
    await emitWithAck(defaultSocket, "deployment-trigger-middleware-room");
    await Promise.all(middlewareRoomEvents);

    await Promise.all(rootSockets.map((socket) => emitWithAck(socket, "join-matrix-rooms", "deployment-leave-room")));
    await emitWithAck(websocketOnly, "leave-matrix-room", "deployment-leave-room");
    let departedCount = 0;
    websocketOnly.on("deployment-after-leave", () => departedCount++);
    const afterLeaveEvents = [defaultSocket, pollingOnly].map((socket) => waitFor(socket, "deployment-after-leave"));
    await emitWithAck(defaultSocket, "deployment-trigger-after-leave");
    await Promise.all(afterLeaveEvents);
    await delay(50);
    assert.equal(departedCount, 0, "socket received a broadcast after leaving the room");

    const disconnecting = await connect(client.io, compatibilityURL, {
      transports: ["polling", "websocket"],
    });
    try {
      const disconnected = waitFor(disconnecting, "disconnect");
      disconnecting.emit("request-server-disconnect");
      await disconnected;
    } finally {
      await closeSocket(disconnecting);
    }

    const asset = await httpGet(`${compatibilityURL}/socket.io/socket.io.js`);
    assert.equal(asset.statusCode, 200);
    assert.equal(asset.headers["content-type"], "application/javascript; charset=utf-8");
    assert.match(asset.headers.etag, /^"[^"]+"$/);
    assert.equal(asset.headers["x-sourcemap"], undefined);
    assert.match(asset.body.toString("utf8"), /engine\.io/);
  } finally {
    if (dynamic) await closeSocket(dynamic);
    await Promise.all([...rootSockets, custom].map(closeSocket));
  }
}

async function checkBroadcastAcknowledgements(client) {
  const first = await connect(client.io, compatibilityURL, { transports: ["websocket"] });
  const second = await connect(client.io, compatibilityURL, { transports: ["websocket"] });
  const third = await connect(client.io, compatibilityURL, { transports: ["websocket"] });
  const sockets = [first, second, third];
  try {
    first.on("broadcast-ack-request", (_mode, callback) => callback(1));
    second.on("broadcast-ack-request", (_mode, callback) => callback(2));
    third.on("broadcast-ack-request", (mode, callback) => {
      if (mode !== "timeout") callback(3);
    });

    const [successError, successResponses] = await emitWithAck(first, "trigger-broadcast-ack", "success");
    assert.equal(successError, false);
    assert.deepEqual([...successResponses].sort(), [1, 2, 3]);

    const [timeoutError, timeoutResponses] = await emitWithAck(first, "trigger-broadcast-ack", "timeout");
    assert.equal(timeoutError, true);
    assert.deepEqual([...timeoutResponses].sort(), [1, 2]);

    const [emptyError, emptyResponses] = await emitWithAck(first, "trigger-broadcast-ack", "zero-clients");
    assert.equal(emptyError, false);
    assert.deepEqual(emptyResponses, []);
  } finally {
    await Promise.all(sockets.map(closeSocket));
  }
}

async function checkSocketMiddleware(client) {
  const socket = await connect(client.io, compatibilityURL, {
    transports: ["websocket"],
  });
  try {
    const [mutated] = await emitWithAck(socket, "middleware-mutate", "original");
    assert.equal(mutated, "modified-by-middleware");

    const middlewareError = waitFor(socket, "middleware-error-observed");
    socket.emit("middleware-blocked", "must-not-reach-handler");
    const [message] = await middlewareError;
    assert.equal(message, "blocked by socket middleware");
  } finally {
    await closeSocket(socket);
  }
}

async function checkDisconnectDuringNamespaceMiddleware(client) {
  const observer = await connect(client.io, compatibilityURL, { transports: ["websocket"] });
  const socket = client.io(`${compatibilityURL}/slow`, socketOptions({
    autoConnect: false,
    transports: ["websocket"],
  }));
  try {
    const engineOpened = waitFor(socket.io, "open");
    socket.connect();
    await engineOpened;
    await closeSocket(socket);
    await delay(250);
    const [connections] = await emitWithAck(observer, "slow-namespace-connections");
    assert.equal(connections, 0, "closed client must not connect after asynchronous middleware completes");
  } finally {
    await closeSocket(socket);
    await closeSocket(observer);
  }
}

async function checkDynamicNamespaces(client) {
  const firstURL = `${compatibilityURL}/dynamic-alpha`;
  const secondURL = `${compatibilityURL}/dynamic-beta`;
  const first = await connect(client.io, firstURL, { transports: ["websocket"] });
  const sameChild = await connect(client.io, firstURL, { transports: ["websocket"] });
  const otherChild = await connect(client.io, secondURL, { transports: ["websocket"] });
  try {
    const identities = await Promise.all([
      emitWithAck(first, "dynamic-identity"),
      emitWithAck(sameChild, "dynamic-identity"),
      emitWithAck(otherChild, "dynamic-identity"),
    ]);
    assert.deepEqual(identities, [["/dynamic-alpha"], ["/dynamic-alpha"], ["/dynamic-beta"]]);

    const counts = [0, 0, 0];
    [first, sameChild, otherChild].forEach((socket, index) => {
      socket.on("dynamic-broadcast", (value) => {
        assert.equal(value, "dynamic-value");
        counts[index]++;
      });
    });
    const firstEvent = waitFor(first, "dynamic-broadcast");
    const sameChildEvent = waitFor(sameChild, "dynamic-broadcast");
    await emitWithAck(first, "trigger-dynamic-broadcast", "dynamic-value");
    await Promise.all([firstEvent, sameChildEvent]);
    await delay(100);
    assert.deepEqual(counts, [1, 1, 0], "dynamic namespaces must be isolated from each other");
  } finally {
    await Promise.all([first, sameChild, otherChild].map(closeSocket));
  }

  await expectConnectError(client.io, `${compatibilityURL}/not-dynamic`, {
    transports: ["websocket"],
  }, client.major === 2);
}

async function waitForNamespacePresence(observer, namespace, expected) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const [present] = await emitWithAck(observer, "namespace-present", namespace);
    if (present === expected) {
      return;
    }
    await delay(20);
  }
  throw new Error(`timed out waiting for namespace ${namespace} presence=${expected}`);
}

async function checkDynamicNamespaceCleanup(client) {
  const observer = await connect(client.io, cleanupURL, { transports: ["websocket"] });
  let child = await connect(client.io, `${cleanupURL}/dynamic-cleanup`, { transports: ["websocket"] });
  try {
    await waitForNamespacePresence(observer, "/dynamic-cleanup", true);
    await closeSocket(child);
    child = null;
    await waitForNamespacePresence(observer, "/dynamic-cleanup", false);

    child = await connect(client.io, `${cleanupURL}/dynamic-cleanup`, { transports: ["websocket"] });
    await waitForNamespacePresence(observer, "/dynamic-cleanup", true);
  } finally {
    if (child) await closeSocket(child);
    await closeSocket(observer);
  }

  const persistentObserver = await connect(client.io, compatibilityURL, { transports: ["websocket"] });
  const persistentChild = await connect(client.io, `${compatibilityURL}/dynamic-persistent`, { transports: ["websocket"] });
  try {
    await closeSocket(persistentChild);
    await delay(50);
    const [present] = await emitWithAck(persistentObserver, "namespace-present", "/dynamic-persistent");
    assert.equal(present, true, "dynamic namespace must remain registered when cleanup is disabled");
  } finally {
    await closeSocket(persistentChild);
    await closeSocket(persistentObserver);
  }
}

async function checkAckLifecycle(client) {
  const socket = await connect(client.io, compatibilityURL, {
    transports: ["websocket"],
  });
  try {
    socket.on("ack-fast", (value, ack) => {
      assert.equal(value, "fast-value");
      ack("client-fast", 3, true);
    });
    socket.on("ack-never", (value, ack) => {
      assert.equal(value, "late-value");
      setTimeout(() => ack("late-response"), 200);
    });
    socket.on("ack-zero", (value, ack) => {
      assert.equal(value, "zero-value");
      ack("too-late-for-zero-timeout");
    });

    const fastResult = waitFor(socket, "ack-fast-result");
    const timeoutResult = waitFor(socket, "ack-timeout-result");
    const finalCount = waitFor(socket, "ack-timeout-final-count");
    const zeroTimeoutResult = waitFor(socket, "ack-zero-result");
    const zeroFinalCount = waitFor(socket, "ack-zero-final-count");
    await emitWithAck(socket, "trigger-ack-lifecycle");

    const [fastArgs, fastHadError] = await fastResult;
    assert.deepEqual(fastArgs, ["client-fast", 3, true]);
    assert.equal(fastHadError, false);

    const [timeoutArgs, timeoutHadError, timeoutCallbacks] = await timeoutResult;
    assert.deepEqual(timeoutArgs, []);
    assert.equal(timeoutHadError, true);
    assert.equal(timeoutCallbacks, 1);

    const [callbacksAfterLateAck] = await finalCount;
    assert.equal(callbacksAfterLateAck, 1, "late ACK must not call the callback again");

    const [zeroArgs, zeroHadError, zeroCallbacks] = await zeroTimeoutResult;
    assert.deepEqual(zeroArgs, []);
    assert.equal(zeroHadError, true);
    assert.equal(zeroCallbacks, 1);
    const [zeroCallbacksAfterAck] = await zeroFinalCount;
    assert.equal(zeroCallbacksAfterAck, 1, "0ms timeout callback must run exactly once");
  } finally {
    await closeSocket(socket);
  }
}

async function checkAnyListeners(client) {
  const socket = await connect(client.io, compatibilityURL, {
    transports: ["websocket"],
  });
  try {
    const incomingAck = await emitWithAck(socket, "any-incoming", "client-value");
    assert.deepEqual(incomingAck, ["first", 2, true]);

    socket.on("any-outgoing", (value, ack) => {
      assert.equal(value, "server-value");
      ack("client-ack", 7);
    });
    const outgoingAckObserved = waitFor(socket, "any-outgoing-ack-observed");
    await emitWithAck(socket, "trigger-any-outgoing");
    assert.deepEqual(await outgoingAckObserved, ["client-ack", 7]);

    const [incomingEvents, outgoingEvents] = await emitWithAck(socket, "any-observer-snapshot");
    assert.deepEqual(incomingEvents, [
      "any-incoming",
      "trigger-any-outgoing",
      "any-observer-snapshot",
    ]);
    assert.deepEqual(outgoingEvents, ["any-outgoing", "any-outgoing-ack-observed"]);
  } finally {
    await closeSocket(socket);
  }
}

async function checkConnectionStateRecovery(client) {
  const socket = client.io(recoveryURL, socketOptions({
    transports: ["websocket"],
    reconnection: true,
    reconnectionDelay: 100,
    reconnectionDelayMax: 100,
  }));
  try {
    const ready = waitFor(socket, "recovery-ready");
    await waitFor(socket, "connect");
    const [readyValue, initiallyRecovered, initialData, initialRoom] = await ready;
    assert.equal(readyValue, "ready");
    assert.equal(initiallyRecovered, false);
    assert.equal(initialData, "persisted-socket-data");
    assert.equal(initialRoom, true);
    const [initialStateRecovered, initialStateData, initialStateRoom, initialMiddlewareCalls] =
      await emitWithAck(socket, "recovery-state");
    assert.equal(initialStateRecovered, false);
    assert.equal(initialStateData, "persisted-socket-data");
    assert.equal(initialStateRoom, true);
    const originalId = socket.id;

    const disconnected = waitFor(socket, "disconnect");
    const reconnected = waitFor(socket, "connect");
    const missed = waitFor(socket, "missed-event");
    const recoveredReady = waitFor(socket, "recovery-ready");
    const [closingMessage] = await emitWithAck(socket, "begin-recovery");
    assert.equal(closingMessage, "closing transport");
    await disconnected;

    const [[missedValue], , recoveredReadyArgs] = await Promise.all([missed, reconnected, recoveredReady]);
    assert.equal(missedValue, "stored while disconnected");
    assert.equal(socket.id, originalId);
    assert.equal(socket.recovered, true);
    assert.deepEqual(recoveredReadyArgs.slice(0, 4), [
      "ready",
      true,
      "persisted-socket-data",
      true,
    ]);
    const [recoveredState, recoveredData, recoveredRoom, recoveredMiddlewareCalls] =
      await emitWithAck(socket, "recovery-state");
    assert.equal(recoveredState, true);
    assert.equal(recoveredData, "persisted-socket-data");
    assert.equal(recoveredRoom, true);
    assert.equal(
      recoveredMiddlewareCalls,
      initialMiddlewareCalls,
      "namespace middleware must be skipped for a recovered connection by default",
    );
  } finally {
    await closeSocket(socket);
  }
}

async function checkConnectionStateRecoveryWithMiddleware(client) {
  const socket = client.io(recoveryWithMiddlewareURL, socketOptions({
    transports: ["websocket"],
    reconnection: true,
    reconnectionDelay: 100,
    reconnectionDelayMax: 100,
  }));
  try {
    const ready = waitFor(socket, "recovery-ready");
    await waitFor(socket, "connect");
    await ready;
    const [, , , middlewareCallsBefore] = await emitWithAck(socket, "recovery-state");

    const disconnected = waitFor(socket, "disconnect");
    const reconnected = waitFor(socket, "connect");
    const recoveredReady = waitFor(socket, "recovery-ready");
    await emitWithAck(socket, "begin-recovery");
    await disconnected;
    await Promise.all([reconnected, recoveredReady]);

    const [recovered, , , middlewareCallsAfter] = await emitWithAck(socket, "recovery-state");
    assert.equal(recovered, true);
    assert.equal(
      middlewareCallsAfter,
      middlewareCallsBefore + 1,
      "namespace middleware must run again when skipMiddlewares is false",
    );
  } finally {
    await closeSocket(socket);
  }
}

async function checkUnknownRecoverySession(client) {
  const socket = client.io(recoveryURL, socketOptions({
    autoConnect: false,
    transports: ["websocket"],
  }));
  try {
    socket._pid = "unknown-session";
    socket._lastOffset = "unknown-offset";
    const connected = waitFor(socket, "connect");
    const ready = waitFor(socket, "recovery-ready");
    socket.connect();
    await connected;
    const [, recovered] = await ready;
    assert.equal(Boolean(socket.recovered), false);
    assert.equal(recovered, false);
  } finally {
    await closeSocket(socket);
  }
}

async function checkRecoveryDisabledByDefault(client) {
  const socket = client.io(compatibilityURL, socketOptions({
    autoConnect: false,
    transports: ["websocket"],
  }));
  try {
    socket._pid = "unknown-session";
    socket._lastOffset = "unknown-offset";
    const connected = waitFor(socket, "connect");
    socket.connect();
    await connected;
    assert.equal(Boolean(socket.recovered), false);
    assert.equal(socket._pid, undefined, "server without recovery must not issue a private session ID");
  } finally {
    await closeSocket(socket);
  }
}

async function checkUnrecoverableNamespaceDisconnect(client) {
  const source = client.io(recoveryURL, socketOptions({
    autoConnect: false,
    transports: ["websocket"],
  }));
  let replacement;
  try {
    const sourceConnected = waitFor(source, "connect");
    const sourceReady = waitFor(source, "recovery-ready");
    source.connect();
    await sourceConnected;
    await sourceReady;
    const original = {
      id: source.id,
      pid: source._pid,
      offset: source._lastOffset,
    };
    const [, , , middlewareCallsBefore] = await emitWithAck(source, "recovery-state");
    assert.ok(original.pid && original.offset, "source client must have recovery credentials");

    const disconnected = waitFor(source, "disconnect");
    const [message] = await emitWithAck(source, "prepare-unrecoverable-disconnect");
    assert.equal(message, "disconnecting namespace");
    const [reason] = await disconnected;
    assert.equal(reason, "io server disconnect");

    replacement = client.io(recoveryURL, socketOptions({
      autoConnect: false,
      transports: ["websocket"],
    }));
    replacement._pid = original.pid;
    replacement._lastOffset = original.offset;
    const connected = waitFor(replacement, "connect");
    const ready = waitFor(replacement, "recovery-ready");
    replacement.connect();
    await connected;
    const readyArgs = await ready;

    assert.equal(replacement.recovered, false);
    assert.notEqual(replacement.id, original.id);
    assert.deepEqual(readyArgs.slice(0, 4), [
      "ready",
      false,
      "persisted-socket-data",
      true,
    ]);
    const [, , , middlewareCallsAfter] = await emitWithAck(replacement, "recovery-state");
    assert.equal(
      middlewareCallsAfter,
      middlewareCallsBefore + 1,
      "namespace middleware must run for a new session after server disconnect",
    );
  } finally {
    await closeSocket(source);
    if (replacement) await closeSocket(replacement);
  }
}

async function runClient(client) {
  if (process.env.SOCKET_IO_MATRIX_ONLY === "upgrade") {
    const repetitions = Number(process.env.SOCKET_IO_MATRIX_REPETITIONS || 20);
    for (let index = 0; index < repetitions; index++) {
      console.error(`[matrix] v${client.major}: upgrade repetition ${index + 1}/${repetitions}`);
      await checkTransport(client, ["polling", "websocket"], "websocket", true);
    }
    return;
  }

  if (process.env.SOCKET_IO_MATRIX_ONLY === "namespace-auth") {
    const repetitions = Number(process.env.SOCKET_IO_MATRIX_REPETITIONS || 20);
    for (let index = 0; index < repetitions; index++) {
      console.error(`[matrix] v${client.major}: namespace auth repetition ${index + 1}/${repetitions}`);
      await checkNamespaceAuth(client);
    }
    return;
  }

  if (process.env.SOCKET_IO_MATRIX_ONLY === "features") {
    const repetitions = Number(process.env.SOCKET_IO_MATRIX_REPETITIONS || 20);
    for (let index = 0; index < repetitions; index++) {
      console.error(`[matrix] v${client.major}: features repetition ${index + 1}/${repetitions}`);
      await checkFeatures(client);
    }
    return;
  }

  console.error(`[matrix] v${client.major}: strict EIO policy`);
  if (client.major === 2) {
    await expectConnectError(client.io, strictURL, { transports: ["polling"] });
  } else {
    const strictSocket = await connect(client.io, strictURL, { transports: ["polling"], upgrade: false });
    await closeSocket(strictSocket);
  }

  console.error(`[matrix] v${client.major}: polling`);
  await checkTransport(client, ["polling"], "polling", false);
  console.error(`[matrix] v${client.major}: websocket`);
  await checkTransport(client, ["websocket"], "websocket", false);
  console.error(`[matrix] v${client.major}: polling to websocket upgrade`);
  await checkTransport(client, ["polling", "websocket"], "websocket", true);
  console.error(`[matrix] v${client.major}: events, ACK, binary, disconnect`);
  await checkFeatures(client);
	console.error(`[matrix] v${client.major}: buffered namespace event order`);
	await checkBufferedNamespaceEventOrder(client);
  console.error(`[matrix] v${client.major}: namespace auth`);
  await checkNamespaceAuth(client);
  console.error(`[matrix] v${client.major}: room broadcast semantics`);
  await checkRoomBroadcastSemantics(client);
  console.error(`[matrix] v${client.major}: namespace broadcast isolation`);
  await checkNamespaceBroadcastIsolation(client);
  console.error(`[matrix] v${client.major}: Go deployment engine parity with official uws.ts behavior`);
  await checkDeploymentEngineParity(client);
  console.error(`[matrix] v${client.major}: broadcast acknowledgements`);
  await checkBroadcastAcknowledgements(client);
  console.error(`[matrix] v${client.major}: socket middleware`);
  await checkSocketMiddleware(client);
	console.error(`[matrix] v${client.major}: disconnect during namespace middleware`);
	await checkDisconnectDuringNamespaceMiddleware(client);
	console.error(`[matrix] v${client.major}: ACK lifecycle and timeout race`);
	await checkAckLifecycle(client);
	console.error(`[matrix] v${client.major}: server catch-all listeners`);
	await checkAnyListeners(client);
	console.error(`[matrix] v${client.major}: dynamic namespaces`);
	await checkDynamicNamespaces(client);
  console.error(`[matrix] v${client.major}: dynamic namespace cleanup`);
	await checkDynamicNamespaceCleanup(client);
	console.error(`[matrix] v${client.major}: manual reconnect event count`);
	await checkManualReconnect(client);
	console.error(`[matrix] v${client.major}: emit after server close and restart`);
	await checkServerRestart(client);
	console.error(`[matrix] v${client.major}: reconnect_failed event count`);
	await checkReconnectFailedOnce(client);

  if (client.major === 4) {
	console.error("[matrix] v4: very large JSON and binary round trips");
	await checkLargePayloads(client);
    console.error("[matrix] v4: connection state recovery");
    await checkConnectionStateRecovery(client);
    console.error("[matrix] v4: connection state recovery with middleware");
    await checkConnectionStateRecoveryWithMiddleware(client);
    console.error("[matrix] v4: unknown recovery session");
    await checkUnknownRecoverySession(client);
    console.error("[matrix] v4: connection state recovery disabled by default");
    await checkRecoveryDisabledByDefault(client);
    console.error("[matrix] v4: unrecoverable namespace disconnect");
    await checkUnrecoverableNamespaceDisconnect(client);
  }

  results.push({
    client: `socket.io-client@${client.major}`,
    eio: client.major === 2 ? 3 : 4,
    polling: "pass",
    websocket: "pass",
    upgrade: "pass",
    eventsAndAck: "pass",
    binary: "pass",
	utf8AndMultipleArguments: "pass",
	messageSend: "pass",
    namespaceAuth: "pass",
	roomBroadcastSemantics: "pass",
	namespaceBroadcastIsolation: "pass",
	deploymentEngineParity: "pass",
	broadcastAcknowledgements: "pass",
	socketMiddleware: "pass",
	disconnectDuringNamespaceMiddleware: "pass",
	dynamicNamespaces: "pass",
	dynamicNamespaceCleanup: "pass",
	ackLifecycle: "pass",
	catchAllListeners: "pass",
	largePayloads: client.major === 4 ? "pass" : "not-applicable",
	manualReconnect: "pass",
	serverRestart: "pass",
	reconnectFailedOnce: "pass",
	connectionStateRecovery: client.major === 4 ? "pass" : "not-applicable",
	recoveryMiddlewareModes: client.major === 4 ? "pass" : "not-applicable",
	unknownRecoverySession: client.major === 4 ? "pass" : "not-applicable",
	recoveryDisabledByDefault: client.major === 4 ? "pass" : "not-applicable",
	unrecoverableDisconnect: client.major === 4 ? "pass" : "not-applicable",
    strictRejection: client.major === 2 ? "pass" : "not-applicable",
  });
}

(async () => {
  for (const client of clients) {
    await runClient(client);
  }
  await new Promise((resolve) => {
    process.stdout.write(`${JSON.stringify(results, null, 2)}\n`, resolve);
  });
  process.exit(0);
})().catch((error) => {
  console.error(error);
  process.exit(1);
});
