"use strict";

const assert = require("node:assert/strict");
const { io } = require("socket.io-client");
const adminUIPackage = require("@socket.io/admin-ui/package.json");

const officialGitHead = "232d87af04777b108725bd70c248b5e929a5057d";
assert.equal(adminUIPackage.version, "0.5.1");

const endpoints = JSON.parse(
  process.env.SOCKET_IO_ADMIN_UI_ENDPOINTS || "null",
);
if (
  !endpoints ||
  !endpoints.main ||
  !endpoints.custom ||
  !endpoints.auth ||
  !endpoints.readOnly ||
  !endpoints.production
) {
  throw new Error("SOCKET_IO_ADMIN_UI_ENDPOINTS is incomplete");
}

const timeoutMs = 7_500;
const verifiedCases = new Set();
const wait = (duration) =>
  new Promise((resolve) => setTimeout(resolve, duration));

function createSocket(baseUrl, namespace, options = {}) {
  return io(`${baseUrl}${namespace}`, {
    autoConnect: false,
    forceNew: true,
    reconnection: false,
    transports: ["websocket"],
    timeout: timeoutMs,
    ...options,
  });
}

function once(socket, event, label = event, timeout = timeoutMs) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.off(event, listener);
      reject(new Error(`timeout: ${label}`));
    }, timeout);
    const listener = (...args) => {
      clearTimeout(timer);
      resolve(args);
    };
    socket.once(event, listener);
  });
}

function onceMatching(socket, event, label, predicate, timeout = timeoutMs) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.off(event, listener);
      reject(new Error(`timeout: ${label}`));
    }, timeout);
    const listener = (...args) => {
      if (!predicate(...args)) return;
      clearTimeout(timer);
      socket.off(event, listener);
      resolve(args);
    };
    socket.on(event, listener);
  });
}

async function connect(baseUrl, namespace, options = {}) {
  const socket = createSocket(baseUrl, namespace, options);
  await new Promise((resolve, reject) => {
    const cleanup = () => {
      clearTimeout(timer);
      socket.off("connect", onConnect);
      socket.off("connect_error", onConnectError);
    };
    const onConnect = () => {
      cleanup();
      resolve();
    };
    const onConnectError = (error) => {
      cleanup();
      reject(error);
    };
    const timer = setTimeout(() => {
      cleanup();
      reject(new Error(`timeout: ${namespace} connect`));
    }, timeoutMs);
    socket.once("connect", onConnect);
    socket.once("connect_error", onConnectError);
    socket.connect();
  });
  return socket;
}

async function expectConnectError(baseUrl, namespace, auth) {
  const socket = createSocket(baseUrl, namespace, { auth });
  try {
    const failed = once(socket, "connect_error", `${namespace} rejection`);
    socket.connect();
    const [error] = await failed;
    assert.equal(error.message, "invalid credentials");
  } finally {
    socket.disconnect();
  }
}

function emitAck(socket, event, ...args) {
  return new Promise((resolve, reject) => {
    socket.timeout(timeoutMs).emit(event, ...args, (error, ...values) => {
      if (error) {
        reject(error);
      } else if (values.length <= 1) {
        resolve(values[0]);
      } else {
        resolve(values);
      }
    });
  });
}

function assertISOString(value) {
  assert.equal(typeof value, "string");
  assert.equal(new Date(value).toISOString(), value);
}

async function close(socket) {
  if (!socket) return;
  const engine = socket.io?.engine;
  if (!engine || engine.readyState === "closed") {
    socket.disconnect();
    return;
  }
  const closed = once(engine, "close", "Engine.IO close");
  socket.disconnect();
  await closed;
}

async function verifyCustomNamespace() {
  const socket = await connect(endpoints.custom, "/custom");
  assert.equal(socket.nsp, "/custom");
  verifiedCases.add(2);
  await close(socket);
}

async function verifyAuthentication() {
  await expectConnectError(endpoints.auth, "/admin", undefined);
  verifiedCases.add(4);

  const authenticated = createSocket(endpoints.auth, "/admin", {
    auth: { username: "admin", password: "secret" },
  });
  const session = once(authenticated, "session", "authenticated session");
  const connected = once(authenticated, "connect", "authenticated connect");
  authenticated.connect();
  const [[sessionId]] = await Promise.all([session, connected]);
  assert.match(sessionId, /^[0-9a-f]{16}$/);
  verifiedCases.add(5);
  await close(authenticated);

  const resumed = await connect(endpoints.auth, "/admin", {
    auth: { sessionId },
  });
  assert.equal(resumed.connected, true);
  verifiedCases.add(6);
  await close(resumed);

  await expectConnectError(endpoints.auth, "/admin", {
    sessionId: "1234",
  });
  verifiedCases.add(7);
}

async function verifyFeatureModes() {
  const readOnly = createSocket(endpoints.readOnly, "/admin");
  const readOnlyConfig = once(readOnly, "config", "readonly config");
  const readOnlyConnected = once(readOnly, "connect", "readonly connect");
  readOnly.connect();
  const [[readOnlyValue]] = await Promise.all([
    readOnlyConfig,
    readOnlyConnected,
  ]);
  assert.deepEqual(readOnlyValue.supportedFeatures, [
    "AGGREGATED_EVENTS",
    "ALL_EVENTS",
  ]);
  verifiedCases.add(9);
  await close(readOnly);

  const production = createSocket(endpoints.production, "/admin");
  const productionConfig = once(production, "config", "production config");
  const productionConnected = once(
    production,
    "connect",
    "production connect",
  );
  production.connect();
  const [[productionValue]] = await Promise.all([
    productionConfig,
    productionConnected,
  ]);
  assert.deepEqual(productionValue.supportedFeatures, ["AGGREGATED_EVENTS"]);
  verifiedCases.add(10);
  await close(production);
}

async function connectMainAdminWithExistingSocket() {
  const application = await connect(endpoints.main, "/", {
    query: { officialCase: "11" },
    extraHeaders: { "x-admin-ui-case": "all-sockets" },
  });
  const admin = createSocket(endpoints.main, "/admin");
  const config = once(admin, "config", "main config");
  const allSockets = once(admin, "all_sockets", "all_sockets");
  const connected = once(admin, "connect", "main admin connect");
  admin.connect();
  const [[configValue], [sockets]] = await Promise.all([
    config,
    allSockets,
    connected,
  ]);

  assert.deepEqual(configValue.supportedFeatures, [
    "EMIT",
    "JOIN",
    "LEAVE",
    "DISCONNECT",
    "MJOIN",
    "MLEAVE",
    "MDISCONNECT",
    "AGGREGATED_EVENTS",
    "ALL_EVENTS",
  ]);
  verifiedCases.add(1);
  verifiedCases.add(8);

  assert.equal(sockets.length, 2);
  const applicationDetails = sockets.find((socket) => socket.nsp === "/");
  const adminDetails = sockets.find((socket) => socket.nsp === "/admin");
  assert.equal(applicationDetails.id, application.id);
  assert.equal(adminDetails.id, admin.id);
  verifiedCases.add(11);
  return { admin, application };
}

async function verifyAdministrativeLifecycle(admin) {
  const connectedEvent = onceMatching(
    admin,
    "socket_connected",
    "lifecycle socket_connected",
    (socket) => socket?.nsp === "/lifecycle",
  );
  const initialJoin = onceMatching(
    admin,
    "room_joined",
    "lifecycle initial room_joined",
    (nsp, room, id) => nsp === "/lifecycle" && room === id,
  );
  const socket = await connect(endpoints.main, "/lifecycle", {
    query: { officialCase: "12" },
    extraHeaders: { "x-admin-ui-case": "lifecycle" },
  });
  const socketId = socket.id;
  const [serialized, connectedAt] = await connectedEvent;
  assert.equal(serialized.id, socketId);
  assert.equal(serialized.nsp, "/lifecycle");
  assert.equal(serialized.transport, "websocket");
  assert.ok(serialized.rooms.includes(socketId));
  assert.deepEqual(Object.keys(serialized.handshake).sort(), [
    "address",
    "headers",
    "issued",
    "query",
    "secure",
    "time",
    "url",
    "xdomain",
  ]);
  assert.equal(serialized.handshake.query.officialCase, "12");
  assert.equal(serialized.handshake.secure, false);
  assertISOString(connectedAt);
  const initialJoinArgs = await initialJoin;
  assert.equal(initialJoinArgs[2], socketId);
  assertISOString(initialJoinArgs[3]);

  const joined = onceMatching(
    admin,
    "room_joined",
    "lifecycle room1 joined",
    (nsp, room, id) =>
      nsp === "/lifecycle" && room === "room1" && id === socketId,
  );
  assert.equal(await emitAck(socket, "lifecycle-join", "room1"), "joined");
  const joinedArgs = await joined;
  assertISOString(joinedArgs[3]);

  const left = onceMatching(
    admin,
    "room_left",
    "lifecycle room1 left",
    (nsp, room, id) =>
      nsp === "/lifecycle" && room === "room1" && id === socketId,
  );
  assert.equal(await emitAck(socket, "lifecycle-leave", "room1"), "left");
  const leftArgs = await left;
  assertISOString(leftArgs[3]);

  const disconnected = onceMatching(
    admin,
    "socket_disconnected",
    "lifecycle socket_disconnected",
    (nsp, id) => nsp === "/lifecycle" && id === socketId,
  );
  socket.emit("lifecycle-disconnect");
  const disconnectedArgs = await disconnected;
  assertISOString(disconnectedArgs[3]);
  verifiedCases.add(12);
  await close(socket);
}

async function verifySocketDataUpdates(admin) {
  const connectedEvent = onceMatching(
    admin,
    "socket_connected",
    "data socket_connected",
    (socket) => socket?.nsp === "/data",
  );
  const socket = await connect(endpoints.main, "/data", {
    transports: ["polling"],
  });
  const [initial] = await connectedEvent;
  assert.deepEqual(initial.data, { count: 1, array: [1] });

  const countUpdated = onceMatching(
    admin,
    "socket_updated",
    "data count update",
    (value) => value?.id === socket.id && value?.data?.count === 2,
  );
  assert.equal(
    await emitAck(socket, "set-data", { count: 2, array: [1] }),
    "updated",
  );
  assert.deepEqual((await countUpdated)[0].data, { count: 2, array: [1] });

  const arrayUpdated = onceMatching(
    admin,
    "socket_updated",
    "data array update",
    (value) =>
      value?.id === socket.id &&
      value?.data?.count === 2 &&
      value?.data?.array?.length === 2,
  );
  assert.equal(
    await emitAck(socket, "set-data", { count: 2, array: [1, 2] }),
    "updated",
  );
  assert.deepEqual((await arrayUpdated)[0].data, {
    count: 2,
    array: [1, 2],
  });
  verifiedCases.add(13);
  await close(socket);
}

async function verifyManagement(admin) {
  const socket = await connect(endpoints.main, "/management");

  const managedEvent = once(socket, "managed-event", "managed emit");
  admin.emit(
    "emit",
    "/management",
    socket.id,
    "managed-event",
    "world",
  );
  assert.deepEqual(await managedEvent, ["world"]);

  const joined = onceMatching(
    admin,
    "room_joined",
    "managed room joined",
    (nsp, room, id) =>
      nsp === "/management" && room === "room1" && id === socket.id,
  );
  admin.emit("join", "/management", "room1", socket.id);
  await joined;
  assert.equal(await emitAck(socket, "has-room", "room1"), true);

  const left = onceMatching(
    admin,
    "room_left",
    "managed room left",
    (nsp, room, id) =>
      nsp === "/management" && room === "room1" && id === socket.id,
  );
  admin.emit("leave", "/management", "room1", socket.id);
  await left;
  assert.equal(await emitAck(socket, "has-room", "room1"), false);

  const disconnected = once(socket, "disconnect", "managed disconnect");
  admin.emit("_disconnect", "/management", false, socket.id);
  assert.equal((await disconnected)[0], "io server disconnect");
  verifiedCases.add(14);
  await close(socket);
}

async function verifyDynamicNamespace(admin) {
  const connectedEvent = onceMatching(
    admin,
    "socket_connected",
    "dynamic socket_connected",
    (socket) => socket?.nsp === "/dynamic-101",
  );
  const socket = await connect(endpoints.main, "/dynamic-101");
  const [serialized] = await connectedEvent;
  assert.equal(serialized.id, socket.id);
  assert.equal(serialized.nsp, "/dynamic-101");
  verifiedCases.add(15);
  await close(socket);
}

async function verifyTrackedEvents(admin) {
  const socket = await connect(endpoints.main, "/events");

  const sentNoAck = onceMatching(
    admin,
    "event_sent",
    "event_sent without ACK",
    (_nsp, id, args) => id === socket.id && args?.[0] === "sent-no-ack",
  );
  const receivedByClient = once(socket, "sent-no-ack", "sent-no-ack client");
  assert.equal(await emitAck(socket, "trigger-sent-no-ack"), "sent");
  const sentClientArgs = await receivedByClient;
  assert.equal(sentClientArgs[0], 1);
  assert.equal(sentClientArgs[1], "2");
  assert.deepEqual(Buffer.from(sentClientArgs[2]), Buffer.from([3]));
  const sentNoAckArgs = await sentNoAck;
  assert.equal(sentNoAckArgs[0], "/events");
  assert.equal(sentNoAckArgs[1], socket.id);
  assert.equal(sentNoAckArgs[2][0], "sent-no-ack");
  assert.equal(sentNoAckArgs[2][1], 1);
  assert.equal(sentNoAckArgs[2][2], "2");
  assert.deepEqual(Buffer.from(sentNoAckArgs[2][3]), Buffer.from([3]));
  assertISOString(sentNoAckArgs[3]);
  verifiedCases.add(18);

  const sentWithAck = onceMatching(
    admin,
    "event_sent",
    "event_sent with ACK",
    (_nsp, id, args) => id === socket.id && args?.[0] === "sent-with-ack",
  );
  const clientAck = new Promise((resolve) => {
    socket.once("sent-with-ack", (ack) => {
      assert.equal(typeof ack, "function");
      ack("world");
      resolve();
    });
  });
  assert.equal(await emitAck(socket, "trigger-sent-with-ack"), "world");
  await clientAck;
  const sentWithAckArgs = await sentWithAck;
  assert.deepEqual(sentWithAckArgs.slice(0, 3), [
    "/events",
    socket.id,
    ["sent-with-ack"],
  ]);
  assertISOString(sentWithAckArgs[3]);
  verifiedCases.add(19);

  const receivedNoAck = onceMatching(
    admin,
    "event_received",
    "event_received without ACK",
    (_nsp, id, args) => id === socket.id && args?.[0] === "received-no-ack",
  );
  socket.emit("received-no-ack", 1, "2", Buffer.from([3]));
  const receivedNoAckArgs = await receivedNoAck;
  assert.equal(receivedNoAckArgs[0], "/events");
  assert.equal(receivedNoAckArgs[1], socket.id);
  assert.equal(receivedNoAckArgs[2][0], "received-no-ack");
  assert.equal(receivedNoAckArgs[2][1], 1);
  assert.equal(receivedNoAckArgs[2][2], "2");
  assert.deepEqual(Buffer.from(receivedNoAckArgs[2][3]), Buffer.from([3]));
  assertISOString(receivedNoAckArgs[3]);
  verifiedCases.add(20);

  const receivedWithAck = onceMatching(
    admin,
    "event_received",
    "event_received with ACK",
    (_nsp, id, args) => id === socket.id && args?.[0] === "received-with-ack",
  );
  assert.equal(await emitAck(socket, "received-with-ack", "world"), "123");
  const receivedWithAckArgs = await receivedWithAck;
  assert.deepEqual(receivedWithAckArgs.slice(0, 3), [
    "/events",
    socket.id,
    ["received-with-ack", "world"],
  ]);
  assertISOString(receivedWithAckArgs[3]);
  verifiedCases.add(21);
  await close(socket);
}

async function main() {
  await verifyCustomNamespace();
  await verifyAuthentication();
  await verifyFeatureModes();

  const { admin, application } = await connectMainAdminWithExistingSocket();
  try {
    await verifyAdministrativeLifecycle(admin);
    await verifySocketDataUpdates(admin);
    await verifyManagement(admin);
    await verifyDynamicNamespace(admin);
    await verifyTrackedEvents(admin);
  } finally {
    await Promise.all([close(application), close(admin)]);
  }

  const cases = [...verifiedCases].sort((first, second) => first - second);
  process.stdout.write(
    `${JSON.stringify({
      officialClient: "socket.io-client@4.8.3",
      officialPackage: "@socket.io/admin-ui@0.5.1",
      officialGitHead,
      verifiedCases: cases,
    })}\n`,
  );
}

const watchdog = setTimeout(() => {
  process.stderr.write("Admin UI official source matrix watchdog expired\n");
  process.exit(124);
}, 70_000);

main()
  .catch((error) => {
    console.error(error && error.stack ? error.stack : error);
    process.exitCode = 1;
  })
  .finally(() => watchdog.unref());
