"use strict";

const assert = require("node:assert/strict");
const path = require("node:path");
const { execFile, fork } = require("node:child_process");
const { promisify } = require("node:util");
const { createServer } = require("node:http");
const { Server } = require("socket.io");
const { io: createClient } = require("socket.io-client");
const {
  createAdapter: createPubSubAdapter,
  createShardedAdapter,
} = require("@socket.io/redis-adapter");
const {
  createAdapter: createStreamsAdapter,
} = require("@socket.io/redis-streams-adapter");
const { createClient: createRedisClient } = require("redis");

const goURL = process.argv[2];
const redisURL = process.argv[3];
const adapterMode = process.argv[4] || "pubsub";
const goWorkerURL = process.argv[5];
const goWorkerPID = Number(process.argv[6]);
const toxiproxyAPI = process.env.SOCKET_IO_REDIS_TOXIPROXY_API || "";
const toxiproxyName = process.env.SOCKET_IO_REDIS_TOXIPROXY_NAME || "redis";
const redisRestartContainer =
  process.env.SOCKET_IO_REDIS_RESTART_CONTAINER || "";
const execFileAsync = promisify(execFile);
if (
  !goURL ||
  !redisURL ||
  !goWorkerURL ||
  !Number.isInteger(goWorkerPID) ||
  !["pubsub", "streams", "sharded"].includes(adapterMode)
) {
  throw new Error(
    "usage: node interop.cjs <go-server-url> <redis-url> <pubsub|streams|sharded> <go-worker-url> <go-worker-pid>",
  );
}

const timeoutMs = 10000;

function once(socket, event, timeout = timeoutMs) {
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

function emitWithAck(socket, event, ...args) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(
      () => reject(new Error(`timed out waiting for ACK of "${event}"`)),
      timeoutMs,
    );
    socket.emit(event, ...args, (...ackArgs) => {
      clearTimeout(timer);
      resolve(ackArgs);
    });
  });
}

function delay(duration) {
  return new Promise((resolve) => setTimeout(resolve, duration));
}

async function waitUntilAsync(predicate, description, timeout = timeoutMs) {
  const deadline = Date.now() + timeout;
  let lastError;
  while (Date.now() < deadline) {
    try {
      if (await predicate()) return;
    } catch (error) {
      lastError = error;
    }
    await delay(50);
  }
  throw new Error(
    `timed out waiting for ${description}${
      lastError ? `: ${lastError.message}` : ""
    }`,
  );
}

async function expectDeliveryMatrix(
  sockets,
  event,
  expectedIndexes,
  action,
  settleDuration = 300,
) {
  const deliveries = sockets.map(() => []);
  const listeners = sockets.map((socket, index) => (...args) => {
    deliveries[index].push(applicationArgs(args));
  });
  sockets.forEach((socket, index) => socket.on(event, listeners[index]));
  try {
    await action();
    if (expectedIndexes.length > 0) {
      await waitUntilAsync(
        () => expectedIndexes.every((index) => deliveries[index].length >= 1),
        `expected deliveries for "${event}"`,
      );
    }
    await delay(settleDuration);
    for (let index = 0; index < sockets.length; index++) {
      const expectedCount = expectedIndexes.includes(index) ? 1 : 0;
      assert.equal(
        deliveries[index].length,
        expectedCount,
        `delivery count for "${event}" on socket ${index}`,
      );
    }
    return deliveries;
  } finally {
    sockets.forEach((socket, index) => socket.off(event, listeners[index]));
  }
}

async function setRedisProxyEnabled(enabled) {
  const response = await fetch(
    `${toxiproxyAPI.replace(/\/$/, "")}/proxies/${encodeURIComponent(
      toxiproxyName,
    )}`,
    {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ enabled }),
    },
  );
  if (!response.ok) {
    throw new Error(
      `Toxiproxy update failed (${response.status}): ${await response.text()}`,
    );
  }
}

async function restartRedisContainer() {
  if (!/^[A-Za-z0-9][A-Za-z0-9_.-]*$/.test(redisRestartContainer)) {
    throw new Error(
      `invalid Redis restart container name: ${redisRestartContainer}`,
    );
  }
  await execFileAsync("docker", ["restart", redisRestartContainer], {
    timeout: 30000,
  });
}

function startWorker(redisURL, mode, generation = "current") {
  return new Promise((resolve, reject) => {
    const worker = fork(
      path.join(__dirname, "worker.cjs"),
      [redisURL, mode, generation],
      {
        stdio: ["ignore", "inherit", "inherit", "ipc"],
      },
    );
    const timer = setTimeout(() => {
      worker.kill("SIGKILL");
      reject(new Error("timed out waiting for Redis interop worker"));
    }, timeoutMs);
    worker.once("error", (error) => {
      clearTimeout(timer);
      reject(error);
    });
    worker.once("exit", (code, signal) => {
      if (!worker.__ready) {
        clearTimeout(timer);
        reject(
          new Error(
            `Redis interop worker exited before ready: code=${code}, signal=${signal}`,
          ),
        );
      }
    });
    worker.once("message", ({ port, generation: readyGeneration }) => {
      clearTimeout(timer);
      worker.__ready = true;
      if (readyGeneration !== generation) {
        worker.kill("SIGKILL");
        reject(
          new Error(
            `Redis interop worker generation mismatch: ${readyGeneration}`,
          ),
        );
        return;
      }
      resolve({ worker, url: `http://127.0.0.1:${port}`, generation });
    });
  });
}

function collectOrderedEvents(
  socket,
  event,
  expectedCount,
  timeout = timeoutMs,
) {
  return new Promise((resolve, reject) => {
    const values = [];
    const timer = setTimeout(() => {
      socket.off(event, onEvent);
      reject(
        new Error(
          `timed out waiting for ${expectedCount} "${event}" events; received ${values.length}`,
        ),
      );
    }, timeout);
    const onEvent = (value) => {
      values.push(value);
      if (values.length === expectedCount) {
        // Keep the listener briefly to detect duplicate deliveries that arrive
        // immediately after the expected batch.
        setTimeout(() => {
          clearTimeout(timer);
          socket.off(event, onEvent);
          resolve(values);
        }, 100);
      }
    };
    socket.on(event, onEvent);
  });
}

function applicationArgs(args) {
  // With connection-state recovery enabled, the Redis Streams adapter appends
  // its stream offset as the final argument. The official client records it as
  // _lastOffset while still forwarding it to listeners.
  const last = args[args.length - 1];
  return adapterMode === "streams" &&
    typeof last === "string" &&
    /^\d+-\d+$/.test(last)
    ? args.slice(0, -1)
    : args;
}

async function connect(url) {
  const socket = createClient(url, {
    forceNew: true,
    reconnection: false,
    transports: ["websocket"],
    timeout: timeoutMs,
  });
  await once(socket, "connect");
  return socket;
}

async function waitForRedisSession(redisClient, pid) {
  const key = `sio:session:${pid}`;
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await redisClient.exists(key)) return;
    await delay(25);
  }
  throw new Error(`timed out waiting for persisted session ${pid}`);
}

async function recoverOn(url, pid, offset, missedEvent) {
  const socket = createClient(url, {
    autoConnect: false,
    forceNew: true,
    reconnection: false,
    transports: ["websocket"],
    timeout: timeoutMs,
  });
  // These fields are the recovery state sent by the official client in the
  // namespace CONNECT packet. Setting them lets one client move to the other
  // server implementation while keeping the official client protocol intact.
  socket._pid = pid;
  socket._lastOffset = offset;
  const connected = once(socket, "connect");
  const missed = once(socket, missedEvent);
  socket.connect();
  await connected;
  const missedArgs = await missed;
  return { socket, missedArgs };
}

async function reconnectWithState(url, pid, offset) {
  const socket = createClient(url, {
    autoConnect: false,
    forceNew: true,
    reconnection: false,
    transports: ["websocket"],
    timeout: timeoutMs,
  });
  socket._pid = pid;
  socket._lastOffset = offset;
  const connected = once(socket, "connect");
  socket.connect();
  await connected;
  return socket;
}

async function main() {
  const officialRows = new Set();
  const markOfficialRows = (...rows) =>
    rows.forEach((row) => officialRows.add(row));
  const pubClient = createRedisClient({ url: redisURL });
  const subClient = pubClient.duplicate();
  pubClient.on("error", () => {});
  subClient.on("error", () => {});
  await Promise.all([pubClient.connect(), subClient.connect()]);

  const httpServer = createServer();
  const io = new Server(httpServer, {
    transports: ["websocket"],
    connectionStateRecovery:
      adapterMode === "streams"
        ? { maxDisconnectionDuration: 30_000, skipMiddlewares: true }
        : undefined,
  });
  if (adapterMode === "streams") {
    io.adapter(
      createStreamsAdapter(pubClient, {
        blockTimeInMs: 100,
      }),
    );
  } else if (adapterMode === "sharded") {
    io.adapter(
      createShardedAdapter(pubClient, subClient, {
        subscriptionMode: "dynamic",
      }),
    );
  } else {
    io.adapter(
      createPubSubAdapter(pubClient, subClient, {
        publishOnSpecificResponseChannel: true,
        requestsTimeout: 1000,
      }),
    );
  }

  io.on("connection", (socket) => {
    socket.join("node-room");
  });
  io.of("/custom");
  io.on("from-go-server", (value, ack) => {
    ack(`node-server:${value}`);
  });

  await new Promise((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
  const address = httpServer.address();
  const nodeURL = `http://127.0.0.1:${address.port}`;

  let goClient;
  let nodeClient;
  let customGoClient;
  let customNodeClient;
  let workerProcess;
  let workerClient;
  let replacementWorkerProcess;
  let replacementWorkerClient;
  let goWorkerClient;
  const recoveryClients = [];
  try {
    [goClient, nodeClient] = await Promise.all([
      connect(goURL),
      connect(nodeURL),
    ]);
    goClient.on("cluster-ack", (value, ack) => ack(`go-client:${value}`));
    nodeClient.on("cluster-ack", (value, ack) => ack(`node-client:${value}`));
    goClient.on("cluster-binary-ack", (_value, ack) =>
      ack(Buffer.from([1, 2, 3])),
    );
    nodeClient.on("cluster-binary-ack", (_value, ack) =>
      ack(Buffer.from([4, 5, 6])),
    );
    await delay(adapterMode === "streams" ? 500 : 250);

    console.error("[redis-interop] official namespace broadcast assertions");
    [customGoClient, customNodeClient] = await Promise.all([
      connect(`${goURL}/custom`),
      connect(`${nodeURL}/custom`),
    ]);
    await delay(adapterMode === "streams" ? 500 : 250);
    let deliveries = await expectDeliveryMatrix(
      [customGoClient, customNodeClient],
      "namespace-from-node",
      [0, 1],
      () => io.of("/custom").emit("namespace-from-node", "node-namespace"),
    );
    deliveries.forEach((values) => {
      assert.deepEqual(values[0], ["node-namespace"]);
    });
    deliveries = await expectDeliveryMatrix(
      [customGoClient, customNodeClient],
      "namespace-from-go",
      [0, 1],
      () =>
        emitWithAck(goClient, "control-namespace-broadcast", "go-namespace"),
    );
    deliveries.forEach((values) => {
      assert.deepEqual(values[0], ["go-namespace"]);
    });
    markOfficialRows(adapterMode === "streams" ? "S02" : "R02");

    console.error(
      "[redis-interop] official fetchSockets single-instance assertions",
    );
    await emitWithAck(goClient, "interop-set-data", "official-test-data");
    const [remoteGoSocket] = await io.in(goClient.id).fetchSockets();
    assert.ok(remoteGoSocket, "fetchSockets() must find the remote Go socket");
    assert.equal(remoteGoSocket.id, goClient.id);
    assert.equal(remoteGoSocket.data, "official-test-data");
    assert.ok(remoteGoSocket.handshake && remoteGoSocket.handshake.url);
    markOfficialRows(adapterMode === "streams" ? "S21" : "R20");

    console.error(
      "[redis-interop] official room union, exclusion, local and single-target assertions",
    );
    const nodeServerSocket = io.sockets.sockets.get(nodeClient.id);
    assert.ok(nodeServerSocket, "Node server socket must exist");
    for (const room of [
      "official-room-1",
      "official-room-2",
      "official-room-3",
    ]) {
      await emitWithAck(goClient, "interop-join", room);
    }
    nodeServerSocket.join(["official-room-1", "official-room-2"]);
    await delay(adapterMode === "streams" ? 400 : 150);
    deliveries = await expectDeliveryMatrix(
      [goClient, nodeClient],
      "official-room-union",
      [1],
      () =>
        io
          .to("official-room-1")
          .to("official-room-2")
          .except("official-room-3")
          .emit("official-room-union", "once"),
    );
    assert.deepEqual(deliveries[1][0], ["once"]);
    deliveries = await expectDeliveryMatrix(
      [goClient, nodeClient],
      "official-room-except",
      [1],
      () => io.except("official-room-3").emit("official-room-except", "except"),
    );
    assert.deepEqual(deliveries[1][0], ["except"]);
    deliveries = await expectDeliveryMatrix(
      [goClient, nodeClient],
      "local-from-go",
      [0],
      () => emitWithAck(goClient, "control-local-broadcast", "local"),
    );
    assert.deepEqual(deliveries[0][0], ["local"]);
    deliveries = await expectDeliveryMatrix(
      [goClient, nodeClient],
      "official-single-target",
      [0],
      () => io.to(goClient.id).emit("official-single-target", "single"),
    );
    assert.deepEqual(deliveries[0][0], ["single"]);
    if (adapterMode === "streams") {
      markOfficialRows("S04", "S05");
    } else {
      markOfficialRows("R04", "R05", "R06", "R07");
    }

    console.error(
      "[redis-interop] official empty-target and timeout acknowledgement assertions",
    );
    const emptyAckResult = await new Promise((resolve) => {
      io.to("official-missing-room")
        .timeout(500)
        .emit("official-empty-ack", (error, responses) =>
          resolve({ error, responses }),
        );
    });
    assert.equal(emptyAckResult.error, null);
    assert.deepEqual(emptyAckResult.responses, []);
    goClient.once("official-timeout-ack", (ack) => ack("go-response"));
    nodeClient.once("official-timeout-ack", () => {});
    const timeoutAckResult = await new Promise((resolve) => {
      io.timeout(500).emit("official-timeout-ack", (error, responses) => {
        resolve({ error, responses });
      });
    });
    assert.ok(timeoutAckResult.error instanceof Error);
    assert.deepEqual(timeoutAckResult.responses, ["go-response"]);
    markOfficialRows(
      ...(adapterMode === "streams" ? ["S08", "S09"] : ["R10", "R11"]),
    );

    const clusterSocketHasRoom = async (socketId, room) => {
      const sockets = await io.in(socketId).fetchSockets();
      return sockets.length === 1 && sockets[0].rooms.has(room);
    };
    console.error(
      "[redis-interop] official all/given socketsJoin and socketsLeave assertions",
    );
    io.socketsJoin("official-join-all");
    await waitUntilAsync(
      async () =>
        (await clusterSocketHasRoom(goClient.id, "official-join-all")) &&
        (await clusterSocketHasRoom(nodeClient.id, "official-join-all")),
      "all sockets to join official-join-all",
    );
    io.in(goClient.id).socketsJoin("official-join-one");
    await waitUntilAsync(
      async () =>
        (await clusterSocketHasRoom(goClient.id, "official-join-one")) &&
        !(await clusterSocketHasRoom(nodeClient.id, "official-join-one")),
      "the selected Go socket to join official-join-one",
    );
    io.socketsLeave("official-join-all");
    await waitUntilAsync(
      async () =>
        !(await clusterSocketHasRoom(goClient.id, "official-join-all")) &&
        !(await clusterSocketHasRoom(nodeClient.id, "official-join-all")),
      "all sockets to leave official-join-all",
    );
    nodeServerSocket.join("official-leave-one");
    await emitWithAck(goClient, "interop-join", "official-leave-one");
    io.in(goClient.id).socketsLeave("official-leave-one");
    await waitUntilAsync(
      async () =>
        !(await clusterSocketHasRoom(goClient.id, "official-leave-one")) &&
        (await clusterSocketHasRoom(nodeClient.id, "official-leave-one")),
      "only the selected Go socket to leave official-leave-one",
    );
    markOfficialRows(
      ...(adapterMode === "streams"
        ? ["S12", "S14", "S15", "S17"]
        : ["R12", "R14", "R15", "R17"]),
    );

    console.error("[redis-interop] official server-side emit assertions");
    deliveries = await expectDeliveryMatrix(
      [goClient, nodeClient],
      "official-matrix-server-side-seen",
      [0],
      () => io.serverSideEmit("official-matrix-server-side", "server-event"),
    );
    assert.deepEqual(deliveries[0][0], ["server-event"]);
    markOfficialRows(adapterMode === "streams" ? "S23" : "R22");
    if (adapterMode === "pubsub") {
      const serverTimeoutResult = await io
        .serverSideEmitWithAck("official-matrix-server-side-timeout")
        .then(
          (responses) => ({ responses }),
          (error) => ({ error, responses: error.responses || [] }),
        );
      assert.ok(serverTimeoutResult.error instanceof Error);
      assert.deepEqual(serverTimeoutResult.responses, []);

      const [allRooms] = await emitWithAck(goClient, "control-all-rooms");
      assert.ok(allRooms.includes(goClient.id));
      assert.ok(allRooms.includes(nodeClient.id));
      assert.ok(allRooms.includes("official-room-1"));
      assert.ok(allRooms.includes("official-room-3"));
      markOfficialRows("R24", "R31");
    }

    console.error("[redis-interop] Node -> Go broadcast");
    const fromNode = once(goClient, "from-node");
    io.emit("from-node", "node-broadcast");
    assert.deepEqual(applicationArgs(await fromNode), ["node-broadcast"]);
    markOfficialRows(adapterMode === "streams" ? "S01" : "R01");

    console.error("[redis-interop] Go -> Node broadcast");
    const fromGo = once(nodeClient, "from-go");
    await emitWithAck(goClient, "control-broadcast", "go-broadcast");
    assert.deepEqual(applicationArgs(await fromGo), ["go-broadcast"]);

    const binary = Buffer.from([0, 1, 2, 127, 255]);
    const binaryFromNode = once(goClient, "binary-from-node");
    io.emit("binary-from-node", binary);
    const [nodeBinaryValue] = await binaryFromNode;
    assert.deepEqual(Buffer.from(nodeBinaryValue), binary);

    const binaryFromGo = once(nodeClient, "binary-from-go");
    await emitWithAck(goClient, "control-binary-broadcast", binary);
    const [goBinaryValue] = await binaryFromGo;
    assert.deepEqual(Buffer.from(goBinaryValue), binary);

    console.error("[redis-interop] room broadcasts");
    const nodeToRoom = once(goClient, "node-to-go-room");
    io.to("go-room").emit("node-to-go-room", "room-value");
    assert.deepEqual(applicationArgs(await nodeToRoom), ["room-value"]);

    const goToRoom = once(nodeClient, "go-to-node-room");
    await emitWithAck(goClient, "control-room-broadcast", "room-value");
    assert.deepEqual(applicationArgs(await goToRoom), ["room-value"]);
    markOfficialRows(adapterMode === "streams" ? "S03" : "R03");

    console.error("[redis-interop] fetchSockets");
    const nodeSockets = await io.fetchSockets();
    assert.equal(
      nodeSockets.length,
      2,
      "Node fetchSockets() must include the Go node socket",
    );
    const [goSocketCount] = await emitWithAck(
      goClient,
      "control-fetch-sockets",
    );
    assert.equal(
      goSocketCount,
      2,
      "Go FetchSockets() must include the Node node socket",
    );
    markOfficialRows(adapterMode === "streams" ? "S20" : "R19");

    console.error("[redis-interop] Node -> Go broadcast ACK");
    const nodeAckResponses = await new Promise((resolve, reject) => {
      io.timeout(3000).emit("cluster-ack", "from-node", (err, responses) => {
        if (err) reject(err);
        else resolve(responses);
      });
    });
    assert.deepEqual(nodeAckResponses.sort(), [
      "go-client:from-node",
      "node-client:from-node",
    ]);

    console.error("[redis-interop] Go -> Node broadcast ACK");
    const [goAckResponses] = await emitWithAck(
      goClient,
      "control-broadcast-ack",
      "from-go",
    );
    assert.deepEqual(goAckResponses.sort(), [
      "go-client:from-go",
      "node-client:from-go",
    ]);
    markOfficialRows(adapterMode === "streams" ? "S06" : "R08");

    const nodeBinaryAckResponses = await new Promise((resolve, reject) => {
      io.timeout(3000).emit("cluster-binary-ack", binary, (err, responses) => {
        if (err) reject(err);
        else resolve(responses);
      });
    });
    assert.deepEqual(
      nodeBinaryAckResponses
        .map((value) => Buffer.from(value).toString("hex"))
        .sort(),
      ["010203", "040506"],
    );

    const [goBinaryAckResponses] = await emitWithAck(
      goClient,
      "control-binary-broadcast-ack",
      binary,
    );
    assert.deepEqual(
      goBinaryAckResponses
        .map((value) => Buffer.from(value).toString("hex"))
        .sort(),
      ["010203", "040506"],
    );
    markOfficialRows(adapterMode === "streams" ? "S07" : "R09");

    console.error("[redis-interop] server-side ACK");
    const nodeServerResponses = await io.serverSideEmitWithAck(
      "from-node-server",
      "request",
    );
    assert.deepEqual(nodeServerResponses, ["go-server:request"]);
    const [goServerResponses] = await emitWithAck(
      goClient,
      "control-server-side-ack",
      "request",
    );
    assert.deepEqual(goServerResponses, ["node-server:request"]);
    markOfficialRows(adapterMode === "streams" ? "S24" : "R23");

    console.error("[redis-interop] socketsJoin");
    io.in("go-room").socketsJoin("joined-by-node");
    await delay(100);
    const joinedByNode = once(goClient, "joined-by-node-event");
    io.to("joined-by-node").emit("joined-by-node-event", "joined");
    assert.deepEqual(applicationArgs(await joinedByNode), ["joined"]);

    await emitWithAck(
      goClient,
      "control-sockets-join",
      "node-room",
      "joined-by-go",
    );
    await delay(100);
    const joinedByGo = once(nodeClient, "joined-by-go-event");
    io.to("joined-by-go").emit("joined-by-go-event", "joined");
    assert.deepEqual(applicationArgs(await joinedByGo), ["joined"]);

    io.in("go-room").socketsLeave("joined-by-node");
    await delay(100);
    const [goStillJoined] = await emitWithAck(
      goClient,
      "control-has-room",
      "joined-by-node",
    );
    assert.equal(goStillJoined, false);

    await emitWithAck(
      goClient,
      "control-sockets-leave",
      "node-room",
      "joined-by-go",
    );
    await delay(100);
    const socketsAfterLeave = await io.fetchSockets();
    const nodeSocketAfterLeave = socketsAfterLeave.find(
      (socket) => socket.id === nodeClient.id,
    );
    assert.ok(
      nodeSocketAfterLeave,
      "Node socket must still be connected after socketsLeave",
    );
    assert.equal(nodeSocketAfterLeave.rooms.has("joined-by-go"), false);
    markOfficialRows(
      ...(adapterMode === "streams" ? ["S13", "S16"] : ["R13", "R16"]),
    );

    if (adapterMode === "streams") {
      console.error("[redis-interop] Node session -> Go recovery");
      const nodeSource = await connect(nodeURL);
      recoveryClients.push(nodeSource);
      const nodeBaseline = once(nodeSource, "node-recovery-baseline");
      io.to(nodeSource.id).emit("node-recovery-baseline", "baseline");
      assert.deepEqual(applicationArgs(await nodeBaseline), ["baseline"]);
      const nodeSession = {
        id: nodeSource.id,
        pid: nodeSource._pid,
        offset: nodeSource._lastOffset,
      };
      assert.ok(
        nodeSession.pid && nodeSession.offset,
        "Node client must receive recovery state",
      );
      const nodeClosed = once(nodeSource, "disconnect");
      nodeSource.io.engine.close();
      await nodeClosed;
      await waitForRedisSession(pubClient, nodeSession.pid);
      await emitWithAck(
        goClient,
        "control-target-broadcast",
        nodeSession.id,
        "node-to-go-recovery-missed",
        "missed-from-go",
      );
      const nodeToGo = await recoverOn(
        goURL,
        nodeSession.pid,
        nodeSession.offset,
        "node-to-go-recovery-missed",
      );
      recoveryClients.push(nodeToGo.socket);
      assert.equal(nodeToGo.socket.id, nodeSession.id);
      assert.equal(nodeToGo.socket.recovered, true);
      assert.deepEqual(applicationArgs(nodeToGo.missedArgs), [
        "missed-from-go",
      ]);
      markOfficialRows("S27");

      console.error("[redis-interop] Go session -> Node recovery");
      const goSource = await connect(goURL);
      recoveryClients.push(goSource);
      const goBaseline = once(goSource, "go-recovery-baseline");
      await emitWithAck(
        goClient,
        "control-target-broadcast",
        goSource.id,
        "go-recovery-baseline",
        "baseline",
      );
      assert.deepEqual(applicationArgs(await goBaseline), ["baseline"]);
      const goSession = {
        id: goSource.id,
        pid: goSource._pid,
        offset: goSource._lastOffset,
      };
      assert.ok(
        goSession.pid && goSession.offset,
        "Go client must receive recovery state",
      );
      const goClosed = once(goSource, "disconnect");
      goSource.io.engine.close();
      await goClosed;
      await waitForRedisSession(pubClient, goSession.pid);
      io.to(goSession.id).emit(
        "go-to-node-recovery-missed",
        "missed-from-node",
      );
      const goToNode = await recoverOn(
        nodeURL,
        goSession.pid,
        goSession.offset,
        "go-to-node-recovery-missed",
      );
      recoveryClients.push(goToNode.socket);
      assert.equal(goToNode.socket.id, goSession.id);
      assert.equal(goToNode.socket.recovered, true);
      assert.deepEqual(applicationArgs(goToNode.missedArgs), [
        "missed-from-node",
      ]);

      console.error("[redis-interop] official filtered missed-packet recovery");
      const filterSource = await connect(goURL);
      recoveryClients.push(filterSource);
      await emitWithAck(filterSource, "interop-join", "official-csr-room-1");
      const filterBaseline = once(filterSource, "official-csr-baseline");
      io.to(filterSource.id).emit("official-csr-baseline", "baseline");
      await filterBaseline;
      const filterSession = {
        id: filterSource.id,
        pid: filterSource._pid,
        offset: filterSource._lastOffset,
      };
      assert.ok(filterSession.pid && filterSession.offset);
      const filterClosed = once(filterSource, "disconnect");
      filterSource.io.engine.close();
      await filterClosed;
      await waitForRedisSession(pubClient, filterSession.pid);
      io.to(filterSession.id).emit("official-csr-missed", 1);
      io.emit("official-csr-missed", 2);
      io.to("official-csr-room-1").emit("official-csr-missed", 3);
      io.to("official-csr-room-2").emit("official-csr-missed", 4);
      io.except("official-csr-room-1").emit("official-csr-missed", 5);
      io.of("/foo").emit("official-csr-missed", 6);

      const filteredRecovery = createClient(goURL, {
        autoConnect: false,
        forceNew: true,
        reconnection: false,
        transports: ["websocket"],
        timeout: timeoutMs,
      });
      recoveryClients.push(filteredRecovery);
      filteredRecovery._pid = filterSession.pid;
      filteredRecovery._lastOffset = filterSession.offset;
      const recoveredValues = [];
      const recoveredOffsets = new Set();
      filteredRecovery.on("official-csr-missed", (...args) => {
        recoveredValues.push(args[0]);
        recoveredOffsets.add(args[args.length - 1]);
      });
      const filterReconnected = once(filteredRecovery, "connect");
      filteredRecovery.connect();
      await filterReconnected;
      await waitUntilAsync(
        () => recoveredValues.length === 3,
        "three filtered recovery packets",
      );
      await delay(100);
      assert.equal(filteredRecovery.recovered, true);
      assert.equal(filteredRecovery.id, filterSession.id);
      assert.deepEqual(recoveredValues, [1, 2, 3]);
      assert.equal(recoveredOffsets.size, 3);
      for (const offset of recoveredOffsets) {
        assert.match(offset, /^\d+-\d+$/);
      }
      markOfficialRows("S28");

      console.error("[redis-interop] official invalid session ID recovery");
      const invalidPidSource = await connect(goURL);
      recoveryClients.push(invalidPidSource);
      const invalidPidBaseline = once(
        invalidPidSource,
        "official-invalid-pid-baseline",
      );
      io.to(invalidPidSource.id).emit(
        "official-invalid-pid-baseline",
        "baseline",
      );
      await invalidPidBaseline;
      const invalidPidOffset = invalidPidSource._lastOffset;
      const invalidPidClosed = once(invalidPidSource, "disconnect");
      invalidPidSource.io.engine.close();
      await invalidPidClosed;
      const invalidPidSocket = await reconnectWithState(
        goURL,
        "abc",
        invalidPidOffset,
      );
      recoveryClients.push(invalidPidSocket);
      assert.equal(invalidPidSocket.recovered, false);
      markOfficialRows("S29");

      console.error("[redis-interop] official invalid offset recovery");
      const invalidOffsetSource = await connect(goURL);
      recoveryClients.push(invalidOffsetSource);
      const invalidOffsetBaseline = once(
        invalidOffsetSource,
        "official-invalid-offset-baseline",
      );
      io.to(invalidOffsetSource.id).emit(
        "official-invalid-offset-baseline",
        "baseline",
      );
      await invalidOffsetBaseline;
      const invalidOffsetPid = invalidOffsetSource._pid;
      const invalidOffsetClosed = once(invalidOffsetSource, "disconnect");
      invalidOffsetSource.io.engine.close();
      await invalidOffsetClosed;
      await waitForRedisSession(pubClient, invalidOffsetPid);
      const invalidOffsetSocket = await reconnectWithState(
        goURL,
        invalidOffsetPid,
        "abc",
      );
      recoveryClients.push(invalidOffsetSocket);
      assert.equal(invalidOffsetSocket.recovered, false);
      markOfficialRows("S30");
    }

    console.error("[redis-interop] Redis connection reset and resubscription");
    const [normalKilled, pubSubKilled, resetError] = await emitWithAck(
      goClient,
      "control-reset-redis-connections",
    );
    assert.equal(resetError, "");
    assert.ok(
      normalKilled > 0,
      "connection reset must kill at least one normal Redis connection",
    );
    assert.ok(
      pubSubKilled > 0,
      "connection reset must kill at least one Pub/Sub Redis connection",
    );
    await delay(adapterMode === "streams" ? 2500 : 1500);

    const postResetFromNode = once(goClient, "from-node");
    io.emit("from-node", "post-reset-node-broadcast");
    assert.deepEqual(applicationArgs(await postResetFromNode), [
      "post-reset-node-broadcast",
    ]);

    const postResetFromGo = once(nodeClient, "from-go");
    await emitWithAck(goClient, "control-broadcast", "post-reset-go-broadcast");
    assert.deepEqual(applicationArgs(await postResetFromGo), [
      "post-reset-go-broadcast",
    ]);

    const expectedSocketCount =
      2 + recoveryClients.filter((socket) => socket.connected).length;
    const postResetNodeSockets = await io.fetchSockets();
    assert.equal(
      postResetNodeSockets.length,
      expectedSocketCount,
      "Node fetchSockets() must recover after Redis reset",
    );
    const [postResetGoSocketCount] = await emitWithAck(
      goClient,
      "control-fetch-sockets",
    );
    assert.equal(
      postResetGoSocketCount,
      expectedSocketCount,
      "Go FetchSockets() must recover after Redis reset",
    );

    const postResetServerResponses = await io.serverSideEmitWithAck(
      "from-node-server",
      "post-reset",
    );
    assert.deepEqual(postResetServerResponses, ["go-server:post-reset"]);

    if (toxiproxyAPI) {
      console.error(
        "[redis-interop] sustained Redis unavailability and recovery",
      );
      await setRedisProxyEnabled(false);
      try {
        await delay(2000);
      } finally {
        await setRedisProxyEnabled(true);
      }
      await delay(adapterMode === "streams" ? 5000 : 4000);

      const postPartitionFromNode = once(goClient, "from-node");
      io.emit("from-node", "post-partition-node-broadcast");
      assert.deepEqual(applicationArgs(await postPartitionFromNode), [
        "post-partition-node-broadcast",
      ]);
      const postPartitionFromGo = once(nodeClient, "from-go");
      await emitWithAck(
        goClient,
        "control-broadcast",
        "post-partition-go-broadcast",
      );
      assert.deepEqual(applicationArgs(await postPartitionFromGo), [
        "post-partition-go-broadcast",
      ]);
      const postPartitionNodeSockets = await io.fetchSockets();
      assert.equal(postPartitionNodeSockets.length, expectedSocketCount);
      const [postPartitionGoSockets] = await emitWithAck(
        goClient,
        "control-fetch-sockets",
      );
      assert.equal(postPartitionGoSockets, expectedSocketCount);
      const postPartitionResponses = await io.serverSideEmitWithAck(
        "from-node-server",
        "post-partition",
      );
      assert.deepEqual(postPartitionResponses, ["go-server:post-partition"]);
    }

    if (redisRestartContainer) {
      console.error("[redis-interop] Redis process restart and recovery");
      await restartRedisContainer();
      await delay(adapterMode === "streams" ? 6000 : 5000);

      const postRestartFromNode = once(goClient, "from-node");
      io.emit("from-node", "post-restart-node-broadcast");
      assert.deepEqual(applicationArgs(await postRestartFromNode), [
        "post-restart-node-broadcast",
      ]);
      const postRestartFromGo = once(nodeClient, "from-go");
      await emitWithAck(
        goClient,
        "control-broadcast",
        "post-restart-go-broadcast",
      );
      assert.deepEqual(applicationArgs(await postRestartFromGo), [
        "post-restart-go-broadcast",
      ]);
      const postRestartNodeSockets = await io.fetchSockets();
      assert.equal(postRestartNodeSockets.length, expectedSocketCount);
      const [postRestartGoSockets] = await emitWithAck(
        goClient,
        "control-fetch-sockets",
      );
      assert.equal(postRestartGoSockets, expectedSocketCount);
      const postRestartResponses = await io.serverSideEmitWithAck(
        "from-node-server",
        "post-restart",
      );
      assert.deepEqual(postRestartResponses, ["go-server:post-restart"]);
    }

    console.error(
      "[redis-interop] rolling upgrade and abrupt old Node worker exit",
    );
    const worker = await startWorker(redisURL, adapterMode, "previous");
    workerProcess = worker.worker;
    workerClient = await connect(worker.url);
    await delay(adapterMode === "streams" ? 500 : 250);
    const countWithWorker = await io.fetchSockets();
    assert.equal(
      countWithWorker.length,
      expectedSocketCount + 1,
      "cluster query must include the additional Node worker",
    );
    const [goCountWithWorker] = await emitWithAck(
      goClient,
      "control-fetch-sockets",
    );
    assert.equal(goCountWithWorker, expectedSocketCount + 1);

    const previousWorkerBroadcast = once(goClient, "from-worker");
    await emitWithAck(workerClient, "control-worker-broadcast", "old-adapter");
    assert.deepEqual(applicationArgs(await previousWorkerBroadcast), [
      "previous",
      "old-adapter",
    ]);

    const replacement = await startWorker(redisURL, adapterMode, "current");
    replacementWorkerProcess = replacement.worker;
    replacementWorkerClient = await connect(replacement.url);
    await delay(adapterMode === "streams" ? 500 : 250);
    const countDuringRollout = await io.fetchSockets();
    assert.equal(countDuringRollout.length, expectedSocketCount + 2);
    const [goCountDuringRollout] = await emitWithAck(
      goClient,
      "control-fetch-sockets",
    );
    assert.equal(goCountDuringRollout, expectedSocketCount + 2);

    const oldReceivesCurrentBroadcast = once(workerClient, "rolling-from-main");
    const newReceivesCurrentBroadcast = once(
      replacementWorkerClient,
      "rolling-from-main",
    );
    io.emit("rolling-from-main", "mixed-version");
    assert.deepEqual(applicationArgs(await oldReceivesCurrentBroadcast), [
      "mixed-version",
    ]);
    assert.deepEqual(applicationArgs(await newReceivesCurrentBroadcast), [
      "mixed-version",
    ]);

    const currentWorkerBroadcast = once(nodeClient, "from-worker");
    await emitWithAck(
      replacementWorkerClient,
      "control-worker-broadcast",
      "new-adapter",
    );
    assert.deepEqual(applicationArgs(await currentWorkerBroadcast), [
      "current",
      "new-adapter",
    ]);
    const [rolloutResponses] = await emitWithAck(
      goClient,
      "control-server-side-ack",
      "rollout",
    );
    assert.deepEqual(rolloutResponses.sort(), [
      "node-server:rollout",
      "worker-current:rollout",
      "worker-previous:rollout",
    ]);

    const workerDisconnected = once(workerClient, "disconnect");
    const workerExited = new Promise((resolve) =>
      workerProcess.once("exit", resolve),
    );
    workerProcess.kill("SIGKILL");
    await Promise.all([workerExited, workerDisconnected]);
    workerProcess = undefined;
    await waitUntilAsync(async () => {
      const sockets = await io.fetchSockets();
      return sockets.length === expectedSocketCount + 1;
    }, "Node cluster membership to converge after old worker SIGKILL");
    await waitUntilAsync(async () => {
      const [count] = await emitWithAck(goClient, "control-fetch-sockets");
      return count === expectedSocketCount + 1;
    }, "Go cluster membership to converge after old worker SIGKILL");
    const postCrashServerResponses = await io.serverSideEmitWithAck(
      "from-node-server",
      "post-crash",
    );
    assert.deepEqual(postCrashServerResponses.sort(), [
      "go-server:post-crash",
      "worker-current:post-crash",
    ]);

    const replacementDisconnected = once(replacementWorkerClient, "disconnect");
    const replacementExited = new Promise((resolve) =>
      replacementWorkerProcess.once("exit", resolve),
    );
    replacementWorkerProcess.kill("SIGTERM");
    await Promise.all([replacementExited, replacementDisconnected]);
    replacementWorkerProcess = undefined;
    await waitUntilAsync(
      async () => (await io.fetchSockets()).length === expectedSocketCount,
      "cluster membership to converge after replacement worker shutdown",
    );

    console.error(
      "[redis-interop] abrupt Go worker exit and cluster convergence",
    );
    process.kill(goWorkerPID, "SIGUSR1");
    await delay(1000);
    goWorkerClient = await connect(goWorkerURL);
    await delay(adapterMode === "streams" ? 500 : 250);
    const countWithGoWorker = await io.fetchSockets();
    assert.equal(
      countWithGoWorker.length,
      expectedSocketCount + 1,
      "cluster query must include the additional Go worker",
    );
    const [goCountWithGoWorker] = await emitWithAck(
      goClient,
      "control-fetch-sockets",
    );
    assert.equal(goCountWithGoWorker, expectedSocketCount + 1);

    const goWorkerDisconnected = once(goWorkerClient, "disconnect");
    process.kill(goWorkerPID, "SIGKILL");
    await goWorkerDisconnected;
    await waitUntilAsync(async () => {
      const sockets = await io.fetchSockets();
      return sockets.length === expectedSocketCount;
    }, "Node cluster membership to converge after Go worker SIGKILL");
    await waitUntilAsync(async () => {
      const [count] = await emitWithAck(goClient, "control-fetch-sockets");
      return count === expectedSocketCount;
    }, "Go cluster membership to converge after Go worker SIGKILL");
    const postGoCrashResponses = await io.serverSideEmitWithAck(
      "from-node-server",
      "post-go-crash",
    );
    assert.deepEqual(postGoCrashResponses, ["go-server:post-go-crash"]);

    console.error("[redis-interop] post-fault ordered broadcast burst");
    const burstCount = 300;
    const expectedBurst = Array.from(
      { length: burstCount },
      (_, index) => index,
    );
    const nodeBurst = collectOrderedEvents(goClient, "node-burst", burstCount);
    for (let index = 0; index < burstCount; index++) {
      io.emit("node-burst", index);
    }
    assert.deepEqual(
      await nodeBurst,
      expectedBurst,
      "Node -> Go burst must be ordered and exactly once",
    );

    const goBurst = collectOrderedEvents(nodeClient, "go-burst", burstCount);
    const [burstError] = await emitWithAck(
      goClient,
      "control-burst-broadcast",
      burstCount,
    );
    assert.equal(burstError, "");
    assert.deepEqual(
      await goBurst,
      expectedBurst,
      "Go -> Node burst must be ordered and exactly once",
    );

    console.error("[redis-interop] Go -> Node remote disconnect");
    const nodeDisconnected = once(nodeClient, "disconnect");
    await emitWithAck(goClient, "control-disconnect-room", "node-room");
    await nodeDisconnected;

    nodeClient = await connect(nodeURL);
    await delay(adapterMode === "streams" ? 500 : 250);

    console.error("[redis-interop] official disconnectSockets all instances");
    const activeRootSockets = [
      goClient,
      nodeClient,
      ...recoveryClients.filter((socket) => socket.connected),
    ];
    const orderedDisconnects = activeRootSockets.map(
      (socket) =>
        new Promise((resolve, reject) => {
          let receivedPacket = adapterMode !== "streams";
          const timer = setTimeout(
            () => reject(new Error("timed out waiting for cluster disconnect")),
            timeoutMs,
          );
          socket.once("official-before-disconnect", () => {
            receivedPacket = true;
          });
          socket.once("disconnect", (reason) => {
            clearTimeout(timer);
            if (!receivedPacket) {
              reject(
                new Error("socket disconnected before the preceding packet"),
              );
            } else if (
              adapterMode !== "streams" &&
              reason !== "io server disconnect"
            ) {
              reject(new Error(`unexpected disconnect reason: ${reason}`));
            } else {
              resolve();
            }
          });
        }),
    );
    if (adapterMode === "streams") {
      io.emit("official-before-disconnect");
      io.disconnectSockets(true);
      markOfficialRows("S18", "S19");
    } else {
      io.disconnectSockets(false);
      markOfficialRows("R18");
    }
    await Promise.all(orderedDisconnects);

    console.log(
      JSON.stringify(
        {
          adapterMode,
          officialAdapterVersion: adapterMode === "streams" ? "0.3.1" : "8.3.0",
          officialSourceRowsExercised: [...officialRows].sort(),
          officialSourceRowCount: officialRows.size,
          broadcastBothDirections: "pass",
          binaryBroadcastAndAckBothDirections: "pass",
          roomBroadcastBothDirections: "pass",
          broadcastAckBothDirections: "pass",
          fetchSocketsBothDirections: "pass",
          serverSideEmitWithAckBothDirections: "pass",
          socketsJoinLeaveBothDirections: "pass",
          remoteDisconnectBothDirections: "pass",
          redisConnectionResetRecovery: "pass",
          sustainedRedisUnavailabilityRecovery: toxiproxyAPI
            ? "pass"
            : "not-configured",
          redisProcessRestartRecovery: redisRestartContainer
            ? "pass"
            : "not-configured",
          abruptNodeExitConvergence: "pass",
          rollingAdapterUpgrade:
            adapterMode === "sharded"
              ? "go-node-compatible-with-both-generations"
              : "pass",
          officialOldToCurrentNodeDirectInterop:
            adapterMode === "sharded"
              ? "unsupported-by-official-wire-format"
              : "pass",
          abruptGoExitConvergence: "pass",
          postFaultOrderedBurstBothDirections: "pass",
          connectionStateRecoveryBothDirections:
            adapterMode === "streams" ? "pass" : "not-applicable",
        },
        null,
        2,
      ),
    );
  } finally {
    if (workerProcess && workerProcess.exitCode === null)
      workerProcess.kill("SIGKILL");
    if (
      replacementWorkerProcess &&
      replacementWorkerProcess.exitCode === null
    ) {
      replacementWorkerProcess.kill("SIGKILL");
    }
    if (workerClient) workerClient.close();
    if (replacementWorkerClient) replacementWorkerClient.close();
    if (goWorkerClient) goWorkerClient.close();
    if (customGoClient) customGoClient.close();
    if (customNodeClient) customNodeClient.close();
    for (const socket of recoveryClients) socket.close();
    if (goClient) goClient.close();
    if (nodeClient) nodeClient.close();
  }
}

main().then(
  () => process.exit(0),
  (error) => {
    console.error(error);
    process.exit(1);
  },
);
