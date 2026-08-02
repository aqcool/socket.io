"use strict";

const assert = require("node:assert/strict");
const http = require("node:http");
const { io } = require("socket.io-client");
const { Socket: EngineSocket } = require("engine.io-client");

const baseUrl = process.env.SOCKET_IO_CLUSTER_URL;
if (!baseUrl) {
  throw new Error("SOCKET_IO_CLUSTER_URL is required");
}

let currentStage = "startup";
const matrixTimeoutMs = Number(
  process.env.SOCKET_IO_CLUSTER_MATRIX_TIMEOUT_MS || 90000,
);
const watchdog = setTimeout(() => {
  const handles = process
    ._getActiveHandles()
    .map((handle) => handle?.constructor?.name || typeof handle)
    .sort();
  process.stderr.write(
    `${JSON.stringify({ type: "WATCHDOG", stage: currentStage, handles })}\n`,
  );
  process.exit(124);
}, matrixTimeoutMs);

function stage(value) {
  currentStage = value;
}

const timeout = (ms, label) =>
  new Promise((_, reject) =>
    setTimeout(() => reject(new Error(`timeout: ${label}`)), ms),
  );

function once(socket, event, label = event) {
  return Promise.race([
    new Promise((resolve) => socket.once(event, (...args) => resolve(args))),
    timeout(5000, label),
  ]);
}

function emitAck(socket, event, ...args) {
  return Promise.race([
    new Promise((resolve, reject) => {
      socket.timeout(8000).emit(event, ...args, (err, value) => {
        if (err) reject(err);
        else resolve(value);
      });
    }),
    timeout(9000, `${event} acknowledgement`),
  ]);
}

async function connect(namespace = "", transports) {
  const socket = io(baseUrl + namespace, {
    autoConnect: false,
    forceNew: true,
    reconnection: false,
    transports,
  });
  const nodePromise = once(socket, "node", "node assignment");
  socket.connect();
  await once(socket, "connect");
  const [node] = await nodePromise;
  socket.nodeId = node;
  return socket;
}

async function expectEventOn(sockets, event, trigger, check) {
  const received = sockets.map((socket, index) =>
    once(socket, event, `${event}[${index}]`).then((args) => {
      if (check) check(args, index);
      return args;
    }),
  );
  await trigger();
  return Promise.all(received);
}

async function connectEngine(transports) {
  const engine = new EngineSocket(baseUrl, {
    path: "/engine.io",
    transports,
    upgrade: transports.includes("websocket"),
  });
  const nodePromise = once(engine, "message", "Engine.IO node assignment");
  await once(engine, "open", "Engine.IO open");
  const [nodeMessage] = await nodePromise;
  engine.nodeId = String(nodeMessage).replace(/^node:/, "");
  return engine;
}

function getStatus(url) {
  return new Promise((resolve, reject) => {
    const request = http.get(
      url,
      { agent: false, headers: { connection: "close" } },
      (response) => {
        response.resume();
        response.once("end", () => resolve(response.statusCode));
      },
    );
    request.setTimeout(5000, () => {
      request.destroy(new Error("timeout: HTTP status request"));
    });
    request.once("error", reject);
  });
}

function closeEngine(engine, label) {
  if (!engine || engine.readyState === "closed") return Promise.resolve();
  return new Promise((resolve, reject) => {
    let settled = false;
    const timer = setTimeout(() => {
      if (settled) return;
      settled = true;
      reject(new Error(`timeout: ${label} Engine.IO close`));
    }, 5000);
    engine.once("close", () => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      resolve();
    });
    engine.close();
  });
}

async function checkClusterEngine() {
  const engines = [];
  try {
    stage("engine:connect-polling");
    for (let index = 0; index < 3; index++)
      engines.push(await connectEngine(["polling"]));
    assert.deepEqual(
      new Set(engines.map((engine) => engine.nodeId)),
      new Set(["node-1", "node-2", "node-3"]),
    );

    stage("engine:text-round-trip");
    for (const [index, engine] of engines.entries()) {
      const text = `text-${index}`;
      const echoed = once(engine, "message", "Engine.IO text echo");
      engine.send(text);
      assert.equal(String((await echoed)[0]), text);
    }

    stage("engine:binary-round-trip");
    const binaryEcho = once(engines[0], "message", "Engine.IO binary echo");
    engines[0].send(Buffer.from([1, 2, 3, 4]));
    assert.deepEqual([...(await binaryEcho)[0]], [1, 2, 3, 4]);

    stage("engine:ordered-writes");
    const ordered = [];
    const orderedDone = new Promise((resolve) => {
      const listener = (value) => {
        ordered.push(String(value));
        if (ordered.length === 6) {
          engines[1].off("message", listener);
          resolve();
        }
      };
      engines[1].on("message", listener);
    });
    for (let value = 1; value <= 6; value++) engines[1].send(String(value));
    await Promise.race([
      orderedDone,
      timeout(5000, "Engine.IO ordered writes"),
    ]);
    assert.deepEqual(ordered, ["1", "2", "3", "4", "5", "6"]);

    stage("engine:deferred-response");
    const deferred = once(engines[2], "message", "Engine.IO deferred read");
    engines[2].send("__deferred__");
    assert.equal(String((await deferred)[0]), "deferred");

    stage("engine:liveness");
    await new Promise((resolve) => setTimeout(resolve, 300));
    assert.ok(engines.every((engine) => engine.readyState === "open"));

    stage("engine:upgrade");
    const upgraded = await connectEngine(["polling", "websocket"]);
    engines.push(upgraded);
    if (upgraded.transport.name !== "websocket")
      await once(upgraded, "upgrade", "Engine.IO upgrade");
    const afterUpgrade = once(
      upgraded,
      "message",
      "Engine.IO echo after upgrade",
    );
    upgraded.send("after-upgrade");
    assert.equal(String((await afterUpgrade)[0]), "after-upgrade");

    stage("engine:invalid-session");
    const invalidStatus = await getStatus(
      `${baseUrl}/engine.io/?EIO=4&transport=polling&sid=01234567890123456789`,
    );
    assert.equal(invalidStatus, 400);

    stage("engine:remote-close");
    const closed = once(engines[0], "close", "Engine.IO remote close");
    engines[0].send("__close__");
    await closed;

    return {
      // This deployment deliberately uses owner-affine routing instead of the
      // packet-level cross-worker IPC implemented by @socket.io/cluster-engine.
      // Keep these as named gateway assertions: a numeric "18/18" would imply
      // that the official cross-worker read locks and Redis transports ran here.
      engineGatewayBoundary: "sticky-owner",
      engineGatewayAssertions: {
        workerDistribution: "pass",
        pollingRoundTrip: "pass",
        binaryRoundTrip: "pass",
        orderedWrites: "pass",
        deferredResponse: "pass",
        connectionLiveness: "pass",
        websocketUpgrade: "pass",
        invalidSession: "pass",
        remoteClose: "pass",
      },
    };
  } finally {
    stage("engine:cleanup");
    await Promise.all(
      engines.map((engine, index) =>
        closeEngine(engine, `raw client ${index}`),
      ),
    );
  }
}

async function main() {
  const clients = [];
  const customClients = [];
  try {
    stage("socket:connect");
    for (let index = 0; index < 6; index++) {
      clients.push(
        await connect("", index < 3 ? ["polling"] : ["polling", "websocket"]),
      );
    }
    assert.deepEqual(
      new Set(clients.map((client) => client.nodeId)),
      new Set(["node-1", "node-2", "node-3"]),
    );

    stage("socket:adapter-matrix");
    await expectEventOn(
      clients,
      "cluster-text",
      async () => {
        await emitAck(clients[0], "control-broadcast", "hello");
      },
      ([value]) => assert.equal(value, "hello"),
    );

    await expectEventOn(
      clients,
      "cluster-binary",
      async () => {
        await emitAck(
          clients[1],
          "control-binary-broadcast",
          Buffer.from([1, 2, 3, 4]),
        );
      },
      ([value]) => assert.deepEqual([...value], [1, 2, 3, 4]),
    );

    const roomMembers = [clients[0], clients[2], clients[4]];
    const roomOutsiders = [clients[1], clients[3], clients[5]];
    for (const client of roomMembers) await emitAck(client, "join", "room-a");
    let outsiderEvents = 0;
    roomOutsiders.forEach((client) =>
      client.on("room-event", () => outsiderEvents++),
    );
    await expectEventOn(
      roomMembers,
      "room-event",
      async () => {
        await emitAck(clients[5], "control-room-broadcast", "room-a", "room");
      },
      ([value]) => assert.equal(value, "room"),
    );
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(outsiderEvents, 0);

    await emitAck(clients[0], "join", "excluded");
    let excludedEvents = 0;
    clients[0].on("except-event", () => excludedEvents++);
    await expectEventOn(
      clients.slice(1),
      "except-event",
      async () => {
        await emitAck(
          clients[1],
          "control-except-broadcast",
          "excluded",
          "except",
        );
      },
      ([value]) => assert.equal(value, "except"),
    );
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(excludedEvents, 0);

    const localNode = clients[0].nodeId;
    const localClients = clients.filter(
      (client) => client.nodeId === localNode,
    );
    const remoteClients = clients.filter(
      (client) => client.nodeId !== localNode,
    );
    let remoteLocalEvents = 0;
    remoteClients.forEach((client) =>
      client.on("local-event", () => remoteLocalEvents++),
    );
    await expectEventOn(localClients, "local-event", async () => {
      await emitAck(clients[0], "control-local-broadcast", "local");
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(remoteLocalEvents, 0);

    clients.forEach((client, index) =>
      client.on("cluster-ack", (value, ack) => ack(`${value}:${index}`)),
    );
    const acknowledgements = await emitAck(
      clients[0],
      "control-broadcast-ack",
      "ack",
    );
    assert.equal(acknowledgements.length, 6);
    assert.equal(new Set(acknowledgements).size, 6);

    clients.forEach((client, index) =>
      client.on("cluster-binary-ack", (_value, ack) =>
        ack(Buffer.from([index])),
      ),
    );
    const binaryAcknowledgements = await emitAck(
      clients[0],
      "control-binary-broadcast-ack",
      Buffer.from([9]),
    );
    assert.equal(binaryAcknowledgements.length, 6);
    assert.deepEqual(
      binaryAcknowledgements.map((value) => value[0]).sort(),
      [0, 1, 2, 3, 4, 5],
    );

    const emptyAcknowledgements = await emitAck(
      clients[0],
      "control-empty-broadcast-ack",
      "empty",
    );
    assert.deepEqual(emptyAcknowledgements, { responses: [], timedOut: false });
    clients.forEach((client, index) =>
      client.on("cluster-timeout-ack", (_value, ack) => {
        if (index !== 5) ack(index);
      }),
    );
    const timedOutAcknowledgements = await emitAck(
      clients[0],
      "control-timeout-broadcast-ack",
      "timeout",
    );
    assert.equal(timedOutAcknowledgements.timedOut, true);
    assert.equal(timedOutAcknowledgements.responses.length, 5);

    assert.equal(await emitAck(clients[0], "control-fetch-sockets"), 6);

    await emitAck(clients[0], "control-sockets-join", "joined-remotely");
    await new Promise((resolve) => setTimeout(resolve, 100));
    for (const client of clients)
      assert.equal(
        await emitAck(client, "control-has-room", "joined-remotely"),
        true,
      );
    await emitAck(clients[0], "control-sockets-leave", "joined-remotely");
    await new Promise((resolve) => setTimeout(resolve, 100));
    for (const client of clients)
      assert.equal(
        await emitAck(client, "control-has-room", "joined-remotely"),
        false,
      );

    const matching = [clients[0], clients[3]];
    for (const client of matching)
      await emitAck(client, "join", "matching-source");
    await emitAck(
      clients[1],
      "control-sockets-join-matching",
      "matching-source",
      "matching-target",
    );
    await new Promise((resolve) => setTimeout(resolve, 100));
    for (const client of clients) {
      assert.equal(
        await emitAck(client, "control-has-room", "matching-target"),
        matching.includes(client),
      );
    }
    await emitAck(
      clients[1],
      "control-sockets-leave-matching",
      "matching-source",
      "matching-target",
    );
    await new Promise((resolve) => setTimeout(resolve, 100));
    for (const client of clients)
      assert.equal(
        await emitAck(client, "control-has-room", "matching-target"),
        false,
      );

    const serverResponses = await emitAck(
      clients[0],
      "control-server-side-ack",
      "request",
    );
    assert.equal(serverResponses.length, 2);
    const expectedServerResponses = ["node-1", "node-2", "node-3"]
      .filter((node) => node !== clients[0].nodeId)
      .map((node) => `${node}:request`);
    assert.deepEqual(
      new Set(serverResponses),
      new Set(expectedServerResponses),
    );

    const observingClients = clients.filter(
      (client) => client.nodeId !== clients[0].nodeId,
    );
    const localObservers = clients.filter(
      (client) => client.nodeId === clients[0].nodeId,
    );
    let localServerEventLeaks = 0;
    localObservers.forEach((client) =>
      client.on("server-side-observed", () => localServerEventLeaks++),
    );
    await expectEventOn(observingClients, "server-side-observed", async () => {
      await emitAck(clients[0], "control-server-side-event", "event");
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(localServerEventLeaks, 0);

    const serverTimeout = await emitAck(
      clients[0],
      "control-server-side-timeout",
      "timeout",
    );
    assert.equal(serverTimeout.timedOut, true);
    assert.ok(serverTimeout.responses.length < 2);

    for (let index = 0; index < 3; index++)
      customClients.push(await connect("/custom", ["polling"]));
    let rootLeak = 0;
    clients.forEach((client) => client.on("custom-event", () => rootLeak++));
    await expectEventOn(
      customClients,
      "custom-event",
      async () => {
        await emitAck(customClients[0], "control-custom-broadcast", "custom");
      },
      ([value]) => assert.equal(value, "custom"),
    );
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(rootLeak, 0);

    stage("engine:start");
    const engineResults = await checkClusterEngine();

    stage("socket:remote-disconnect");
    const disconnectTargets = [clients[1], clients[2]];
    for (const client of disconnectTargets)
      await emitAck(client, "join", "disconnect-room");
    const disconnects = disconnectTargets.map((client) =>
      once(client, "disconnect", "remote disconnect"),
    );
    await emitAck(clients[0], "control-disconnect-room", "disconnect-room");
    await Promise.all(disconnects);
    assert.equal(clients[0].connected, true);
    assert.equal(clients[3].connected, true);

    const stillConnected = clients.filter((client) => client.connected);
    const allDisconnects = stillConnected.map((client) =>
      once(client, "disconnect", "disconnect all"),
    );
    await emitAck(stillConnected[0], "control-disconnect-all");
    await Promise.all(allDisconnects);

    stage("complete");
    process.stdout.write(
      JSON.stringify({
        clusterAdapterOfficialCases: 18,
        ...engineResults,
        pollingSticky: "pass",
        websocketUpgrade: clients
          .slice(3)
          .every((client) => client.io.engine.transport.name === "websocket"),
        namespaceIsolation: "pass",
        binary: "pass",
      }) + "\n",
    );
  } finally {
    stage("socket:cleanup");
    await Promise.all(
      [...customClients, ...clients].map(async (socket, index) => {
        const engine = socket.io?.engine;
        socket.disconnect();
        await closeEngine(engine, `Socket.IO client ${index}`);
      }),
    );
  }
}

main()
  .then(() => {
    stage("complete:waiting-for-event-loop");
  })
  .catch((error) => {
    console.error(error.stack || error);
    process.exitCode = 1;
  })
  .finally(() => {
    // Do not let the watchdog itself keep an otherwise clean run alive. If
    // another handle leaks, the unref'ed timer still fires and reports it.
    watchdog.unref();
  });
