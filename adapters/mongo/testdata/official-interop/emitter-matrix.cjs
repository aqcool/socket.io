"use strict";

const assert = require("node:assert/strict");
const { createServer } = require("node:http");
const { Server } = require("socket.io");
const { io: createClient } = require("socket.io-client");
const { MongoClient } = require("mongodb");
const { createAdapter } = require("@socket.io/mongo-adapter");
const { Emitter } = require("@socket.io/mongo-emitter");

const [goURL, mongoURI, databaseName] = process.argv.slice(2);
if (!goURL || !mongoURI || !databaseName) {
  throw new Error(
    "usage: node emitter-matrix.cjs <go-url> <mongo-uri> <database-name>",
  );
}

const timeoutMs = 10_000;
const propagationDelayMs = 450;
const delay = (duration) =>
  new Promise((resolve) => setTimeout(resolve, duration));

function waitFor(socket, event, timeout = timeoutMs) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      socket.off(event, onEvent);
      reject(new Error(`timeout waiting for ${event}`));
    }, timeout);
    const onEvent = (...args) => {
      clearTimeout(timer);
      resolve(args);
    };
    socket.once(event, onEvent);
  });
}

function expectNoEvent(socket, event, duration = 600) {
  return new Promise((resolve, reject) => {
    const onEvent = (...args) => {
      clearTimeout(timer);
      reject(new Error(`unexpected ${event}: ${JSON.stringify(args)}`));
    };
    const timer = setTimeout(() => {
      socket.off(event, onEvent);
      resolve();
    }, duration);
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

async function connect(url, namespace = "") {
  const socket = createClient(`${url}${namespace}`, {
    forceNew: true,
    reconnection: false,
    transports: ["websocket"],
    timeout: timeoutMs,
  });
  const connected = waitFor(socket, "connect");
  const failed = waitFor(socket, "connect_error").then(([error]) => {
    throw error;
  });
  await Promise.race([connected, failed]);
  return socket;
}

async function closeSocket(socket) {
  if (!socket) return;
  socket.removeAllListeners();
  socket.close();
  await delay(20);
}

async function expectBoth(
  goSocket,
  nodeSocket,
  event,
  trigger,
  validate = () => {},
) {
  const goEvent = waitFor(goSocket, event).catch((error) => {
    throw new Error(`Go adapter: ${error.message}`);
  });
  const nodeEvent = waitFor(nodeSocket, event).catch((error) => {
    throw new Error(`Node adapter: ${error.message}`);
  });
  await trigger();
  const [goArgs, nodeArgs] = await Promise.all([goEvent, nodeEvent]);
  validate(goArgs);
  validate(nodeArgs);
}

async function expectOnly(
  expected,
  excluded,
  event,
  trigger,
  validate = () => {},
) {
  const expectedEvent = waitFor(expected, event);
  const excludedEvent = expectNoEvent(excluded, event);
  await trigger();
  const [args] = await Promise.all([expectedEvent, excludedEvent]);
  validate(args);
}

async function expectNone(first, second, event, trigger) {
  const firstEvent = expectNoEvent(first, event);
  const secondEvent = expectNoEvent(second, event);
  await trigger();
  await Promise.all([firstEvent, secondEvent]);
}

async function join(socket, room) {
  const [result] = await emitAck(socket, "interop-join", room);
  assert.equal(result, "joined");
}

async function runOfficialTwelve(emitter, goURL, nodeURL, nodeServer) {
  let goSocket = await connect(goURL);
  let nodeSocket = await connect(nodeURL);
  let customGoSocket;
  let customNodeSocket;
  let assertions = 0;
  try {
    await delay(propagationDelayMs);

    await expectBoth(
      goSocket,
      nodeSocket,
      "emitter-all",
      () => {
        emitter.emit("emitter-all", 1, "2", Buffer.from([3, 4]));
      },
      ([one, two, binary]) => {
        assert.equal(one, 1);
        assert.equal(two, "2");
        assert.deepEqual(Buffer.from(binary), Buffer.from([3, 4]));
      },
    );
    assertions++;

    customGoSocket = await connect(goURL, "/custom");
    customNodeSocket = await connect(nodeURL, "/custom");
    await delay(propagationDelayMs);
    await expectBoth(
      customGoSocket,
      customNodeSocket,
      "emitter-namespace",
      () => {
        emitter.of("/custom").emit("emitter-namespace", "custom");
      },
      ([value]) => assert.equal(value, "custom"),
    );
    assertions++;

    await join(goSocket, "emitter-room");
    await expectOnly(goSocket, nodeSocket, "emitter-room-event", () => {
      emitter.to("emitter-room").emit("emitter-room-event", "room");
    });
    assertions++;

    await join(goSocket, "emitter-excluded");
    await expectOnly(nodeSocket, goSocket, "emitter-except-event", () => {
      emitter.except("emitter-excluded").emit("emitter-except-event", "except");
    });
    assertions++;

    emitter.socketsJoin("emitter-join-all");
    await delay(propagationDelayMs);
    await expectBoth(goSocket, nodeSocket, "emitter-join-all-event", () => {
      emitter.to("emitter-join-all").emit("emitter-join-all-event", "joined");
    });
    assertions++;

    await join(goSocket, "emitter-join-selector");
    emitter.in("emitter-join-selector").socketsJoin("emitter-join-matching");
    await delay(propagationDelayMs);
    await expectOnly(
      goSocket,
      nodeSocket,
      "emitter-join-matching-event",
      () => {
        emitter
          .to("emitter-join-matching")
          .emit("emitter-join-matching-event", "joined");
      },
    );
    assertions++;

    emitter.in(goSocket.id).socketsJoin("emitter-join-sid");
    await delay(propagationDelayMs);
    await expectOnly(goSocket, nodeSocket, "emitter-join-sid-event", () => {
      emitter.to("emitter-join-sid").emit("emitter-join-sid-event", "joined");
    });
    assertions++;

    await Promise.all([
      join(goSocket, "emitter-leave-all"),
      join(nodeSocket, "emitter-leave-all"),
    ]);
    emitter.socketsLeave("emitter-leave-all");
    await delay(propagationDelayMs);
    await expectNone(goSocket, nodeSocket, "emitter-leave-all-event", () => {
      emitter.to("emitter-leave-all").emit("emitter-leave-all-event", "left");
    });
    assertions++;

    await Promise.all([
      join(goSocket, "emitter-leave-matching"),
      join(nodeSocket, "emitter-leave-matching"),
    ]);
    await join(goSocket, "emitter-leave-selector");
    emitter.in("emitter-leave-selector").socketsLeave("emitter-leave-matching");
    await delay(propagationDelayMs);
    await expectOnly(
      nodeSocket,
      goSocket,
      "emitter-leave-matching-event",
      () => {
        emitter
          .to("emitter-leave-matching")
          .emit("emitter-leave-matching-event", "left");
      },
    );
    assertions++;

    await Promise.all([
      join(goSocket, "emitter-leave-sid"),
      join(nodeSocket, "emitter-leave-sid"),
    ]);
    emitter.in(goSocket.id).socketsLeave("emitter-leave-sid");
    await delay(propagationDelayMs);
    await expectOnly(nodeSocket, goSocket, "emitter-leave-sid-event", () => {
      emitter.to("emitter-leave-sid").emit("emitter-leave-sid-event", "left");
    });
    assertions++;

    const nodeServerSide = new Promise((resolve) =>
      nodeServer.once("official-emitter-server-side", (...args) =>
        resolve(args),
      ),
    );
    const goServerSide = waitFor(goSocket, "official-emitter-server-side-seen");
    emitter.serverSideEmit("official-emitter-server-side", "official-node");
    assert.deepEqual(await nodeServerSide, ["official-node"]);
    assert.deepEqual(await goServerSide, ["official-node"]);
    assertions++;

    const goDisconnected = waitFor(goSocket, "disconnect");
    const nodeDisconnected = waitFor(nodeSocket, "disconnect");
    emitter.disconnectSockets();
    assert.equal((await goDisconnected)[0], "io server disconnect");
    assert.equal((await nodeDisconnected)[0], "io server disconnect");
    assertions++;
    assert.equal(assertions, 12);
  } finally {
    await closeSocket(goSocket);
    await closeSocket(nodeSocket);
    await closeSocket(customGoSocket);
    await closeSocket(customNodeSocket);
  }
}

async function runGoEmitterSmoke(goURL, nodeURL, nodeServer) {
  let goSocket = await connect(goURL);
  let nodeSocket = await connect(nodeURL);
  let customGoSocket;
  let customNodeSocket;
  const command = async (...args) => {
    const [error] = await emitAck(
      goSocket,
      "interop-go-emitter-command",
      ...args,
    );
    assert.equal(error, null);
  };
  try {
    await delay(propagationDelayMs);
    await expectBoth(
      goSocket,
      nodeSocket,
      "go-emitter-broadcast",
      () => command("broadcast", "go-emitter-broadcast", "go"),
      ([label, binary]) => {
        assert.equal(label, "go");
        assert.deepEqual(Buffer.from(binary), Buffer.from([1, 2, 3]));
      },
    );

    customGoSocket = await connect(goURL, "/custom");
    customNodeSocket = await connect(nodeURL, "/custom");
    await delay(propagationDelayMs);
    await expectBoth(
      customGoSocket,
      customNodeSocket,
      "go-emitter-namespace",
      () => command("namespace", "go-emitter-namespace", "custom"),
    );

    await join(nodeSocket, "go-emitter-room");
    await expectOnly(nodeSocket, goSocket, "go-emitter-room-event", () =>
      command("room", "go-emitter-room", "go-emitter-room-event", "room"),
    );
    await join(goSocket, "go-emitter-excluded");
    await expectOnly(nodeSocket, goSocket, "go-emitter-except-event", () =>
      command(
        "except",
        "go-emitter-excluded",
        "go-emitter-except-event",
        "except",
      ),
    );

    await command("join", nodeSocket.id, "go-emitter-joined");
    await delay(propagationDelayMs);
    await expectOnly(nodeSocket, goSocket, "go-emitter-joined-event", () =>
      command("room", "go-emitter-joined", "go-emitter-joined-event", "joined"),
    );
    await command("leave", nodeSocket.id, "go-emitter-joined");
    await delay(propagationDelayMs);
    await expectNone(goSocket, nodeSocket, "go-emitter-left-event", () =>
      command("room", "go-emitter-joined", "go-emitter-left-event", "left"),
    );

    const serverSide = new Promise((resolve) =>
      nodeServer.once("go-emitter-server-side", (...args) => resolve(args)),
    );
    await command("server-side", "go-emitter-server-side", "go");
    assert.deepEqual(await serverSide, ["go"]);
    const disconnected = waitFor(nodeSocket, "disconnect");
    await command("disconnect", nodeSocket.id);
    assert.equal((await disconnected)[0], "io server disconnect");
  } finally {
    await closeSocket(goSocket);
    await closeSocket(nodeSocket);
    await closeSocket(customGoSocket);
    await closeSocket(customNodeSocket);
  }
}

async function main() {
  const mongoClient = new MongoClient(mongoURI);
  await mongoClient.connect();
  const collection = mongoClient.db(databaseName).collection("events");
  await collection.createIndex({ createdAt: 1 }, { expireAfterSeconds: 60 });

  const httpServer = createServer();
  const io = new Server(httpServer, { transports: ["websocket"] });
  io.adapter(
    createAdapter(collection, {
      addCreatedAtField: true,
      heartbeatInterval: 200,
      heartbeatTimeout: 600,
    }),
  );
  io.of("/custom");
  io.on("connection", (socket) => {
    socket.on("interop-join", (room, ack) => {
      socket.join(room);
      ack("joined");
    });
  });

  await new Promise((resolve) => httpServer.listen(0, "127.0.0.1", resolve));
  const nodeURL = `http://127.0.0.1:${httpServer.address().port}`;
  const emitter = new Emitter(collection, "/", { addCreatedAtField: true });
  try {
    await delay(1000);
    await runOfficialTwelve(emitter, goURL, nodeURL, io);
    await runGoEmitterSmoke(goURL, nodeURL, io);
    process.stdout.write(
      `${JSON.stringify({
        emitter: "@socket.io/mongo-emitter@0.2.0",
        officialBehaviorMatrix: "12/12",
        officialEmitterToGoAdapter: "pass",
        goEmitterToOfficialAdapter: "pass",
        externalServiceGate: "SOCKET_IO_MONGO_OFFICIAL_INTEROP=1",
      })}\n`,
    );
  } finally {
    await new Promise((resolve) => io.close(resolve));
    if (httpServer.listening)
      await new Promise((resolve) => httpServer.close(resolve));
    await mongoClient.close();
  }
}

main().catch((error) => {
  console.error(error && error.stack ? error.stack : error);
  process.exitCode = 1;
});
