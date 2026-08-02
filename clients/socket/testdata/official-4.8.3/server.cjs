"use strict";

const http = require("node:http");
const { Server } = require("socket.io");

const httpServer = http.createServer();
const io = new Server(httpServer, {
  connectionStateRecovery: {
    maxDisconnectionDuration: 30_000,
    skipMiddlewares: true,
  },
});

if (process.env.SOCKET_IO_FIXTURE_TRACE === "1") {
  io.engine.on("connection", (engineSocket) => {
    engineSocket.on("packet", (packet) => {
      const data = Buffer.isBuffer(packet.data)
        ? `binary:${packet.data.toString("utf8")}`
        : `text:${String(packet.data)}`;
      process.stderr.write(`fixture packet ${packet.type} ${data}\n`);
    });
  });
}

let connectionSequence = 0;

function installSocketHandlers(socket) {
  connectionSequence++;
  let receivedOrderedBinary = false;
  socket.on("echo", (...args) => {
    const ack = typeof args.at(-1) === "function" ? args.pop() : null;
    if (ack) ack(...args);
  });
  socket.on("never-ack", () => {});
  socket.on("delayed-ack", (delay, value, ack) => {
    setTimeout(() => ack(value), delay);
  });
  socket.on("receive-date", (value, ack) => ack(typeof value, value));
  socket.on("receive-date-object", (value, ack) =>
    ack(typeof value.when, value.when),
  );
  socket.on("retry-never", () => socket.emit("retry-seen"));
  socket.on("get-id", (ack) => ack(socket.id));
  socket.on("date-ack", (ack) => ack(new Date("2024-01-02T03:04:05.000Z")));
  socket.on("request-client-ack", (ack) => {
    socket.emit("client-ack", 5, { test: true }, (...values) => ack(...values));
  });
  socket.on("get-handshake", (ack) => {
    ack({ query: socket.handshake.query, auth: socket.handshake.auth });
  });
  socket.on("binary", (value, ack) => ack(value));
  socket.on("binary-nested", (value, ack) => ack(value));
  socket.on("ordered-binary-first", (value) => {
    receivedOrderedBinary =
      Buffer.isBuffer(value) && value.equals(Buffer.from("binary-first"));
  });
  socket.on("ordered-binary-second", (value) => {
    socket.emit("ordered-binary-ack", receivedOrderedBinary, value);
  });
  socket.on("ordered", (value, ack) => ack(value));
  socket.on("connection-sequence", (ack) => ack(connectionSequence));
  socket.on("init-recovery", () => socket.emit("recovery-event", "ready"));
  socket.on("disconnect-namespace", () => socket.disconnect());
}

io.on("connection", installSocketHandlers);
io.of("/custom").on("connection", installSocketHandlers);

io.of("/auth")
  .use((socket, next) => {
    if (socket.handshake.auth.token === "accepted") return next();
    next(new Error("not authorized"));
  })
  .on("connection", installSocketHandlers);

io.of("/reject")
  .use((_socket, next) => next(new Error("middleware rejected")))
  .on("connection", installSocketHandlers);

httpServer.listen(0, "127.0.0.1", () => {
  const address = httpServer.address();
  process.stdout.write(JSON.stringify({ type: "READY", port: address.port }) + "\n");
});

function shutdown() {
  const forceExit = setTimeout(() => process.exit(0), 1_000);
  forceExit.unref();
  io.close(() => process.exit(0));
}

process.on("SIGTERM", shutdown);
process.on("SIGINT", shutdown);
