"use strict";

const assert = require("node:assert/strict");
const { io } = require("socket.io-client");

const baseUrl = process.env.SOCKET_IO_CLUSTER_URL;
const expectedPeers = Number(process.env.SOCKET_IO_CLUSTER_EXPECTED_PEERS);
if (!baseUrl || !Number.isInteger(expectedPeers)) throw new Error("missing fault matrix configuration");

function emitAck(socket, event, ...args) {
  return new Promise((resolve, reject) => {
    socket.timeout(7000).emit(event, ...args, (err, value) => err ? reject(err) : resolve(value));
  });
}

async function main() {
  const socket = io(baseUrl, { forceNew: true, reconnection: false, transports: ["polling"] });
  try {
    await new Promise((resolve, reject) => {
      socket.once("connect", resolve);
      socket.once("connect_error", reject);
    });
    assert.equal(await emitAck(socket, "control-fetch-sockets"), 1);
    const responses = await emitAck(socket, "control-server-side-ack", "convergence");
    assert.equal(responses.length, expectedPeers);
    const event = new Promise((resolve) => socket.once("cluster-text", resolve));
    await emitAck(socket, "control-broadcast", "after-change");
    assert.equal(await event, "after-change");
    process.stdout.write(JSON.stringify({ expectedPeers, convergence: "pass" }) + "\n");
  } finally {
    socket.disconnect();
  }
}

main().catch((error) => {
  console.error(error.stack || error);
  process.exitCode = 1;
});
