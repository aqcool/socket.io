"use strict";

const assert = require("node:assert/strict");

const clients = {
  "3": { module: require("engine.io-client-v3"), protocol: 3 },
  "4": { module: require("engine.io-client-v4"), protocol: 4 },
  "6": { module: require("engine.io-client-v6"), protocol: 4 },
};

const strictURL = process.argv[2];
const compatibilityURL = process.argv[3];
const selectedVersion = process.argv[4];
if (!strictURL || !compatibilityURL || !clients[selectedVersion]) {
  throw new Error("usage: node matrix.cjs <strict-url> <EIO3-compatible-url> <client-major>");
}

const selected = clients[selectedVersion];
const Socket = selected.module.Socket || selected.module;
const timeoutMs = 15000;
const sendPacketMethod = selectedVersion === "3" ? "sendPacket" : "_sendPacket";

function removeListener(socket, event, listener) {
  if (typeof socket.off === "function") socket.off(event, listener);
  else socket.removeListener(event, listener);
}

function waitFor(socket, event, timeout = timeoutMs) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      removeListener(socket, event, onEvent);
      reject(new Error(`timed out waiting for ${event}; readyState=${socket.readyState}`));
    }, timeout);
    const onEvent = (...args) => {
      clearTimeout(timer);
      resolve(args);
    };
    socket.once(event, onEvent);
  });
}

function waitForMessage(socket, expected, timeout = timeoutMs) {
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      removeListener(socket, "message", onMessage);
      reject(new Error(`timed out waiting for message ${expected}`));
    }, timeout);
    const onMessage = (message) => {
      if (message !== expected) return;
      clearTimeout(timer);
      removeListener(socket, "message", onMessage);
      resolve(message);
    };
    socket.on("message", onMessage);
  });
}

function openSocket(url, options) {
  return new Promise((resolve, reject) => {
    const socket = new Socket(url, options);
    const timer = setTimeout(() => {
      cleanup();
      socket.close();
      reject(new Error(`timed out opening socket; readyState=${socket.readyState}`));
    }, timeoutMs);
    const cleanup = () => {
      clearTimeout(timer);
      removeListener(socket, "open", onOpen);
      removeListener(socket, "error", onError);
    };
    const onOpen = () => {
      cleanup();
      resolve(socket);
    };
    const onError = (error) => {
      cleanup();
      reject(error instanceof Error ? error : new Error(String(error)));
    };
    socket.once("open", onOpen);
    socket.once("error", onError);
  });
}

async function closeSocket(socket) {
  if (!socket || socket.readyState === "closed") return;
  const closed = waitFor(socket, "close", 2000).catch(() => []);
  socket.close();
  await closed;
}

function bytes(value) {
  if (Buffer.isBuffer(value)) return value;
  if (value instanceof ArrayBuffer) return Buffer.from(value);
  if (ArrayBuffer.isView(value)) return Buffer.from(value.buffer, value.byteOffset, value.byteLength);
  throw new Error(`expected binary message, got ${Object.prototype.toString.call(value)}`);
}

async function checkTransport(url, transports, expectedTransport, expectUpgrade) {
  const socket = new Socket(url, {
    transports,
    upgrade: expectUpgrade,
    rememberUpgrade: false,
  });
  const opened = waitFor(socket, "open");
  const upgraded = expectUpgrade ? waitFor(socket, "upgrade") : Promise.resolve();
  try {
    const initialTransport = socket.transport && socket.transport.name;
    await opened;
    if (expectUpgrade) {
      assert.equal(initialTransport, "polling");
      await upgraded;
      await new Promise((resolve) => setTimeout(resolve, 350));
      assert.equal(socket.readyState, "open", "upgraded client must survive heartbeat cycles");
    }
    assert.equal(socket.transport.name, expectedTransport);
  } finally {
    await closeSocket(socket);
  }
}

async function checkTrafficDuringUpgrade(url) {
  const socket = new Socket(url, {
    transports: ["polling", "websocket"],
    upgrade: true,
    rememberUpgrade: false,
  });
  let sent = 0;
  let serverReceived = 0;
  let echoesReceived = 0;
  let upgrading = 0;
  let upgraded = 0;
  let interval;
  const completed = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timed out during upgrade stress")), timeoutMs);
    const maybeComplete = () => {
      if (sent === 50 && serverReceived === 50 && echoesReceived === 50 && upgrading === 1 && upgraded === 1) {
        clearTimeout(timer);
        resolve();
      }
    };
    socket.on("message", (message) => {
      try {
        if (typeof message !== "string") return;
        if (message.startsWith("server-stress:")) {
          assert.equal(Number(message.slice("server-stress:".length)), ++serverReceived);
        } else if (message.startsWith("client-stress:")) {
          assert.equal(Number(message.slice("client-stress:".length)), ++echoesReceived);
        }
        maybeComplete();
      } catch (error) {
        clearTimeout(timer);
        reject(error);
      }
    });
    socket.on("upgrading", (transport) => {
      try {
        assert.equal(transport.name, "websocket");
        upgrading++;
      } catch (error) {
        clearTimeout(timer);
        reject(error);
      }
    });
    socket.on("upgrade", (transport) => {
      try {
        assert.equal(transport.name, "websocket");
        upgraded++;
        maybeComplete();
      } catch (error) {
        clearTimeout(timer);
        reject(error);
      }
    });
    socket.once("error", (error) => {
      clearTimeout(timer);
      reject(error instanceof Error ? error : new Error(String(error)));
    });
    socket.once("open", () => {
      socket.send("__upgrade_stress__");
      interval = setInterval(() => {
        socket.send(`client-stress:${++sent}`);
        if (sent === 50) {
          clearInterval(interval);
          maybeComplete();
        }
      }, 2);
    });
  });
  try {
    await completed;
  } finally {
    clearInterval(interval);
    await closeSocket(socket);
  }
}

async function checkMessagesAndHeartbeat(url) {
  const socket = new Socket(url, {
    transports: ["websocket"],
    upgrade: false,
  });
  const protocolMessage = waitFor(socket, "message");
  try {
    await waitFor(socket, "open");
    const [protocol] = await protocolMessage;
    assert.equal(protocol, `protocol:${selected.protocol}`);

    const textEcho = waitFor(socket, "message");
    socket.send(`hello-v${selectedVersion}`);
    assert.deepEqual(await textEcho, [`hello-v${selectedVersion}`]);

    const binary = Buffer.from([0, 1, 2, 127, 255]);
    const binaryEcho = waitFor(socket, "message");
    socket.send(binary);
    const [echoed] = await binaryEcho;
    assert.deepEqual(bytes(echoed), binary);

    await new Promise((resolve) => setTimeout(resolve, 350));
    assert.equal(socket.readyState, "open", "official client must survive multiple heartbeat cycles");
  } finally {
    await closeSocket(socket);
  }
}

async function checkServerInitiatedClose(url, transport) {
  const socket = await openSocket(url, {
    transports: [transport],
    upgrade: false,
  });
  try {
    const closed = waitFor(socket, "close");
    socket.send("__server_close__");
    const [reason] = await closed;
    assert.equal(reason, "transport close");
  } finally {
    await closeSocket(socket);
  }
}

async function checkClientInitiatedClose(url, transport) {
  const socket = await openSocket(url, {
    transports: [transport],
    upgrade: false,
  });
  try {
    const ready = waitForMessage(socket, "__close_ready__");
    socket.send("__client_will_close__");
    assert.equal(await ready, "__close_ready__");
    const closed = waitFor(socket, "close");
    socket.close();
    const [reason] = await closed;
    assert.equal(reason, "forced close");
  } finally {
    await closeSocket(socket);
  }
}

async function checkPingTimeoutOnBothEnds(url) {
  const socket = await openSocket(url, {
    transports: ["websocket"],
    upgrade: false,
  });
  try {
    const ready = waitForMessage(socket, "__timeout_ready__");
    socket.send("__timeout_ready__");
    assert.equal(await ready, "__timeout_ready__");
    socket[sendPacketMethod] = () => {};
    socket.transport.removeListener("packet");
    socket.transport.removeListener("close");
    const [reason] = await waitFor(socket, "close", 3000);
    assert.equal(reason, "ping timeout");
  } finally {
    if (socket.transport) socket.transport.close();
    await closeSocket(socket);
  }
}

async function checkClientCloseWhileProcessingPayload(url) {
  const socket = await openSocket(url, {
    transports: ["polling"],
    upgrade: false,
  });
  let receivedSecondPacket = false;
  const onMessage = (message) => {
    if (message === "__must_not_arrive__") receivedSecondPacket = true;
    if (message === "__close_now__") socket.close();
  };
  socket.on("message", onMessage);
  try {
    const closed = waitFor(socket, "close");
    socket.send("__close_in_payload__");
    const [reason] = await closed;
    assert.equal(reason, "forced close");
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(receivedSecondPacket, false);
  } finally {
    removeListener(socket, "message", onMessage);
    await closeSocket(socket);
  }
}

async function checkClientCloseDuringUpgrade(url) {
  const socket = new Socket(url, {
    transports: ["polling", "websocket"],
    upgrade: true,
    rememberUpgrade: false,
  });
  const opened = waitFor(socket, "open");
  const closed = waitFor(socket, "close");
  try {
    await opened;
    socket.close();
    const [reason] = await closed;
    assert.equal(reason, "forced close");
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(socket.readyState, "closed");
  } finally {
    if (socket.transport) socket.transport.close();
    await closeSocket(socket);
  }
}

async function checkServerToClientMessages(url, transport, binaryType) {
  const socket = new Socket(url, {
    transports: [transport],
    upgrade: false,
  });
  if (binaryType) socket.binaryType = binaryType;
  const received = new Promise((resolve, reject) => {
    const expectedText = ["a", "b", "c"];
    let textIndex = 0;
    const timer = setTimeout(() => reject(new Error("timed out waiting for server messages")), timeoutMs);
    socket.on("message", (message) => {
      if (typeof message === "string") {
        if (message.startsWith("protocol:")) return;
        if (message !== expectedText[textIndex]) {
          clearTimeout(timer);
          reject(new Error(`message ${textIndex} = ${message}`));
          return;
        }
        textIndex += 1;
        return;
      }
      try {
        assert.equal(textIndex, expectedText.length);
        assert.deepEqual(bytes(message), Buffer.from([0, 1, 2, 3, 4]));
        if (binaryType === "arraybuffer") assert.equal(message instanceof ArrayBuffer, true);
        clearTimeout(timer);
        resolve();
      } catch (error) {
        clearTimeout(timer);
        reject(error);
      }
    });
  });
  try {
    await waitFor(socket, "open");
    socket.send("__server_messages__");
    await received;
  } finally {
    await closeSocket(socket);
  }
}

async function checkOrderedMessagesBeforeClose(url, transport, delayed) {
  const socket = await openSocket(url, {
    transports: [transport],
    upgrade: false,
  });
  const received = [];
  socket.on("message", (message) => {
    if (["a", "b", "c"].includes(message)) received.push(message);
  });
  try {
    const closed = waitFor(socket, "close");
    socket.send(delayed ? "__ordered_close_delayed__" : "__ordered_close__");
    const [reason] = await closed;
    assert.equal(reason, "transport close");
    assert.deepEqual(received, ["a", "b", "c"]);
  } finally {
    if (socket.transport) socket.transport.close();
    await closeSocket(socket);
  }
}

async function checkUnicodeRoundTrip(url) {
  const unicodeMessages = [".", "石室詩士施氏，嗜獅，誓食十獅。", "氏時時適市視獅。"];
  const socket = await openSocket(url, {
    transports: ["polling"],
    upgrade: false,
  });
  let index = 0;
  let sentReply = false;
  const completed = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error("timed out waiting for Unicode messages")), timeoutMs);
    socket.on("message", (message) => {
      if (!unicodeMessages.includes(message)) return;
      const expected = unicodeMessages[index % unicodeMessages.length];
      if (message !== expected) {
        clearTimeout(timer);
        reject(new Error(`Unicode message ${index} = ${message}, want ${expected}`));
        return;
      }
      index += 1;
      if (index === unicodeMessages.length && !sentReply) {
        sentReply = true;
        for (const value of unicodeMessages) socket.send(value);
      }
      if (index === unicodeMessages.length * 2) {
        clearTimeout(timer);
        resolve();
      }
    });
  });
  try {
    socket.send("__unicode_roundtrip__");
    await completed;
  } finally {
    await closeSocket(socket);
  }
}

async function checkBufferedMessagesInterleaveWithPongs(url) {
  const socket = await openSocket(url, {
    transports: ["websocket"],
    upgrade: false,
  });
  let received = 0;
  const completed = new Promise((resolve, reject) => {
    const timer = setTimeout(() => reject(new Error(`received ${received}/100 burst messages`)), timeoutMs);
    socket.on("message", (message) => {
      if (typeof message !== "string" || !message.includes("|message: ")) return;
      const index = Number(message.slice(message.lastIndexOf(" ") + 1));
      if (index !== received) {
        clearTimeout(timer);
        reject(new Error(`burst message index = ${index}, want ${received}`));
        return;
      }
      received += 1;
      if (received === 100) {
        clearTimeout(timer);
        resolve();
      }
    });
  });
  try {
    socket.send("__message_burst__");
    await completed;
    await new Promise((resolve) => setTimeout(resolve, 350));
    assert.equal(socket.readyState, "open");
  } finally {
    await closeSocket(socket);
  }
}

async function expectStrictEIO3Rejection() {
  const socket = new Socket(strictURL, {
    transports: ["polling"],
    upgrade: false,
  });
  try {
    await Promise.race([
      waitFor(socket, "error"),
      waitFor(socket, "open").then(() => {
        throw new Error("EIO3 client unexpectedly connected to strict server");
      }),
    ]);
  } finally {
    await closeSocket(socket);
  }
}

async function main() {
  const url = selected.protocol === 3 ? compatibilityURL : strictURL;
  if (selected.protocol === 3) await expectStrictEIO3Rejection();
  await checkTransport(url, ["polling"], "polling", false);
  await checkTransport(url, ["websocket"], "websocket", false);
  await checkTransport(url, ["polling", "websocket"], "websocket", true);
  await checkTrafficDuringUpgrade(url);
  await checkMessagesAndHeartbeat(url);
  await checkServerInitiatedClose(url, "polling");
  await checkServerInitiatedClose(url, "websocket");
  await checkClientInitiatedClose(url, "polling");
  await checkClientInitiatedClose(url, "websocket");
  await checkPingTimeoutOnBothEnds(url);
  await checkClientCloseWhileProcessingPayload(url);
  await checkClientCloseDuringUpgrade(url);
  await checkServerToClientMessages(url, "polling");
  await checkServerToClientMessages(url, "websocket");
  await checkServerToClientMessages(url, "polling", "arraybuffer");
  await checkServerToClientMessages(url, "websocket", "arraybuffer");
  await checkOrderedMessagesBeforeClose(url, "polling", true);
  await checkOrderedMessagesBeforeClose(url, "websocket", true);
  await checkOrderedMessagesBeforeClose(url, "websocket", false);
  await checkUnicodeRoundTrip(url);
  await checkBufferedMessagesInterleaveWithPongs(url);
  process.stdout.write(JSON.stringify({
    client: `engine.io-client@${selectedVersion}`,
    protocol: selected.protocol,
    status: "ok",
  }) + "\n");
}

main().then(
  () => process.exit(0),
  (error) => {
    console.error(error && error.stack ? error.stack : error);
    process.exit(1);
  },
);
