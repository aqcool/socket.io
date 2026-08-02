"use strict";

const assert = require("node:assert/strict");
const http = require("node:http");
const readline = require("node:readline");
const { io } = require("socket.io-client");

// Control is newline-delimited JSON. The driver waits for stdout READY, writes
// PHASE start/end records to stdin around each real fault, and waits for the
// corresponding RECOVERY record before advancing. RESULT is always the final
// stdout record on success.

const baseUrl = process.env.SOCKET_IO_CLUSTER_URL;
const durationMs = numberFromEnv("SOCKET_IO_SOAK_DURATION_MS", NaN, 10000);
const clientCount = numberFromEnv("SOCKET_IO_SOAK_CLIENTS", 36, 3);
const ackIntervalMs = numberFromEnv(
  "SOCKET_IO_SOAK_ACK_INTERVAL_MS",
  2000,
  100,
);
const broadcastIntervalMs = numberFromEnv(
  "SOCKET_IO_SOAK_BROADCAST_INTERVAL_MS",
  2000,
  100,
);
const churnIntervalMs = numberFromEnv(
  "SOCKET_IO_SOAK_CHURN_INTERVAL_MS",
  7000,
  1000,
);
const ackTimeoutMs = numberFromEnv("SOCKET_IO_SOAK_ACK_TIMEOUT_MS", 1500, 250);
const recoveryTimeoutMs = numberFromEnv(
  "SOCKET_IO_SOAK_RECOVERY_TIMEOUT_MS",
  30000,
  5000,
);
const partitionDurationMs = numberFromEnv(
  "SOCKET_IO_SOAK_PARTITION_DURATION_MS",
  Math.floor(durationMs / 5),
  1,
);
const requirePhases = process.env.SOCKET_IO_SOAK_REQUIRE_PHASES === "1";

if (!baseUrl || !Number.isFinite(durationMs)) {
  throw new Error(
    "SOCKET_IO_CLUSTER_URL and a duration of at least 10 seconds are required",
  );
}

const TRANSPORT_POLLING = "polling-only";
const TRANSPORT_WEBSOCKET = "websocket-only";
const TRANSPORT_UPGRADE = "polling-to-websocket";
const EXPECTED_PHASES = ["partition", "rolling-restart"];

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

function numberFromEnv(name, fallback, minimum) {
  const raw = process.env[name];
  const value = raw === undefined || raw === "" ? fallback : Number(raw);
  if (!Number.isFinite(value) || value < minimum) {
    throw new Error(
      `${name} must be a number greater than or equal to ${minimum}`,
    );
  }
  return value;
}

function ratioFromEnv(name, fallback) {
  const value = numberFromEnv(name, fallback, 0);
  if (value > 1) throw new Error(`${name} must be less than or equal to 1`);
  return value;
}

function roundRatio(value) {
  return Math.round(value * 10000) / 10000;
}

function delta(after, before, key) {
  return after[key] - before[key];
}

async function waitUntil(predicate, timeoutMs, label) {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    if (await predicate()) return;
    await sleep(50);
  }
  throw new Error(`timeout: ${label}`);
}

function transportClassFor(index) {
  return [TRANSPORT_POLLING, TRANSPORT_WEBSOCKET, TRANSPORT_UPGRADE][index % 3];
}

function socketOptions(transportClass, agent) {
  const options = {
    autoConnect: false,
    forceNew: true,
    reconnection: true,
    reconnectionDelay: 500,
    reconnectionDelayMax: 2000,
    randomizationFactor: 0.5,
    timeout: 2000,
    agent,
  };
  if (transportClass === TRANSPORT_POLLING) {
    options.transports = ["polling"];
    options.upgrade = false;
  } else if (transportClass === TRANSPORT_WEBSOCKET) {
    options.transports = ["websocket"];
  } else {
    options.transports = ["polling", "websocket"];
  }
  return options;
}

function newClientState(index, socket) {
  return {
    index,
    socket,
    transportClass: transportClassFor(index),
    engines: new WeakSet(),
    observedTransports: new Set(),
    upgradeObserved: false,
    upgradeErrors: 0,
    currentNode: null,
    observedNodes: new Set(),
    nodeAssignments: [],
    connects: 0,
    disconnects: 0,
    deliveries: 0,
    ack: {
      attempts: 0,
      success: 0,
      timeouts: 0,
      mismatches: 0,
      duplicateCallbacks: 0,
      currentConsecutiveFailures: 0,
      longestConsecutiveFailures: 0,
      maxLatencyMs: 0,
    },
    finalRecovered: false,
  };
}

async function writeJson(record) {
  const line = `${JSON.stringify(record)}\n`;
  await new Promise((resolve, reject) => {
    process.stdout.write(line, (error) => (error ? reject(error) : resolve()));
  });
}

function countAgentHandles(agent) {
  const flatten = (collection) => Object.values(collection).flat();
  const active = flatten(agent.sockets);
  const free = flatten(agent.freeSockets);
  const queued = flatten(agent.requests);
  return {
    active: active.filter((socket) => !socket.destroyed).length,
    free: free.filter((socket) => !socket.destroyed).length,
    queued: queued.length,
    destroyedEntriesAwaitingRemoval: [...active, ...free].filter(
      (socket) => socket.destroyed,
    ).length,
  };
}

async function main() {
  const agent = new http.Agent({
    keepAlive: true,
    keepAliveMsecs: 1000,
    maxSockets: clientCount * 4,
    maxFreeSockets: clientCount * 2,
  });
  const states = [];
  const broadcastRecords = new Map();
  const phaseRecords = new Map();
  const controlErrors = [];
  let sequence = 0;
  let readyAt = 0;
  let running = true;
  let stopping = false;
  let recoveryInFlight = 0;
  let phaseQueue = Promise.resolve();
  let control;
  const workloadSleepers = new Set();
  const totals = {
    connects: 0,
    disconnects: 0,
    ackAttempts: 0,
    ackSuccess: 0,
    ackTimeouts: 0,
    ackMismatches: 0,
    duplicateAcks: 0,
    broadcastAttempts: 0,
    broadcastAckSuccess: 0,
    broadcastAckTimeouts: 0,
    broadcastDuplicateCallbacks: 0,
    expectedDeliveries: 0,
    eventDeliveries: 0,
    uniqueEventDeliveries: 0,
    duplicateEvents: 0,
    unknownEvents: 0,
  };

  const shortRun = durationMs < 60000;
  const budgets = {
    minimumAckSuccessRatio: ratioFromEnv(
      "SOCKET_IO_SOAK_MIN_ACK_SUCCESS_RATIO",
      shortRun ? 0.6 : 0.85,
    ),
    minimumPerClientAckSuccessRatio: ratioFromEnv(
      "SOCKET_IO_SOAK_MIN_CLIENT_ACK_SUCCESS_RATIO",
      shortRun ? 0.4 : 0.7,
    ),
    minimumBroadcastAckSuccessRatio: ratioFromEnv(
      "SOCKET_IO_SOAK_MIN_BROADCAST_ACK_SUCCESS_RATIO",
      shortRun ? 0.5 : 0.8,
    ),
    minimumBroadcastDeliveryRatio: ratioFromEnv(
      "SOCKET_IO_SOAK_MIN_BROADCAST_DELIVERY_RATIO",
      shortRun ? 0.6 : 0.85,
    ),
    maximumConsecutiveAckFailures: numberFromEnv(
      "SOCKET_IO_SOAK_MAX_CONSECUTIVE_ACK_FAILURES",
      Math.max(5, Math.ceil(partitionDurationMs / ackIntervalMs) + 10),
      1,
    ),
  };
  const configuration = {
    ackIntervalMs,
    broadcastIntervalMs,
    churnIntervalMs,
    ackTimeoutMs,
    recoveryTimeoutMs,
    partitionDurationMs,
    requirePhases,
  };

  function elapsedMs() {
    return readyAt === 0 ? 0 : Date.now() - readyAt;
  }

  function recordPhaseImpact(state) {
    for (const record of phaseRecords.values()) {
      if (
        record.endedAtMs === null &&
        record.targetClientIndexes.includes(state.index)
      ) {
        record.affectedClientIndexes.add(state.index);
      }
    }
  }

  function workloadSleep(ms) {
    if (!running) return Promise.resolve();
    return new Promise((resolve) => {
      const sleeper = { timer: undefined, resolve };
      sleeper.timer = setTimeout(() => {
        workloadSleepers.delete(sleeper);
        resolve();
      }, ms);
      workloadSleepers.add(sleeper);
    });
  }

  function stopWorkload() {
    running = false;
    for (const sleeper of workloadSleepers) {
      clearTimeout(sleeper.timer);
      sleeper.resolve();
    }
    workloadSleepers.clear();
  }

  function observeTransport(state, transport) {
    const name = typeof transport === "string" ? transport : transport?.name;
    if (name) state.observedTransports.add(name);
  }

  function attachEngineObservers(state) {
    const engine = state.socket.io.engine;
    if (!engine || state.engines.has(engine)) return;
    state.engines.add(engine);
    observeTransport(state, engine.transport);
    engine.on("upgrade", (transport) => {
      observeTransport(state, transport);
      if (
        state.observedTransports.has("polling") &&
        transport?.name === "websocket"
      ) {
        state.upgradeObserved = true;
      }
    });
    engine.on("upgradeError", () => state.upgradeErrors++);
  }

  function recordAckOutcome(state, outcome, latencyMs) {
    state.ack.maxLatencyMs = Math.max(state.ack.maxLatencyMs, latencyMs);
    if (outcome === "success") {
      state.ack.success++;
      totals.ackSuccess++;
      state.ack.currentConsecutiveFailures = 0;
      return;
    }
    if (outcome === "timeout") {
      state.ack.timeouts++;
      totals.ackTimeouts++;
    } else {
      state.ack.mismatches++;
      totals.ackMismatches++;
    }
    state.ack.currentConsecutiveFailures++;
    recordPhaseImpact(state);
    state.ack.longestConsecutiveFailures = Math.max(
      state.ack.longestConsecutiveFailures,
      state.ack.currentConsecutiveFailures,
    );
  }

  function emitAck(state, id, timeoutMs = ackTimeoutMs) {
    state.ack.attempts++;
    totals.ackAttempts++;
    const startedAt = Date.now();
    return new Promise((resolve) => {
      let completed = false;
      const finish = (outcome) => {
        if (completed) {
          state.ack.duplicateCallbacks++;
          totals.duplicateAcks++;
          return;
        }
        completed = true;
        clearTimeout(fallbackTimer);
        recordAckOutcome(state, outcome, Date.now() - startedAt);
        resolve(outcome);
      };
      const fallbackTimer = setTimeout(
        () => finish("timeout"),
        timeoutMs + 250,
      );
      state.socket
        .timeout(timeoutMs)
        .emit("control-soak", id, (error, echoed) => {
          if (error) finish("timeout");
          else if (echoed !== id) finish("mismatch");
          else finish("success");
        });
    });
  }

  function connectedMask() {
    let mask = 0n;
    let count = 0;
    for (const state of states) {
      if (state.socket.connected) {
        mask |= 1n << BigInt(state.index);
        count++;
      }
    }
    return { mask, count };
  }

  function emitBroadcast(sender, id, timeoutMs = ackTimeoutMs) {
    const expected = connectedMask();
    const record = {
      id,
      expectedMask: expected.mask,
      expectedCount: expected.count,
      deliveryMask: 0n,
      uniqueDeliveries: 0,
      rawDeliveries: 0,
      ack: "pending",
    };
    broadcastRecords.set(id, record);
    totals.broadcastAttempts++;
    totals.expectedDeliveries += expected.count;
    return new Promise((resolve) => {
      let completed = false;
      const finish = (outcome) => {
        if (completed) {
          totals.broadcastDuplicateCallbacks++;
          return;
        }
        completed = true;
        clearTimeout(fallbackTimer);
        record.ack = outcome;
        if (outcome === "success") totals.broadcastAckSuccess++;
        else totals.broadcastAckTimeouts++;
        resolve(record);
      };
      const fallbackTimer = setTimeout(
        () => finish("timeout"),
        timeoutMs + 250,
      );
      sender.socket
        .timeout(timeoutMs)
        .emit("control-soak-broadcast", id, (error) => {
          finish(error ? "timeout" : "success");
        });
    });
  }

  function transportSummary() {
    const summary = {};
    for (const transportClass of [
      TRANSPORT_POLLING,
      TRANSPORT_WEBSOCKET,
      TRANSPORT_UPGRADE,
    ]) {
      const classStates = states.filter(
        (state) => state.transportClass === transportClass,
      );
      summary[transportClass] = {
        clients: classStates.length,
        connected: classStates.filter((state) => state.socket.connected).length,
        connectEvents: classStates.reduce(
          (total, state) => total + state.connects,
          0,
        ),
        disconnectEvents: classStates.reduce(
          (total, state) => total + state.disconnects,
          0,
        ),
        pollingObserved: classStates.filter((state) =>
          state.observedTransports.has("polling"),
        ).length,
        websocketObserved: classStates.filter((state) =>
          state.observedTransports.has("websocket"),
        ).length,
        upgradedClients: classStates.filter((state) => state.upgradeObserved)
          .length,
        upgradeErrors: classStates.reduce(
          (total, state) => total + state.upgradeErrors,
          0,
        ),
      };
    }
    return summary;
  }

  function nodeSummary() {
    const summary = {};
    for (const state of states) {
      const node = state.currentNode ?? "unassigned";
      summary[node] = (summary[node] ?? 0) + 1;
    }
    return summary;
  }

  function snapshot() {
    return {
      atMs: elapsedMs(),
      connected: states.filter((state) => state.socket.connected).length,
      connects: totals.connects,
      disconnects: totals.disconnects,
      ackAttempts: totals.ackAttempts,
      ackSuccess: totals.ackSuccess,
      ackTimeouts: totals.ackTimeouts,
      ackMismatches: totals.ackMismatches,
      broadcastAttempts: totals.broadcastAttempts,
      broadcastAckSuccess: totals.broadcastAckSuccess,
      broadcastAckTimeouts: totals.broadcastAckTimeouts,
      eventDeliveries: totals.uniqueEventDeliveries,
    };
  }

  async function recoveryAckProbe(state, phase) {
    for (let attempt = 1; attempt <= 3; attempt++) {
      if (!state.socket.connected) {
        state.socket.connect();
        await sleep(100);
        continue;
      }
      const id = `recovery-ack:${phase}:${state.index}:${attempt}:${sequence++}`;
      if (
        (await emitAck(state, id, Math.max(ackTimeoutMs, 2000))) === "success"
      )
        return true;
      await sleep(200);
    }
    return false;
  }

  async function recoveryBroadcastProbe(phase) {
    for (let attempt = 1; attempt <= 3; attempt++) {
      const sender = states.find((state) => state.socket.connected);
      if (!sender) {
        await sleep(200);
        continue;
      }
      const id = `recovery-broadcast:${phase}:${attempt}:${sequence++}`;
      const record = await emitBroadcast(
        sender,
        id,
        Math.max(ackTimeoutMs, 2000),
      );
      const required = Math.max(1, Math.ceil(record.expectedCount * 0.9));
      try {
        await waitUntil(
          () => record.ack === "success" && record.uniqueDeliveries >= required,
          3000,
          `${phase} recovery broadcast attempt ${attempt}`,
        );
        return {
          attempt,
          expected: record.expectedCount,
          delivered: record.uniqueDeliveries,
          required,
        };
      } catch {
        await sleep(200);
      }
    }
    throw new Error(
      `${phase} recovery broadcast did not reach at least 90% of connected clients`,
    );
  }

  async function ensureRecoveredNodeReceivesTraffic(record) {
    const expectedNode =
      record.phase === "rolling-restart" ? record.replacement : record.target;
    if (!expectedNode) return { expectedNode, clientIndexes: [] };
    const assignedSincePhaseStart = () =>
      states
        .filter((state, index) =>
          state.nodeAssignments
            .slice(record.assignmentCounts[index])
            .some((assignment) => assignment.node === expectedNode),
        )
        .map((state) => state.index);
    let assigned = assignedSincePhaseStart();
    for (const state of states) {
      if (assigned.length > 0) break;
      const previousAssignmentCount = state.nodeAssignments.length;
      state.socket.disconnect();
      await sleep(50);
      state.socket.connect();
      await waitUntil(
        () =>
          state.socket.connected &&
          state.nodeAssignments.length > previousAssignmentCount,
        5000,
        `${record.phase} route probe client ${state.index}`,
      );
      assigned = assignedSincePhaseStart();
    }
    return { expectedNode, clientIndexes: assigned };
  }

  async function proveRecovery(phase, record) {
    recoveryInFlight++;
    const startedAt = Date.now();
    try {
      for (const state of states) {
        if (!state.socket.connected) state.socket.connect();
      }
      await waitUntil(
        () => states.every((state) => state.socket.connected),
        recoveryTimeoutMs,
        `${phase} client recovery`,
      );
      const recoveredNodeTraffic =
        await ensureRecoveredNodeReceivesTraffic(record);
      const ackResults = await Promise.all(
        states.map((state) => recoveryAckProbe(state, phase)),
      );
      if (!ackResults.every(Boolean)) {
        const failed = ackResults.flatMap((success, index) =>
          success ? [] : [index],
        );
        throw new Error(
          `${phase} recovery ACK failed for clients ${failed.join(",")}`,
        );
      }
      const broadcast = await recoveryBroadcastProbe(phase);
      const requiredAffectedClients = Math.max(
        1,
        Math.ceil(record.targetClientIndexes.length * 0.8),
      );
      const affectedClients = [...record.affectedClientIndexes].sort(
        (a, b) => a - b,
      );
      if (requirePhases && affectedClients.length < requiredAffectedClients) {
        throw new Error(
          `${phase} affected ${affectedClients.length}/${record.targetClientIndexes.length} ` +
            `target clients, want at least ${requiredAffectedClients}`,
        );
      }
      const expectedRecoveredNode = recoveredNodeTraffic.expectedNode;
      const clientsAssignedToRecoveredNode = recoveredNodeTraffic.clientIndexes;
      if (requirePhases && !expectedRecoveredNode) {
        throw new Error(
          `${phase} did not identify the worker that must receive recovered traffic`,
        );
      }
      if (requirePhases && clientsAssignedToRecoveredNode.length === 0) {
        throw new Error(
          `${phase} recovery sent no client to ${expectedRecoveredNode}`,
        );
      }
      record.afterRecovery = snapshot();
      record.impact = {
        disconnects: delta(record.afterRecovery, record.before, "disconnects"),
        ackTimeouts: delta(record.afterRecovery, record.before, "ackTimeouts"),
        ackMismatches: delta(
          record.afterRecovery,
          record.before,
          "ackMismatches",
        ),
        broadcastTimeouts: delta(
          record.afterRecovery,
          record.before,
          "broadcastAckTimeouts",
        ),
      };
      record.impact.observed =
        record.impact.disconnects > 0 ||
        record.impact.ackTimeouts > 0 ||
        record.impact.broadcastTimeouts > 0;
      if (requirePhases && !record.impact.observed) {
        throw new Error(
          `${phase} produced no observable disconnect or timeout`,
        );
      }
      record.recovery = {
        status: "passed",
        completedAtMs: elapsedMs(),
        latencyMs: Date.now() - startedAt,
        connected: states.filter((state) => state.socket.connected).length,
        ackClients: ackResults.filter(Boolean).length,
        broadcast,
        impact: record.impact,
        targetClients: record.targetClientIndexes,
        affectedClients,
        requiredAffectedClients,
        expectedRecoveredNode,
        clientsAssignedToRecoveredNode,
      };
      await writeJson({ type: "RECOVERY", phase, ...record.recovery });
    } catch (error) {
      record.recovery = {
        status: "failed",
        completedAtMs: elapsedMs(),
        latencyMs: Date.now() - startedAt,
        error: error.message,
        impact: record.impact,
      };
      controlErrors.push(`${phase}: ${error.message}`);
      await writeJson({ type: "RECOVERY", phase, ...record.recovery });
    } finally {
      recoveryInFlight--;
    }
  }

  function normalizePhaseCommand(command) {
    if (
      !command ||
      String(command.type).toLowerCase() !== "phase" ||
      typeof command.phase !== "string"
    ) {
      throw new Error('expected {type:"phase",phase,state}');
    }
    let phase = command.phase.toLowerCase().replaceAll("_", "-");
    let state =
      typeof command.state === "string"
        ? command.state.toLowerCase()
        : command.state;
    for (const suffix of ["-start", "-end"]) {
      if (phase.endsWith(suffix) && state === undefined) {
        state = suffix === "-start" ? "start" : "end";
        phase = phase.slice(0, -suffix.length);
      }
    }
    if (state === "started") state = "start";
    if (state === "ended" || state === "restored") state = "end";
    if (["rolling", "rolling-replacement", "worker-restart"].includes(phase)) {
      phase = "rolling-restart";
    }
    if (!EXPECTED_PHASES.includes(phase) || !["start", "end"].includes(state)) {
      throw new Error(`invalid phase transition ${phase}:${state}`);
    }
    return {
      phase,
      state,
      target: command.target,
      replacement: command.replacement,
    };
  }

  async function handlePhaseCommand(command) {
    const { phase, state, target, replacement } =
      normalizePhaseCommand(command);
    if (!readyAt) throw new Error("phase command received before READY");
    if (state === "start") {
      if (phaseRecords.has(phase)) throw new Error(`${phase} already started`);
      const activePhase = [...phaseRecords.values()].find(
        (record) => record.endedAtMs === null,
      );
      if (activePhase) throw new Error(`${activePhase.phase} is still active`);
      if (requirePhases && !target)
        throw new Error(`${phase} requires a target worker`);
      const record = {
        phase,
        target: target ?? null,
        startedAtMs: elapsedMs(),
        before: snapshot(),
        endedAtMs: null,
        afterFault: null,
        impact: null,
        recovery: null,
        targetClientIndexes: states
          .filter((client) => client.currentNode === target)
          .map((client) => client.index),
        affectedClientIndexes: new Set(),
        replacement: null,
        assignmentCounts: states.map((client) => client.nodeAssignments.length),
      };
      if (requirePhases && record.targetClientIndexes.length === 0) {
        throw new Error(`${phase} target ${target} has no connected clients`);
      }
      phaseRecords.set(phase, record);
      await writeJson({
        type: "PHASE",
        phase,
        state: "started",
        target: record.target,
        atMs: record.startedAtMs,
        snapshot: record.before,
        targetClients: record.targetClientIndexes,
      });
      return;
    }

    const record = phaseRecords.get(phase);
    if (!record || record.endedAtMs !== null)
      throw new Error(`${phase} was not started or already ended`);
    record.endedAtMs = elapsedMs();
    record.replacement = replacement ?? null;
    record.afterFault = snapshot();
    record.impact = {
      disconnects: delta(record.afterFault, record.before, "disconnects"),
      ackTimeouts: delta(record.afterFault, record.before, "ackTimeouts"),
      ackMismatches: delta(record.afterFault, record.before, "ackMismatches"),
      broadcastTimeouts: delta(
        record.afterFault,
        record.before,
        "broadcastAckTimeouts",
      ),
    };
    record.impact.observed =
      record.impact.disconnects > 0 ||
      record.impact.ackTimeouts > 0 ||
      record.impact.broadcastTimeouts > 0;
    await writeJson({
      type: "PHASE",
      phase,
      state: "ended",
      target: record.target,
      atMs: record.endedAtMs,
      impact: record.impact,
      snapshot: record.afterFault,
      targetClients: record.targetClientIndexes,
      affectedClients: [...record.affectedClientIndexes].sort((a, b) => a - b),
      replacement: record.replacement,
    });
    await proveRecovery(phase, record);
  }

  function startControlChannel() {
    const input = readline.createInterface({
      input: process.stdin,
      crlfDelay: Infinity,
    });
    input.on("line", (line) => {
      if (!line.trim()) return;
      phaseQueue = phaseQueue.then(async () => {
        try {
          await handlePhaseCommand(JSON.parse(line));
        } catch (error) {
          controlErrors.push(error.message);
          await writeJson({ type: "CONTROL_ERROR", error: error.message });
        }
      });
    });
    return input;
  }

  async function gracefulShutdown() {
    stopping = true;
    stopWorkload();
    if (control) control.close();
    const engines = states
      .map((state) => state.socket.io.engine)
      .filter(Boolean);
    for (const state of states) {
      state.socket.disconnect();
      state.socket.io.disconnect();
    }
    await waitUntil(
      () => states.every((state) => !state.socket.connected),
      3000,
      "Socket.IO client close",
    ).catch(() => {});
    await waitUntil(
      () => engines.every((engine) => engine.readyState === "closed"),
      3000,
      "Engine.IO client close",
    ).catch(() => {});
    agent.destroy();
    await waitUntil(
      () => {
        const handles = countAgentHandles(agent);
        return (
          handles.active === 0 && handles.free === 0 && handles.queued === 0
        );
      },
      3000,
      "HTTP agent close",
    ).catch(() => {});
    await sleep(100);
    return {
      disconnectedClients: states.filter((state) => !state.socket.connected)
        .length,
      closedEngines: engines.filter((engine) => engine.readyState === "closed")
        .length,
      engineCount: engines.length,
      agentHandles: countAgentHandles(agent),
    };
  }

  let shutdown;
  let result;
  try {
    control = startControlChannel();
    for (let index = 0; index < clientCount; index++) {
      const transportClass = transportClassFor(index);
      const socket = io(baseUrl, socketOptions(transportClass, agent));
      const state = newClientState(index, socket);
      states.push(state);
      socket.io.on("open", () => attachEngineObservers(state));
      socket.on("node", (node) => {
        state.currentNode = String(node);
        state.observedNodes.add(state.currentNode);
        state.nodeAssignments.push({
          node: state.currentNode,
          atMs: elapsedMs(),
        });
      });
      socket.on("connect", () => {
        state.connects++;
        totals.connects++;
        attachEngineObservers(state);
        observeTransport(state, socket.io.engine?.transport);
      });
      socket.on("disconnect", () => {
        state.disconnects++;
        totals.disconnects++;
        recordPhaseImpact(state);
      });
      socket.on("soak-event", (id) => {
        totals.eventDeliveries++;
        state.deliveries++;
        const record = broadcastRecords.get(id);
        if (!record) {
          totals.unknownEvents++;
          return;
        }
        record.rawDeliveries++;
        const bit = 1n << BigInt(index);
        if ((record.deliveryMask & bit) !== 0n) {
          totals.duplicateEvents++;
          return;
        }
        record.deliveryMask |= bit;
        record.uniqueDeliveries++;
        totals.uniqueEventDeliveries++;
      });
      socket.connect();
    }

    await waitUntil(
      () => states.every((state) => state.socket.connected),
      30000,
      "initial client connections",
    );
    await waitUntil(
      () => states.every((state) => state.currentNode !== null),
      30000,
      "initial worker assignments",
    );
    await waitUntil(
      () =>
        states.every((state) => {
          if (state.transportClass === TRANSPORT_POLLING) {
            return (
              state.observedTransports.has("polling") &&
              !state.observedTransports.has("websocket")
            );
          }
          if (state.transportClass === TRANSPORT_WEBSOCKET) {
            return (
              state.observedTransports.has("websocket") &&
              !state.observedTransports.has("polling")
            );
          }
          return (
            state.observedTransports.has("polling") &&
            state.observedTransports.has("websocket") &&
            state.upgradeObserved
          );
        }),
      30000,
      "transport matrix coverage",
    );

    readyAt = Date.now();
    await writeJson({
      type: "READY",
      protocolVersion: 1,
      control: "stdin-jsonl",
      acceptedCommands: EXPECTED_PHASES,
      startedAt: new Date(readyAt).toISOString(),
      durationMs,
      clientCount,
      configuration,
      transportClasses: transportSummary(),
      nodeDistribution: nodeSummary(),
    });

    const sendLoops = states.map(async (state) => {
      await workloadSleep(
        Math.floor((state.index * ackIntervalMs) / states.length),
      );
      while (running) {
        if (state.socket.connected) {
          const id = `ack:${state.index}:${sequence++}`;
          await emitAck(state, id);
        }
        await workloadSleep(ackIntervalMs);
      }
    });

    const broadcastLoop = (async () => {
      while (running) {
        const sender = states.find((state) => state.socket.connected);
        if (sender) await emitBroadcast(sender, `broadcast:${sequence++}`);
        await workloadSleep(broadcastIntervalMs);
      }
    })();

    const churnLoop = (async () => {
      let index = 0;
      while (running) {
        await workloadSleep(churnIntervalMs);
        if (!running) break;
        if (recoveryInFlight > 0) continue;
        const state = states[index++ % states.length];
        if (state.socket.connected) {
          state.socket.disconnect();
          await workloadSleep(100);
          if (running && !stopping) state.socket.connect();
        }
      }
    })();

    await sleep(durationMs);
    stopWorkload();
    await Promise.all([...sendLoops, broadcastLoop, churnLoop]);
    await phaseQueue;

    for (const state of states) {
      if (!state.socket.connected) state.socket.connect();
    }
    await waitUntil(
      () => states.every((state) => state.socket.connected),
      recoveryTimeoutMs,
      "final client recovery",
    );
    const finalRecovery = await Promise.all(
      states.map(async (state) => {
        state.finalRecovered = await recoveryAckProbe(state, "final");
        return state.finalRecovered;
      }),
    );
    assert.ok(
      finalRecovery.every(Boolean),
      "not every client passed the final business ACK probe",
    );
    const finalBroadcast = await recoveryBroadcastProbe("final");

    // Let late callback or delivery attempts surface before evaluating duplicate invariants.
    await sleep(ackTimeoutMs + 100);

    const expectedAttemptsPerClient = Math.floor(durationMs / ackIntervalMs);
    const recoveryAllowanceMs = Math.min(
      2 * recoveryTimeoutMs,
      durationMs * 0.1,
    );
    const allowedUnavailabilityMs = partitionDurationMs + recoveryAllowanceMs;
    const minimumAttemptsPerClient = Math.max(
      2,
      Math.floor(
        (Math.max(0, durationMs - allowedUnavailabilityMs) / ackIntervalMs) *
          0.75,
      ),
    );
    const minimumAckAttempts = clientCount * minimumAttemptsPerClient;
    const expectedBroadcastAttempts = Math.floor(
      durationMs / broadcastIntervalMs,
    );
    const minimumBroadcastAttempts = Math.max(
      2,
      Math.floor(expectedBroadcastAttempts * 0.75),
    );
    const ackSuccessRatio =
      totals.ackAttempts === 0 ? 0 : totals.ackSuccess / totals.ackAttempts;
    const broadcastAckSuccessRatio =
      totals.broadcastAttempts === 0
        ? 0
        : totals.broadcastAckSuccess / totals.broadcastAttempts;
    const broadcastDeliveryRatio =
      totals.expectedDeliveries === 0
        ? 0
        : totals.uniqueEventDeliveries / totals.expectedDeliveries;

    assert.ok(
      totals.ackAttempts >= minimumAckAttempts,
      `only ${totals.ackAttempts} ACK attempts, want at least ${minimumAckAttempts}`,
    );
    assert.ok(
      ackSuccessRatio >= budgets.minimumAckSuccessRatio,
      `ACK success ratio ${ackSuccessRatio} is below ${budgets.minimumAckSuccessRatio}`,
    );
    for (const state of states) {
      const ratio =
        state.ack.attempts === 0 ? 0 : state.ack.success / state.ack.attempts;
      assert.ok(
        state.ack.attempts >= minimumAttemptsPerClient,
        `client ${state.index} made only ${state.ack.attempts} ACK attempts, ` +
          `want at least ${minimumAttemptsPerClient}`,
      );
      assert.ok(
        ratio >= budgets.minimumPerClientAckSuccessRatio,
        `client ${state.index} ACK success ratio ${ratio} is below ${budgets.minimumPerClientAckSuccessRatio}`,
      );
      assert.ok(
        state.ack.longestConsecutiveFailures <=
          budgets.maximumConsecutiveAckFailures,
        `client ${state.index} had ${state.ack.longestConsecutiveFailures} consecutive ACK failures`,
      );
    }
    assert.ok(
      totals.broadcastAttempts >= minimumBroadcastAttempts,
      `only ${totals.broadcastAttempts} broadcasts, want at least ${minimumBroadcastAttempts}`,
    );
    assert.ok(
      broadcastAckSuccessRatio >= budgets.minimumBroadcastAckSuccessRatio,
      `broadcast ACK ratio ${broadcastAckSuccessRatio} is below ${budgets.minimumBroadcastAckSuccessRatio}`,
    );
    assert.ok(
      broadcastDeliveryRatio >= budgets.minimumBroadcastDeliveryRatio,
      `broadcast delivery ratio ${broadcastDeliveryRatio} is below ${budgets.minimumBroadcastDeliveryRatio}`,
    );
    assert.equal(totals.duplicateAcks, 0, "duplicate ACK callbacks observed");
    assert.equal(totals.ackMismatches, 0, "ACK payload mismatches observed");
    assert.equal(
      totals.broadcastDuplicateCallbacks,
      0,
      "duplicate broadcast callbacks observed",
    );
    assert.equal(
      totals.duplicateEvents,
      0,
      "duplicate broadcast deliveries observed",
    );
    assert.equal(
      totals.unknownEvents,
      0,
      "unknown broadcast deliveries observed",
    );
    assert.ok(totals.connects > clientCount, "reconnect coverage missing");
    assert.ok(totals.disconnects > 0, "disconnect coverage missing");
    assert.equal(
      controlErrors.length,
      0,
      `control errors: ${controlErrors.join("; ")}`,
    );

    if (requirePhases) {
      for (const phase of EXPECTED_PHASES) {
        const record = phaseRecords.get(phase);
        assert.ok(record, `${phase} phase was not recorded`);
        assert.ok(
          record.impact?.observed,
          `${phase} did not produce observable client impact`,
        );
        assert.equal(
          record.recovery?.status,
          "passed",
          `${phase} business recovery was not proven`,
        );
      }
    }

    const phaseResults = Object.fromEntries(
      [...phaseRecords].map(([phase, record]) => {
        const { affectedClientIndexes, assignmentCounts, ...serializable } =
          record;
        return [
          phase,
          {
            ...serializable,
            affectedClientIndexes: [...affectedClientIndexes].sort(
              (a, b) => a - b,
            ),
          },
        ];
      }),
    );
    const perClient = states.map((state) => ({
      index: state.index,
      transportClass: state.transportClass,
      connects: state.connects,
      disconnects: state.disconnects,
      observedTransports: [...state.observedTransports].sort(),
      upgradeObserved: state.upgradeObserved,
      upgradeErrors: state.upgradeErrors,
      currentNode: state.currentNode,
      observedNodes: [...state.observedNodes].sort(),
      nodeAssignments: state.nodeAssignments,
      broadcastDeliveries: state.deliveries,
      ack: {
        ...state.ack,
        successRatio: roundRatio(state.ack.success / state.ack.attempts),
      },
      finalRecovered: state.finalRecovered,
    }));

    result = {
      type: "RESULT",
      protocolVersion: 1,
      startedAt: new Date(readyAt).toISOString(),
      completedAt: new Date().toISOString(),
      durationMs,
      clientCount,
      configuration,
      connects: totals.connects,
      disconnects: totals.disconnects,
      transportClasses: transportSummary(),
      nodeDistribution: nodeSummary(),
      ackAttempts: totals.ackAttempts,
      ackSuccess: totals.ackSuccess,
      ackTimeouts: totals.ackTimeouts,
      ackMismatches: totals.ackMismatches,
      duplicateAcks: totals.duplicateAcks,
      longestConsecutiveAckFailures: Math.max(
        ...states.map((state) => state.ack.longestConsecutiveFailures),
      ),
      finalRecoveredClients: states.filter((state) => state.finalRecovered)
        .length,
      broadcasts: {
        attempts: totals.broadcastAttempts,
        ackSuccess: totals.broadcastAckSuccess,
        ackTimeouts: totals.broadcastAckTimeouts,
        duplicateCallbacks: totals.broadcastDuplicateCallbacks,
        expectedDeliveries: totals.expectedDeliveries,
        eventDeliveries: totals.eventDeliveries,
        uniqueDeliveries: totals.uniqueEventDeliveries,
        duplicateDeliveries: totals.duplicateEvents,
        unknownDeliveries: totals.unknownEvents,
        ackSuccessRatio: roundRatio(broadcastAckSuccessRatio),
        deliveryRatio: roundRatio(broadcastDeliveryRatio),
        finalProbe: finalBroadcast,
      },
      // Keep these top-level aliases for the existing Go invariant parser.
      duplicateEvents: totals.duplicateEvents,
      unknownEvents: totals.unknownEvents,
      finalConnected: states.filter((state) => state.socket.connected).length,
      phasesRequired: requirePhases,
      phases: phaseResults,
      budgets: {
        ...budgets,
        expectedAttemptsPerClient,
        minimumAttemptsPerClient,
        allowedUnavailabilityMs,
        expectedBroadcastAttempts,
        minimumAckAttempts,
        minimumBroadcastAttempts,
        actualAckSuccessRatio: roundRatio(ackSuccessRatio),
        actualBroadcastAckSuccessRatio: roundRatio(broadcastAckSuccessRatio),
        actualBroadcastDeliveryRatio: roundRatio(broadcastDeliveryRatio),
      },
      clients: perClient,
    };
  } finally {
    shutdown = await gracefulShutdown();
  }

  result.shutdown = shutdown;
  assert.equal(
    shutdown.disconnectedClients,
    clientCount,
    "not every Socket.IO client closed",
  );
  assert.equal(
    shutdown.closedEngines,
    shutdown.engineCount,
    "not every Engine.IO client closed",
  );
  assert.equal(
    shutdown.agentHandles.active,
    0,
    "live HTTP agent sockets remain after shutdown",
  );
  assert.equal(
    shutdown.agentHandles.free,
    0,
    "live free HTTP agent sockets remain after shutdown",
  );
  assert.equal(
    shutdown.agentHandles.queued,
    0,
    "queued HTTP agent requests remain after shutdown",
  );
  await writeJson(result);
}

main().then(
  () => process.exit(0),
  async (error) => {
    try {
      await writeJson({
        type: "ERROR",
        error: error.message,
        stack: error.stack,
      });
    } catch {
      // The consumer may already have closed stdout; stderr is the last resort.
    }
    console.error(error.stack || error);
    process.exit(1);
  },
);
