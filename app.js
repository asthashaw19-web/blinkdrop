"use strict";

const $ = (id) => document.getElementById(id);

const BLOCK = 4 * 1024 * 1024;
const CHUNK = 16 * 1024;
const MEMORY_LIMIT = 256 * 1024 * 1024;

const socket = io({
  autoConnect: false,
  reconnection: false
});

let config;
let pc;
let channel;
let connectTimer;
let toastTimer;
let peerLabel = "";
let pairingURL = "";
let pendingPair = null;
let ended = false;
let pumping = false;
let activeJob = null;
let incoming = null;
let saveFolder = null;
let retainedBytes = 0;

let signalChain = Promise.resolve();
let receiveChain = Promise.resolve();

const candidates = [];
const queue = [];
const requests = new Map();
const records = new Set();

const deepLink = location.hash.slice(1);

if (deepLink) {
  history.replaceState(null, "", location.pathname + location.search);
}

$("deviceName").value =
  /Android|iPhone|iPad|Mobile/i.test(navigator.userAgent)
    ? "My phone"
    : "My laptop";

function uid() {
  return Array.from(crypto.getRandomValues(new Uint8Array(16)))
    .map((byte) => byte.toString(16).padStart(2, "0"))
    .join("");
}

function formatBytes(bytes) {
  if (!bytes) return "0 B";

  const units = ["B", "KiB", "MiB", "GiB", "TiB"];
  const index = Math.min(
    Math.floor(Math.log(bytes) / Math.log(1024)),
    units.length - 1
  );

  return `${(bytes / 1024 ** index).toFixed(index ? 1 : 0)} ${units[index]}`;
}

function toast(message) {
  clearTimeout(toastTimer);
  $("toast").textContent = message;
  $("toast").hidden = false;

  toastTimer = setTimeout(() => {
    $("toast").hidden = true;
  }, 6500);
}

function run(fn) {
  return (...args) => {
    try {
      Promise.resolve(fn(...args)).catch((error) => toast(error.message));
    } catch (error) {
      toast(error.message);
    }
  };
}

function ready() {
  return !ended && channel?.readyState === "open";
}

function status(message, online = false) {
  $("connection").textContent = message;
  $("connection").classList.toggle("live", online);
  $("badge").classList.toggle("online", online);
  $("badgeText").textContent = online
    ? "Devices connected"
    : ended
      ? "Session ended"
      : "Waiting to connect";

  $("sendText").disabled = !online;
}

async function copy(text) {
  try {
    if (!navigator.clipboard) throw new Error("Clipboard unavailable");
    await navigator.clipboard.writeText(text);
  } catch {
    const area = document.createElement("textarea");
    area.value = text;
    area.style.position = "fixed";
    area.style.opacity = "0";
    document.body.append(area);
    area.select();

    const copied = document.execCommand("copy");
    area.remove();

    if (!copied) {
      throw new Error("Copy is unavailable here. Select and copy the text manually.");
    }
  }

  toast("Copied.");
}

function callServer(event, payload) {
  return new Promise((resolve, reject) => {
    if (!socket.connected) {
      reject(new Error("The pairing server is unavailable."));
      return;
    }

    socket.timeout(12_000).emit(event, payload, (error, result) => {
      if (error) {
        reject(new Error("The server did not respond. Please try again."));
      } else if (!result?.ok) {
        reject(new Error(result?.error || "The request failed."));
      } else {
        resolve(result);
      }
    });
  });
}

function record(name, detail) {
  const element = document.createElement("article");
  element.className = "transfer";

  const title = document.createElement("div");
  title.className = "transfer-name";
  title.textContent = name;

  const description = document.createElement("div");
  description.className = "transfer-detail";
  description.textContent = detail;

  const progress = document.createElement("progress");
  progress.max = 1;
  progress.value = 0;
  progress.setAttribute("aria-label", `Transfer progress for ${name}`);

  const actions = document.createElement("div");
  actions.className = "actions";

  element.append(title, description, progress, actions);
  $("transfers").prepend(element);
  $("empty").hidden = true;

  const item = {
    element,
    description,
    progress,
    actions,
    done: false,
    url: null,
    retained: 0,
    started: performance.now()
  };

  records.add(item);
  return item;
}

function update(item, bytes, total, verb) {
  const seconds = Math.max((performance.now() - item.started) / 1000, 0.01);
  const speed = bytes / seconds;
  const percent = total ? Math.floor((bytes / total) * 100) : 100;

  item.progress.max = total || 1;
  item.progress.value = total ? bytes : 1;

  item.description.textContent =
    `${verb} · ${percent}% · ${formatBytes(bytes)} / ` +
    `${formatBytes(total)} · ${formatBytes(speed)}/s`;
}

function finish(item, message, success = true) {
  item.done = true;
  item.description.textContent = message;

  if (success) {
    item.progress.value = item.progress.max;
  } else {
    item.progress.hidden = true;
  }
}

function textRecord(text, detail) {
  const item = record("Text message", detail);
  item.progress.hidden = true;

  const preview = document.createElement("pre");
  preview.textContent = text;
  item.element.insertBefore(preview, item.actions);

  const button = document.createElement("button");
  button.className = "text-button";
  button.type = "button";
  button.textContent = "Copy text";
  button.onclick = run(() => copy(text));
  item.actions.append(button);

  return item;
}

function control(message) {
  if (!ready()) throw new Error("The devices are no longer connected.");
  channel.send(JSON.stringify(message));
}

function request(message) {
  return new Promise((resolve, reject) => {
    const key = uid();

    const timer = setTimeout(() => {
      requests.delete(key);
      reject(new Error("The other device stopped responding."));
    }, 60_000);

    requests.set(key, { resolve, reject, timer });

    try {
      control({ ...message, request: key });
    } catch (error) {
      clearTimeout(timer);
      requests.delete(key);
      reject(error);
    }
  });
}

function reply(message, result = {}) {
  control({ reply: message.request, ok: true, ...result });
}

function cleanFileName(name) {
  return String(name)
    .replace(/[\\/:*?"<>|\u0000-\u001f]/g, "_")
    .replace(/[. ]+$/g, "")
    .slice(0, 170) || "file";
}

async function cleanIncoming(item) {
  if (!item.folder) return;

  try {
    const writer = await item.opening;
    await writer?.abort();
  } catch {
    // The stream may already be closed or aborted.
  }

  try {
    await item.folder.removeEntry(item.savedName);
  } catch {
    // No partial file may have been created.
  }
}

function stop(message) {
  if (ended) return;
  ended = true;

  clearTimeout(connectTimer);

  if ($("pairDialog").open) $("pairDialog").close();

  status(message);
  $("setup").hidden = true;
  $("pairedView").hidden = true;
  $("dropzone").disabled = true;
  $("joinButton").disabled = true;

  for (const pending of requests.values()) {
    clearTimeout(pending.timer);
    pending.reject(new Error(message));
  }

  requests.clear();

  if (activeJob && !activeJob.item.done) {
    finish(activeJob.item, "Interrupted. Pair again and resend this file.", false);
  }

  for (const job of queue.splice(0)) {
    finish(job.item, "Not sent. Start a new session to try again.", false);
  }

  if (incoming) {
    const item = incoming;
    incoming = null;

    if (!item.folder) retainedBytes -= item.size;
    item.parts.length = 0;
    item.block.length = 0;

    finish(item.row, "Transfer interrupted. Please resend the file.", false);
    void cleanIncoming(item);
  }

  try { channel?.close(); } catch {}
  try { pc?.close(); } catch {}

  if (socket.connected) socket.emit("leave");
}

function waitForBuffer(dc) {
  return new Promise((resolve, reject) => {
    let timer;

    function cleanup() {
      clearTimeout(timer);
      dc.removeEventListener("bufferedamountlow", check);
      dc.removeEventListener("close", closed);
      dc.removeEventListener("error", closed);
    }

    function closed() {
      cleanup();
      reject(new Error("The connection closed during the transfer."));
    }

    function check() {
      if (dc.readyState !== "open") {
        closed();
      } else if (dc.bufferedAmount <= dc.bufferedAmountLowThreshold) {
        cleanup();
        resolve();
      }
    }

    dc.addEventListener("bufferedamountlow", check);
    dc.addEventListener("close", closed);
    dc.addEventListener("error", closed);

    timer = setTimeout(() => {
      cleanup();
      reject(new Error("The connection is too slow or has stopped."));
    }, 60_000);

    check();
  });
}

async function sendBlock(buffer) {
  const dc = channel;
  const negotiated = pc.sctp?.maxMessageSize;
  const chunkSize = negotiated > 0
    ? Math.min(CHUNK, negotiated)
    : CHUNK;

  for (let offset = 0; offset < buffer.byteLength; offset += chunkSize) {
    if (!ready() || dc !== channel) {
      throw new Error("The connection closed.");
    }

    if (dc.bufferedAmount > 1024 * 1024) {
      await waitForBuffer(dc);
    }

    dc.send(buffer.slice(offset, offset + chunkSize));
  }
}

async function pump() {
  if (pumping || !ready()) return;
  pumping = true;

  try {
    while (queue.length && ready()) {
      const job = queue.shift();
      activeJob = job;

      try {
        const { file, item, id } = job;

        item.description.textContent = "Waiting for the receiving device…";

        const accepted = await request({
          type: "file",
          id,
          name: file.name,
          size: file.size
        });

        if (!accepted.ok) {
          finish(item, accepted.error || "The receiver declined this file.", false);
          continue;
        }

        item.started = performance.now();

        for (let offset = 0; offset < file.size; offset += BLOCK) {
          const end = Math.min(offset + BLOCK, file.size);
          const buffer = await file.slice(offset, end).arrayBuffer();

          await sendBlock(buffer);

          const acknowledged = await request({
            type: "block",
            id,
            offset: end
          });

          if (!acknowledged.ok) {
            throw new Error(acknowledged.error || "The transfer failed.");
          }

          update(item, end, file.size, "Sending");
        }

        const completed = await request({ type: "end", id });

        if (!completed.ok) {
          throw new Error(completed.error || "The transfer could not finish.");
        }

        finish(item, `Delivered · ${formatBytes(file.size)}`);
      } catch (error) {
        finish(job.item, error.message, false);
        stop(error.message);
        break;
      } finally {
        activeJob = null;
      }
    }
  } finally {
    pumping = false;
  }
}

function enqueue(files) {
  if (ended) {
    toast("Start a new session before adding files.");
    return;
  }

  const available = Math.max(0, 100 - queue.length - (activeJob ? 1 : 0));
  const selected = Array.from(files);

  if (selected.length > available) {
    toast("The queue can hold 100 files. Add the remaining files later.");
  }

  for (const file of selected.slice(0, available)) {
    const item = record(
      file.name,
      `${formatBytes(file.size)} · ${ready() ? "Queued" : "Waiting for pairing"}`
    );

    queue.push({ file, item, id: uid() });
  }

  void pump();
}

async function receive(data) {
  if (ended) return;

  if (data instanceof ArrayBuffer) {
    const item = incoming;

    if (
      !item ||
      item.blockBytes + data.byteLength > BLOCK ||
      item.received + item.blockBytes + data.byteLength > item.size
    ) {
      throw new Error("Received an invalid file chunk.");
    }

    item.block.push(data);
    item.blockBytes += data.byteLength;
    return;
  }

  if (typeof data !== "string" || data.length > 32_000) {
    throw new Error("Received an invalid message.");
  }

  const message = JSON.parse(data);

  if (message.reply) {
    const waiting = requests.get(message.reply);

    if (waiting) {
      clearTimeout(waiting.timer);
      requests.delete(message.reply);
      waiting.resolve(message);
    }

    return;
  }

  if (typeof message.request !== "string") {
    throw new Error("Received an invalid transfer request.");
  }

  if (message.type === "text") {
    if (typeof message.text !== "string" || message.text.length > 4000) {
      reply(message, { ok: false, error: "Text exceeds 4,000 characters." });
      return;
    }

    const item = textRecord(message.text, `Received from ${peerLabel}`);
    finish(item, `Received from ${peerLabel}`);
    reply(message);
    return;
  }

  if (message.type === "file") {
    if (
      incoming ||
      typeof message.id !== "string" ||
      !/^[a-f0-9]{32}$/.test(message.id) ||
      typeof message.name !== "string" ||
      message.name.length > 255 ||
      !Number.isSafeInteger(message.size) ||
      message.size < 0
    ) {
      reply(message, { ok: false, error: "Invalid file details." });
      return;
    }

    if (!saveFolder && retainedBytes + message.size > MEMORY_LIMIT) {
      const error =
        "Receiver storage is full or this file exceeds the browser limit. " +
        "Choose a save folder on the receiving laptop, or download and clear " +
        "finished items, then resend.";

      reply(message, { ok: false, error });
      toast(error);
      return;
    }

    const row = record(message.name, "Preparing to receive…");
    const folder = saveFolder;

    const item = {
      id: message.id,
      name: message.name,
      size: message.size,
      received: 0,
      blockBytes: 0,
      block: [],
      parts: [],
      folder,
      writer: null,
      opening: null,
      row,
      savedName: `BlinkDrop-${uid()}-${cleanFileName(message.name)}`
    };

    incoming = item;

    if (folder) {
      item.opening = (async () => {
        const handle = await folder.getFileHandle(item.savedName, {
          create: true
        });

        return handle.createWritable();
      })();

      item.writer = await item.opening;
    } else {
      retainedBytes += item.size;
    }

    if (ended || incoming !== item) return;

    row.started = performance.now();
    row.description.textContent = `Receiving · ${formatBytes(item.size)}`;
    reply(message);
    return;
  }

  if (message.type === "block") {
    const item = incoming;

    if (
      !item ||
      message.id !== item.id ||
      !item.blockBytes ||
      message.offset !== item.received + item.blockBytes
    ) {
      throw new Error("File chunks arrived out of sequence.");
    }

    if (item.writer) {
      await item.writer.write(new Blob(item.block));
    } else {
      item.parts.push(...item.block);
    }

    if (ended || incoming !== item) return;

    item.received += item.blockBytes;
    item.block = [];
    item.blockBytes = 0;

    update(item.row, item.received, item.size, "Receiving");
    reply(message);
    return;
  }

  if (message.type === "end") {
    const item = incoming;

    if (
      !item ||
      message.id !== item.id ||
      item.received !== item.size ||
      item.blockBytes !== 0
    ) {
      throw new Error("The received file is incomplete.");
    }

    if (item.writer) {
      await item.writer.close();

      if (ended || incoming !== item) return;

      finish(
        item.row,
        `Saved in ${item.folder.name} as ${item.savedName}`
      );
    } else {
      const blob = new Blob(item.parts, {
        type: "application/octet-stream"
      });

      item.row.url = URL.createObjectURL(blob);
      item.row.retained = item.size;

      const download = document.createElement("a");
      download.href = item.row.url;
      download.download = cleanFileName(item.name);
      download.textContent = "Download file";

      item.row.actions.append(download);
      finish(item.row, `Received · ${formatBytes(item.size)} · Ready to download`);
    }

    item.parts = [];
    incoming = null;
    reply(message);
    return;
  }

  throw new Error("Unsupported transfer request.");
}

function attachChannel(dc) {
  channel = dc;
  dc.binaryType = "arraybuffer";
  dc.bufferedAmountLowThreshold = 256 * 1024;

  dc.onopen = () => {
    if (ended) return;

    clearTimeout(connectTimer);
    $("setup").hidden = true;
    $("pairedView").hidden = false;
    $("peerName").textContent = peerLabel;

    status(`Connected to ${peerLabel}. You can share in both directions.`, true);
    void pump();
  };

  dc.onmessage = (event) => {
    receiveChain = receiveChain
      .then(() => receive(event.data))
      .catch((error) => stop(error.message));
  };

  dc.onclose = () => stop("The connection closed. Start a new session to reconnect.");
  dc.onerror = () => stop("The transfer connection failed. Please pair again.");
}

async function beginPeer({ initiator, name }) {
  if (ended) return;

  peerLabel = name;
  $("setup").hidden = true;
  status(`Connecting to ${peerLabel}…`);

  pc = new RTCPeerConnection({ iceServers: config.iceServers });

  pc.onicecandidate = ({ candidate }) => {
    if (candidate && socket.connected) {
      socket.emit("signal", { candidate: candidate.toJSON() });
    }
  };

  pc.ondatachannel = ({ channel: dc }) => attachChannel(dc);

  pc.onconnectionstatechange = () => {
    if (pc.connectionState === "failed") {
      stop("Connection blocked. Try the same Wi-Fi network and pair again.");
    }
  };

  connectTimer = setTimeout(() => {
    if (!ready()) {
      stop("Could not connect. Check that both devices can reach each other.");
    }
  }, 30_000);

  if (initiator) {
    attachChannel(pc.createDataChannel("blinkdrop", { ordered: true }));
    await pc.setLocalDescription(await pc.createOffer());

    if (!ended) {
      socket.emit("signal", { description: pc.localDescription });
    }
  }
}

async function handleSignal(message) {
  if (!pc || ended) return;

  if (message.description) {
    await pc.setRemoteDescription(message.description);

    while (candidates.length) {
      await pc.addIceCandidate(candidates.shift());
    }

    if (message.description.type === "offer") {
      await pc.setLocalDescription(await pc.createAnswer());

      if (!ended) {
        socket.emit("signal", { description: pc.localDescription });
      }
    }
  } else if (message.candidate) {
    if (pc.remoteDescription) {
      await pc.addIceCandidate(message.candidate);
    } else {
      candidates.push(message.candidate);
    }
  }
}

async function join(code, token) {
  const result = await callServer("join", {
    code,
    token,
    name: $("deviceName").value
  });

  $("hostInfo").hidden = true;
  $("joinButton").disabled = true;
  $("joinCode").disabled = true;

  status(
    `Check number ${result.pin} on your other device, then approve the connection there.`
  );
}

socket.on("connect", run(async () => {
  if (deepLink) {
    const match = deepLink.match(/^(\d{8})\.([a-f0-9]{48})$/);

    if (match) {
      await join(match[1], match[2]);
      return;
    }
  }

  const result = await callServer("create", {
    name: $("deviceName").value
  });

  pairingURL = result.url;
  $("roomCode").textContent = result.code.replace(/(.{4})/, "$1 ");
  $("qr").src = result.qr;
  $("hostInfo").hidden = false;

  status("Open BlinkDrop on your other device and scan or enter this code.");
}));

socket.on("connect_error", () => {
  stop("Cannot reach the server. Check that it is running, then refresh.");
});

socket.on("disconnect", () => {
  stop("Lost contact with the pairing server. Start a new session to reconnect.");
});

socket.on("ended", ({ reason }) => stop(reason));

socket.on("pair-request", (request) => {
  pendingPair = request;
  $("requestName").textContent = request.name;
  $("requestPin").textContent = request.pin;

  if (!$("pairDialog").open) $("pairDialog").showModal();
});

socket.on("request-cancelled", () => {
  pendingPair = null;
  if ($("pairDialog").open) $("pairDialog").close();
});

socket.on("paired", (message) => {
  if ($("pairDialog").open) $("pairDialog").close();
  pendingPair = null;
  beginPeer(message).catch((error) => stop(error.message));
});

socket.on("signal", (message) => {
  signalChain = signalChain
    .then(() => handleSignal(message))
    .catch((error) => stop(error.message));
});

async function approve(accept) {
  if (!pendingPair) return;

  const id = pendingPair.id;
  await callServer("approve", { id, accept });

  pendingPair = null;
  if ($("pairDialog").open) $("pairDialog").close();
}

$("acceptPair").onclick = run(() => approve(true));
$("rejectPair").onclick = run(() => approve(false));

$("pairDialog").addEventListener("cancel", (event) => {
  event.preventDefault();
  run(() => approve(false))();
});

$("joinForm").onsubmit = run(async (event) => {
  event.preventDefault();
  const code = $("joinCode").value.replace(/\s/g, "");

  if (!/^\d{8}$/.test(code)) {
    throw new Error("Enter the eight-digit code from your other device.");
  }

  await join(code);
});

$("copyLink").onclick = run(() => copy(pairingURL));

$("restart").onclick = () => {
  location.replace(location.pathname + location.search);
};

$("dropzone").onclick = () => $("fileInput").click();

$("fileInput").onchange = (event) => {
  enqueue(event.target.files);
  event.target.value = "";
};

for (const event of ["dragenter", "dragover"]) {
  $("dropzone").addEventListener(event, (e) => {
    e.preventDefault();
    $("dropzone").classList.add("drag");
  });
}

for (const event of ["dragleave", "drop"]) {
  $("dropzone").addEventListener(event, (e) => {
    e.preventDefault();
    $("dropzone").classList.remove("drag");
  });
}

$("dropzone").addEventListener("drop", (event) => {
  enqueue(event.dataTransfer.files);
});

window.addEventListener("dragover", (event) => event.preventDefault());
window.addEventListener("drop", (event) => event.preventDefault());

function selectTab(text) {
  $("filesPanel").hidden = text;
  $("textPanel").hidden = !text;

  $("filesTab").classList.toggle("active", !text);
  $("textTab").classList.toggle("active", text);

  $("filesTab").setAttribute("aria-pressed", String(!text));
  $("textTab").setAttribute("aria-pressed", String(text));
}

$("filesTab").onclick = () => selectTab(false);
$("textTab").onclick = () => selectTab(true);

$("sendText").onclick = run(async () => {
  const text = $("textInput").value;

  if (!text.trim()) throw new Error("Add some text first.");
  if (!ready()) throw new Error("Connect your other device first.");

  const item = textRecord(text, "Sending…");
  $("sendText").disabled = true;

  try {
    const result = await request({ type: "text", text });

    if (!result.ok) {
      throw new Error(result.error || "Could not send the text.");
    }

    finish(item, "Delivered");

    if ($("textInput").value === text) {
      $("textInput").value = "";
    }
  } catch (error) {
    finish(item, error.message, false);
    throw error;
  } finally {
    $("sendText").disabled = !ready();
  }
});

if (window.isSecureContext && "showDirectoryPicker" in window) {
  $("chooseFolder").hidden = false;
}

$("chooseFolder").onclick = run(async () => {
  try {
    saveFolder = await window.showDirectoryPicker({ mode: "readwrite" });

    $("receiveMode").textContent =
      `New incoming files will save to “${saveFolder.name}”. ` +
      "Each file receives a unique filename.";

    $("chooseFolder").textContent = "Change folder";
    toast("Save folder selected.");
  } catch (error) {
    if (error.name !== "AbortError") throw error;
  }
});

$("clearFinished").onclick = () => {
  for (const item of records) {
    if (!item.done) continue;

    if (item.url) {
      URL.revokeObjectURL(item.url);
      retainedBytes -= item.retained;
    }

    item.element.remove();
    records.delete(item);
  }

  $("empty").hidden = records.size !== 0;
};

window.addEventListener("beforeunload", (event) => {
  if (activeJob || incoming || queue.length || retainedBytes > 0) {
    event.preventDefault();
    event.returnValue = "";
  }
});

(async () => {
  try {
    if (!window.RTCPeerConnection) {
      throw new Error("This browser does not support device-to-device sharing.");
    }

    const response = await fetch("/config");

    if (!response.ok) throw new Error("Could not load the server configuration.");

    config = await response.json();
    socket.connect();
  } catch (error) {
    stop(error.message);
  }
})();