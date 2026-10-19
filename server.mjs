import { createServer } from "node:http";
import { readFileSync } from "node:fs";
import { networkInterfaces } from "node:os";
import { randomBytes, randomInt } from "node:crypto";
import { Server } from "socket.io";
import QRCode from "qrcode";

const PORT = Number(process.env.PORT || 3000);

const PUBLIC_URL = process.env.PUBLIC_URL
  ? new URL(process.env.PUBLIC_URL).origin
  : null;

const iceServers = JSON.parse(
  process.env.ICE_SERVERS ||
    '[{"urls":"stun:stun.l.google.com:19302"}]'
);

const addresses = Object.values(networkInterfaces())
  .flat()
  .filter((item) => item && item.family === "IPv4" && !item.internal)
  .map((item) => item.address);

const assets = new Map([
  ["/", ["text/html; charset=utf-8", readFileSync(new URL("./index.html", import.meta.url))]],
  ["/app.js", ["text/javascript; charset=utf-8", readFileSync(new URL("./app.js", import.meta.url))]]
]);

const server = createServer((req, res) => {
  res.setHeader("X-Content-Type-Options", "nosniff");
  res.setHeader("X-Frame-Options", "DENY");
  res.setHeader("Referrer-Policy", "no-referrer");
  res.setHeader("Cache-Control", "no-store");
  res.setHeader(
    "Content-Security-Policy",
    "default-src 'self'; script-src 'self'; style-src 'self' 'unsafe-inline'; " +
      "img-src 'self' data:; connect-src 'self' ws: wss:; " +
      "object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
  );

  if (req.method !== "GET") {
    res.writeHead(405).end("Method not allowed");
    return;
  }

  const path = new URL(req.url, "http://localhost").pathname;

  if (path === "/config") {
    res.setHeader("Content-Type", "application/json");
    res.end(JSON.stringify({ iceServers }));
    return;
  }

  const asset = assets.get(path);

  if (!asset) {
    res.writeHead(404).end("Not found");
    return;
  }

  res.setHeader("Content-Type", asset[0]);
  res.end(asset[1]);
});

const io = new Server(server, {
  maxHttpBufferSize: 64 * 1024,
  serveClient: true,
  allowRequest(req, callback) {
    try {
      const origin = req.headers.origin;
      const allowed =
        !origin ||
        new URL(origin).host === req.headers.host ||
        origin === PUBLIC_URL;

      callback(null, allowed);
    } catch {
      callback(null, false);
    }
  }
});

const rooms = new Map();
const limits = new Map();

function limit(socket, action, maximum) {
  const key = `${socket.handshake.address}:${action}`;
  const now = Date.now();
  let bucket = limits.get(key);

  if (!bucket || now >= bucket.until) {
    bucket = { count: 0, until: now + 60_000 };
    limits.set(key, bucket);
  }

  if (++bucket.count > maximum) {
    throw new Error("Too many attempts. Please wait one minute.");
  }
}

function deviceName(value) {
  return String(value || "My device").trim().slice(0, 40) || "My device";
}

function originFor(socket) {
  if (PUBLIC_URL) return PUBLIC_URL;

  const url = new URL(
    socket.handshake.headers.origin ||
      `http://${socket.handshake.headers.host}`
  );

  if (["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)) {
    if (addresses[0]) url.hostname = addresses[0];
  }

  return url.origin;
}

function closeRoom(room, reason, quietSocketId = null) {
  rooms.delete(room.code);

  for (const id of [room.host, room.guest, room.pending]) {
    if (!id) continue;

    const socket = io.sockets.sockets.get(id);

    if (socket?.data.room === room.code) {
      socket.data.room = null;

      if (id !== quietSocketId) {
        socket.emit("ended", { reason });
      }
    }
  }
}

function rejectPending(room, reason) {
  const guest = io.sockets.sockets.get(room.pending);

  if (guest) {
    guest.data.room = null;
    guest.emit("ended", { reason });
  }

  room.pending = null;
  room.pin = null;
  io.to(room.host).emit("request-cancelled");
}

function leave(socket, quiet = false) {
  const room = rooms.get(socket.data.room);
  socket.data.room = null;

  if (!room) return;

  if (room.pending === socket.id) {
    room.pending = null;
    room.pin = null;
    io.to(room.host).emit("request-cancelled");
    return;
  }

  closeRoom(
    room,
    "The other device disconnected. Start a new session to reconnect.",
    quiet ? socket.id : null
  );
}

io.on("connection", (socket) => {
  function action(event, handler) {
    socket.on(event, (payload, acknowledge) => {
      if (typeof acknowledge !== "function") return;

      Promise.resolve()
        .then(() => handler(payload || {}))
        .then((result) => acknowledge({ ok: true, ...result }))
        .catch((error) =>
          acknowledge({ ok: false, error: error.message })
        );
    });
  }

  action("create", async ({ name }) => {
    limit(socket, "create", 15);

    if (socket.data.room) {
      throw new Error("You already have a session. Refresh to start again.");
    }

    if (rooms.size >= 1000) {
      throw new Error("The server is busy. Please try again later.");
    }

    let code;

    do {
      code = String(randomInt(10_000_000, 100_000_000));
    } while (rooms.has(code));

    const token = randomBytes(24).toString("hex");
    const url = `${originFor(socket)}/#${code}.${token}`;

    const room = {
      code,
      token,
      host: socket.id,
      guest: null,
      pending: null,
      expires: Date.now() + 10 * 60_000
    };

    socket.data.name = deviceName(name);
    socket.data.room = code;
    rooms.set(code, room);

    try {
      const qr = await QRCode.toDataURL(url, {
        width: 240,
        margin: 4,
        errorCorrectionLevel: "M"
      });

      if (!socket.connected || rooms.get(code) !== room) {
        throw new Error("Session closed.");
      }

      return { code, url, qr };
    } catch (error) {
      closeRoom(room, "Could not create the pairing code.", socket.id);
      throw error;
    }
  });

  action("join", ({ code, token, name }) => {
    limit(socket, "join", 25);

    code = String(code || "").replace(/\s/g, "");
    const room = rooms.get(code);

    if (!room || room.expires < Date.now()) {
      throw new Error("That code is invalid or has expired.");
    }

    if (room.host === socket.id) {
      throw new Error("Enter the code shown on your other device.");
    }

    if (token && token !== room.token) {
      throw new Error("That pairing link is invalid.");
    }

    if (room.guest || room.pending) {
      throw new Error("That session already has a device joining.");
    }

    const current = rooms.get(socket.data.room);

    if (current?.guest || current?.pending) {
      throw new Error("Finish your current pairing or refresh first.");
    }

    leave(socket, true);

    socket.data.name = deviceName(name);
    socket.data.room = code;

    room.pending = socket.id;
    room.pendingUntil = Date.now() + 60_000;
    room.pin = String(randomInt(100_000, 1_000_000));

    io.to(room.host).emit("pair-request", {
      id: socket.id,
      name: socket.data.name,
      pin: room.pin
    });

    return { pin: room.pin };
  });

  action("approve", ({ id, accept }) => {
    const room = rooms.get(socket.data.room);

    if (
      !room ||
      room.host !== socket.id ||
      room.pending !== id
    ) {
      throw new Error("That pairing request is no longer available.");
    }

    if (room.pendingUntil < Date.now()) {
      rejectPending(room, "The pairing request expired.");
      throw new Error("The pairing request expired.");
    }

    if (accept !== true) {
      rejectPending(room, "The other device declined the connection.");
      return {};
    }

    const guest = io.sockets.sockets.get(room.pending);

    if (!guest) {
      rejectPending(room, "The device disconnected.");
      throw new Error("The device disconnected.");
    }

    room.guest = room.pending;
    room.pending = null;

    guest.emit("paired", {
      initiator: false,
      name: socket.data.name
    });

    socket.emit("paired", {
      initiator: true,
      name: guest.data.name
    });

    return {};
  });

  socket.on("signal", (message) => {
    const room = rooms.get(socket.data.room);

    if (!room?.guest) return;
    if (![room.host, room.guest].includes(socket.id)) return;

    try {
      if (!message || JSON.stringify(message).length > 48_000) return;

      const other = socket.id === room.host ? room.guest : room.host;
      io.to(other).emit("signal", message);
    } catch {
      // Ignore malformed signaling messages.
    }
  });

  socket.on("leave", () => leave(socket));
  socket.on("disconnect", () => leave(socket));
});

setInterval(() => {
  const now = Date.now();

  for (const room of rooms.values()) {
    if (!room.guest && room.expires <= now) {
      closeRoom(room, "Your pairing code expired. Start a new session.");
    } else if (room.pending && room.pendingUntil <= now) {
      rejectPending(room, "The pairing request expired.");
    }
  }

  for (const [key, value] of limits) {
    if (value.until <= now) limits.delete(key);
  }
}, 5000).unref();

server.listen(PORT, "0.0.0.0", () => {
  console.log(`\nBlinkDrop is running.`);
  console.log(`Laptop: http://localhost:${PORT}`);

  for (const address of addresses) {
    console.log(`Phone:  http://${address}:${PORT}`);
  }

  if (PUBLIC_URL) console.log(`Public: ${PUBLIC_URL}`);

  console.log("\nKeep this terminal open while sharing.\n");
});