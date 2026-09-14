// server.js
// Servidor que conecta a un TikTok LIVE con tiktok-live-connector,
// escucha los regalos en tiempo real y los reenvía al navegador
// (index.html) por socket.io para mover a la gatita en la torre.

const path = require("path");
const fs = require("fs");
const express = require("express");
const http = require("http");
const { Server } = require("socket.io");
const { WebcastPushConnection } = require("tiktok-live-connector");

const app = express();
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

app.use(express.static(path.join(__dirname, "public")));
app.use(express.json());

// ---- config.json: se puede editar en caliente sin reiniciar el servidor ----
const CONFIG_PATH = path.join(__dirname, "config.json");

function cargarConfig() {
  try {
    const raw = fs.readFileSync(CONFIG_PATH, "utf-8");
    return JSON.parse(raw);
  } catch (err) {
    console.error("No se pudo leer config.json:", err.message);
    return { meta_metros: 1000, regalos: {}, default_subir_por_moneda: 0.3 };
  }
}

let config = cargarConfig();

// Recarga config.json automáticamente si lo editas mientras el server corre
fs.watchFile(CONFIG_PATH, { interval: 1000 }, () => {
  config = cargarConfig();
  io.emit("config-actualizado", config);
  console.log("config.json recargado");
});

app.get("/api/config", (req, res) => res.json(config));

// Conexión activa al LIVE actual (una por proceso; simple y suficiente para un streamer)
let conexionActiva = null;
let usernameActivo = null;

function calcularEfecto(nombreRegalo, monedasPorUnidad, repeatCount) {
  const entrada = config.regalos[nombreRegalo];
  if (entrada) {
    if (entrada.accion === "subir") {
      return { accion: "subir", metros: entrada.metros * (repeatCount || 1) };
    }
    if (entrada.accion === "bajar") {
      return { accion: "bajar", metros: entrada.metros * (repeatCount || 1) };
    }
    if (entrada.accion === "reiniciar") {
      return { accion: "reiniciar", metros: 0 };
    }
    if (entrada.accion === "encerrar") {
      return { accion: "encerrar", segundos: entrada.segundos || 5, metros: 0 };
    }
  }
  // Regalo no listado en config.json: se calcula según su valor en monedas
  const metros = Math.max(1, Math.round(monedasPorUnidad * config.default_subir_por_moneda)) * (repeatCount || 1);
  return { accion: "subir", metros };
}

app.post("/api/conectar", async (req, res) => {
  const { username } = req.body;
  if (!username) return res.status(400).json({ ok: false, error: "Falta el @usuario" });

  if (conexionActiva) {
    try { await conexionActiva.disconnect(); } catch (_) {}
    conexionActiva = null;
  }

  const conexion = new WebcastPushConnection(username.replace(/^@/, ""));

  try {
    const estado = await conexion.connect();
    conexionActiva = conexion;
    usernameActivo = username;
    console.log(`Conectado al LIVE de @${username}, roomId=${estado.roomId}`);

    conexion.on("gift", (data) => {
      // tiktok-live-connector agrupa "combos" de regalos repetibles;
      // solo procesamos cuando el combo termina (o si el regalo no es repetible)
      const esRepetible = data.giftType === 1;
      if (esRepetible && !data.repeatEnd) return;

      const nombreRegalo = data.giftName;
      const monedas = data.diamondCount || 1;
      const repeticiones = data.repeatCount || 1;
      const efecto = calcularEfecto(nombreRegalo, monedas, repeticiones);

      io.emit("regalo", {
        usuario: data.uniqueId,
        regalo: nombreRegalo,
        monedas,
        repeticiones,
        ...efecto,
      });
    });

    conexion.on("streamEnd", () => {
      io.emit("live-terminado");
      conexionActiva = null;
    });

    conexion.on("disconnected", () => {
      io.emit("live-terminado");
    });

    io.emit("conectado", { username });
    res.json({ ok: true, roomId: estado.roomId });
  } catch (err) {
    console.error("Error al conectar al LIVE:", err.message);
    res.status(500).json({ ok: false, error: "No se pudo conectar. ¿El LIVE está activo ahora mismo?" });
  }
});

app.post("/api/desconectar", async (req, res) => {
  if (conexionActiva) {
    try { await conexionActiva.disconnect(); } catch (_) {}
    conexionActiva = null;
    usernameActivo = null;
  }
  res.json({ ok: true });
});

// ---- Modo prueba: simula un regalo desde el navegador sin necesitar un LIVE activo ----
app.post("/api/simular-regalo", (req, res) => {
  const { regalo, monedas, repeticiones } = req.body;
  const efecto = calcularEfecto(regalo || "Rose", monedas || 1, repeticiones || 1);
  io.emit("regalo", {
    usuario: "tu_prueba",
    regalo: regalo || "Rose",
    monedas: monedas || 1,
    repeticiones: repeticiones || 1,
    ...efecto,
  });
  res.json({ ok: true });
});

io.on("connection", (socket) => {
  socket.emit("config-actualizado", config);
  if (usernameActivo) socket.emit("conectado", { username: usernameActivo });
});

const PUERTO = process.env.PORT || 3000;
server.listen(PUERTO, () => {
  console.log(`Servidor Hello Kitty UP corriendo en http://localhost:${PUERTO}`);
});
