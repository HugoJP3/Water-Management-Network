"""
panel_web.py - Panel de monitorización y control de WM_Central (versión web).
"""
import base64
import hmac
import json
import os
import re
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import database as db
import logic

_info = {"broker": None, "started": time.time()}
_ID_RE = re.compile(r"^[\w\-]{1,30}$")


# ------------------------------------------------------------------ estado
def _state() -> dict:
    stations = db.list_stations()
    summary = {}
    for s in stations:
        summary[s["status"]] = summary.get(s["status"], 0) + 1
    return {
        "stations": [{
            "id": s["id"],
            "location": s["location"],
            "status": s["status"],
            "flow": s["flow"],
            "volume": s["volume"],
            "operator": s["operator_id"] if s["status"] == db.WATERING else None,
        } for s in stations],
        "summary": summary,
        "operators": logic.operators_overview(),
        "log": logic.get_log(),
        "broker": _info["broker"],
        "uptime": int(time.time() - _info["started"]),
    }


def _run_command(cmd: dict):
    """Ejecuta una orden del panel. Devuelve (http_status, respuesta)."""
    action, ws = cmd.get("action"), cmd.get("ws")
    if action not in ("start", "block", "activate"):
        return 400, {"ok": False, "error": "Acción desconocida"}
    if not isinstance(ws, str) or not (ws == "ALL" or _ID_RE.match(ws)):
        return 400, {"ok": False, "error": "Estación inválida"}
    if ws != "ALL" and db.get_station(ws) is None:
        return 404, {"ok": False, "error": f"La estación {ws} no existe"}

    if action == "start":
        try:
            seconds = int(cmd.get("seconds", 0))
        except (TypeError, ValueError):
            seconds = 0
        if not 1 <= seconds <= 3600:
            return 400, {"ok": False, "error": "Duración inválida (1-3600 s)"}
        results = logic.manual_start(ws, seconds)
        return 200, {"ok": True, "results": [
            {"ws": w, "ok": ok, "reason": reason} for w, ok, reason in results]}
    if action == "block":
        logic.block_station(ws)
    else:
        logic.activate_station(ws)
    return 200, {"ok": True, "results": []}


# --------------------------------------------------------------- servidor
class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):      # silenciamos el log de cada petición
        pass

    def _authorized(self) -> bool:
        password = os.environ.get("PANEL_PASSWORD")
        if not password:
            return True                     # sin contraseña configurada (pruebas locales)
        header = self.headers.get("Authorization", "")
        if not header.startswith("Basic "):
            return False
        try:
            user, _, pwd = base64.b64decode(header[6:]).decode().partition(":")
        except Exception:
            return False
        good_user = os.environ.get("PANEL_USER", "admin")
        # compare_digest compara en tiempo constante (evita ataques por tiempo)
        return (hmac.compare_digest(user.encode(), good_user.encode())
                and hmac.compare_digest(pwd.encode(), password.encode()))

    def _send(self, status: int, body: bytes, ctype: str):
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def _json(self, status: int, obj):
        self._send(status, json.dumps(obj).encode(), "application/json")

    def _challenge(self):
        self.send_response(401)
        self.send_header("WWW-Authenticate", 'Basic realm="WM_Central"')
        self.send_header("Content-Length", "0")
        self.end_headers()

    def do_GET(self):
        if not self._authorized():
            return self._challenge()
        if self.path in ("/", "/index.html"):
            self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
        elif self.path == "/api/state":
            try:
                self._json(200, _state())
            except Exception:
                logic.log("ERROR en el panel (/api/state):\n" + traceback.format_exc())
                self._json(500, {"error": "Error interno de CENTRAL (ver logs)"})
        else:
            self._json(404, {"error": "No encontrado"})

    def do_POST(self):
        if not self._authorized():
            return self._challenge()
        if self.path != "/api/command":
            return self._json(404, {"error": "No encontrado"})
        if self.headers.get("X-Requested-With") != "wm-panel":
            return self._json(403, {"ok": False, "error": "Origen no permitido"})
        try:
            length = int(self.headers.get("Content-Length", 0))
            if length > 4096:
                raise ValueError
            cmd = json.loads(self.rfile.read(length) or b"{}")
            if not isinstance(cmd, dict):
                raise ValueError
        except (ValueError, json.JSONDecodeError):
            return self._json(400, {"ok": False, "error": "Petición inválida"})
        try:
            status, body = _run_command(cmd)
        except Exception:
            logic.log("ERROR en el panel (/api/command):\n" + traceback.format_exc())
            return self._json(500, {"ok": False, "error": "Error interno de CENTRAL (ver logs)"})
        self._json(status, body)


def start(port: int, broker=None):
    """Arranca el panel en un hilo daemon y devuelve el servidor."""
    _info["broker"] = broker
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    logic.log(f"Panel web en http://IP_PUBLICA:{port}"
              + (" (protegido con contraseña)" if os.environ.get("PANEL_PASSWORD") else
                 " (SIN contraseña: define PANEL_PASSWORD en producción)"))
    return server


# ------------------------------------------------------------------ página
PAGE = r"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>WM_Central · Panel de riego</title>
<style>
  :root { --bg:#0f1b24; --panel:#16262f; --txt:#e8eef2; --muted:#8aa0ad; --line:#26404d; }
  * { box-sizing: border-box; }
  body { margin:0; font-family: system-ui, Segoe UI, Arial, sans-serif; background:var(--bg); color:var(--txt); }
  header { display:flex; flex-wrap:wrap; gap:12px; align-items:center; justify-content:space-between; padding:14px 20px; border-bottom:1px solid var(--line); }
  h1 { margin:0; font-size:20px; font-weight:600; } h1 span { color:var(--muted); font-weight:400; }
  h2 { margin:18px 20px 8px; font-size:13px; text-transform:uppercase; letter-spacing:.08em; color:var(--muted); }
  #chips { display:flex; flex-wrap:wrap; gap:8px; }
  .chip { padding:4px 10px; border-radius:999px; font-size:13px; color:#10202a; font-weight:600; }
  .bar { display:flex; flex-wrap:wrap; gap:10px; align-items:center; padding:12px 20px; }
  button { background:var(--panel); color:var(--txt); border:1px solid var(--line); padding:8px 14px; border-radius:6px; cursor:pointer; font-size:14px; }
  button:hover { border-color:#4a7a90; }
  label { font-size:14px; color:var(--muted); } #sel { font-size:14px; color:var(--muted); }
  .wrap { margin:0 20px; overflow-x:auto; border:1px solid var(--line); border-radius:8px; }
  table { width:100%; border-collapse:collapse; font-size:14px; }
  th { text-align:left; padding:9px 12px; background:var(--panel); color:var(--muted); font-weight:500; }
  td { padding:9px 12px; color:#10202a; font-weight:500; }
  td.num { text-align:right; font-variant-numeric: tabular-nums; }
  tr.row { cursor:pointer; border-top:1px solid var(--line); }
  tr.row.sel { outline:3px solid #ffffff; outline-offset:-3px; }
  .DISPONIBLE { background:#7bd88f; }
  .REGANDO { background:#2ecc71; animation: blink 1s steps(2, start) infinite; }
  .FUGA { background:#ff6b6b; }
  .FUERA_DE_SERVICIO { background:#ffb347; }
  .DESCONECTADA { background:#c4c9cf; }
  @keyframes blink { 50% { background:#b6f5c6; } }
  #log { margin:0 20px; height:230px; overflow:auto; padding:10px 12px; background:#0a1319; border:1px solid var(--line); border-radius:8px; font:12.5px/1.5 Consolas, monospace; white-space:pre-wrap; }
  #ops td { color: var(--txt); }
  #ops tr { border-top:1px solid var(--line); }
  .on { color:#2ecc71; font-weight:600; }
  footer { padding:10px 20px 20px; font-size:12px; color:var(--muted); }
</style>
</head>
<body>
<header>
  <h1>WaterManagement <span>· Panel de CENTRAL</span></h1>
  <div id="chips"></div>
</header>

<div class="bar">
  <button id="bStart">▶ Iniciar riego</button>
  <button id="bBlock">⛔ Bloquear WS</button>
  <button id="bAct">✔ Activar WS</button>
  <label><input type="checkbox" id="all"> Aplicar a todas (ALL)</label>
  <span id="sel">Ninguna estación seleccionada</span>
</div>

<div class="wrap">
<table>
  <thead><tr>
    <th>ID</th><th>Ubicación</th><th>Estado</th>
    <th style="text-align:right">Caudal (L/min)</th>
    <th style="text-align:right">Volumen (L)</th><th>Operario</th>
  </tr></thead>
  <tbody id="rows"></tbody>
</table>
</div>

<h2>Operarios</h2>
<div class="wrap">
<table>
  <thead><tr><th>ID</th><th>Nombre</th><th>Tipo</th><th>Estado</th></tr></thead>
  <tbody id="ops"></tbody>
</table>
</div>

<h2>Mensajes de la aplicación</h2>
<pre id="log"></pre>
<footer id="foot">Conectando…</footer>

<script>
const LABEL = { DISPONIBLE:"Disponible", REGANDO:"Regando", FUGA:"Fuga",
                FUERA_DE_SERVICIO:"Fuera de Servicio", DESCONECTADA:"Desconectada" };
const COLOR = { DISPONIBLE:"#7bd88f", REGANDO:"#2ecc71", FUGA:"#ff6b6b",
                FUERA_DE_SERVICIO:"#ffb347", DESCONECTADA:"#c4c9cf" };
let selected = null;

function cell(text, cls) {
  const td = document.createElement("td");
  td.textContent = text;            // textContent: nunca se interpreta como HTML
  if (cls) td.className = cls;
  return td;
}

function render(d) {
  // Contadores por estado
  const chips = document.getElementById("chips");
  chips.replaceChildren();
  for (const st of Object.keys(LABEL)) {
    const c = document.createElement("span");
    c.className = "chip";
    c.style.background = COLOR[st];
    c.textContent = (d.summary[st] || 0) + " " + LABEL[st];
    chips.appendChild(c);
  }

  // Tabla de estaciones
  const tbody = document.getElementById("rows");
  tbody.replaceChildren();
  for (const s of d.stations) {
    const tr = document.createElement("tr");
    tr.className = "row " + s.status + (s.id === selected ? " sel" : "");
    tr.onclick = () => { selected = s.id; render(d); };
    const live = s.status !== "DESCONECTADA";
    tr.appendChild(cell(s.id));
    tr.appendChild(cell(s.location));
    tr.appendChild(cell(LABEL[s.status] || s.status));
    tr.appendChild(cell(live ? s.flow.toFixed(1) : "—", "num"));
    tr.appendChild(cell(live ? s.volume.toFixed(1) : "—", "num"));
    tr.appendChild(cell(s.operator || "—"));
    tbody.appendChild(tr);
  }
  document.getElementById("sel").textContent =
    selected ? "Seleccionada: " + selected : "Ninguna estación seleccionada";

  // Operarios (los de la BD más los internos del sistema)
  const ops = document.getElementById("ops");
  ops.replaceChildren();
  for (const o of d.operators) {
    const tr = document.createElement("tr");
    const busy = o.watering_at.length > 0;
    tr.appendChild(cell(o.id));
    tr.appendChild(cell(o.name || "—"));
    tr.appendChild(cell(o.system ? "Sistema" : "Operario"));
    tr.appendChild(cell(busy ? "Regando en " + o.watering_at.join(", ") : "Libre",
                        busy ? "on" : ""));
    ops.appendChild(tr);
  }

  // Mensajes: si estabas leyendo el final, seguimos bajando con los nuevos
  const log = document.getElementById("log");
  const atBottom = log.scrollTop + log.clientHeight >= log.scrollHeight - 20;
  log.textContent = d.log.join("\n");
  if (atBottom) log.scrollTop = log.scrollHeight;

  document.getElementById("foot").textContent =
    "Kafka: " + (d.broker || "sin broker") + " · CENTRAL activa desde hace " +
    d.uptime + " s · " + d.stations.length + " estaciones registradas";
}

async function refresh() {
  try {
    const r = await fetch("/api/state");
    if (!r.ok) throw new Error("HTTP " + r.status);
    render(await r.json());
  } catch (e) {
    document.getElementById("foot").textContent = "Sin conexión con CENTRAL (" + e + ")";
  }
}

async function command(action) {
  const ws = document.getElementById("all").checked ? "ALL" : selected;
  if (!ws) { alert("Selecciona una estación o marca «Aplicar a todas»."); return; }
  let seconds = 0;
  if (action === "start") {
    const t = prompt("Duración del riego en segundos:", "30");
    if (t === null) return;
    seconds = parseInt(t, 10);
    if (!(seconds > 0)) { alert("Duración inválida."); return; }
  }
  try {
    const r = await fetch("/api/command", {
      method: "POST",
      headers: { "Content-Type": "application/json", "X-Requested-With": "wm-panel" },
      body: JSON.stringify({ action, ws, seconds })
    });
    const d = await r.json();
    if (!r.ok || !d.ok) { alert(d.error || "Error al ejecutar la orden"); return; }
    const fails = (d.results || []).filter(x => !x.ok);
    if (fails.length) alert(fails.map(x => x.ws + ": " + x.reason).join("\n"));
  } catch (e) { alert("No se pudo contactar con CENTRAL: " + e); }
  refresh();
}

document.getElementById("bStart").onclick = () => command("start");
document.getElementById("bBlock").onclick = () => command("block");
document.getElementById("bAct").onclick = () => command("activate");

refresh();
setInterval(refresh, 500);
</script>
</body>
</html>
"""