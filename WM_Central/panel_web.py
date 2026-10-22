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
<title>WM_Central · Panel de Riego</title>
<style>
  :root {
    --bg: #0f172a;
    --panel: #1e293b;
    --panel-border: #334155;
    --txt: #f8fafc;
    --muted: #94a3b8;
    --accent: #38bdf8;
  }
  * { box-sizing: border-box; }
  body { margin:0; font-family: system-ui, -apple-system, sans-serif; background:var(--bg); color:var(--txt); }
  
  header { display:flex; flex-wrap:wrap; gap:12px; align-items:center; justify-content:space-between; padding:16px 24px; background:var(--panel); border-bottom:1px solid var(--panel-border); }
  h1 { margin:0; font-size:20px; font-weight:700; display:flex; align-items:center; gap:10px; } 
  h1 span { color:var(--muted); font-weight:400; font-size:15px; }
  
  .live-tag { display:inline-flex; align-items:center; gap:6px; background:#065f46; color:#34d399; font-size:12px; font-weight:600; padding:3px 10px; border-radius:99px; }
  .dot { width:8px; height:8px; background:#34d399; border-radius:50%; animation: pulse 1.5s infinite; }
  @keyframes pulse { 0% { opacity:1; transform:scale(1); } 50% { opacity:0.4; transform:scale(1.2); } 100% { opacity:1; transform:scale(1); } }

  /* Grid de KPIs */
  .kpi-grid { display:grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap:16px; margin:20px; }
  .kpi-card { background:var(--panel); border:1px solid var(--panel-border); border-radius:10px; padding:16px; }
  .kpi-title { font-size:12px; text-transform:uppercase; color:var(--muted); letter-spacing:0.05em; font-weight:600; }
  .kpi-value { font-size:24px; font-weight:700; margin-top:6px; color:var(--accent); }

  /* Filtros y Barra de Control */
  .bar { display:flex; flex-wrap:wrap; gap:12px; align-items:center; margin:0 20px 16px; justify-content:space-between; }
  .bar-left, .bar-right { display:flex; flex-wrap:wrap; gap:8px; align-items:center; }
  
  input[type="text"], select { background:var(--panel); border:1px solid var(--panel-border); color:var(--txt); padding:8px 12px; border-radius:6px; font-size:14px; outline:none; }
  input[type="text"]:focus { border-color:var(--accent); }
  
  button { background:var(--panel); color:var(--txt); border:1px solid var(--panel-border); padding:8px 14px; border-radius:6px; cursor:pointer; font-size:14px; font-weight:500; transition:all 0.2s; }
  button:hover { background:#334155; border-color:var(--accent); }
  button.primary { background:#0284c7; border-color:#0284c7; }
  button.primary:hover { background:#0369a1; }
  
  /* Chips de Estado */
  #chips { display:flex; flex-wrap:wrap; gap:8px; }
  .chip { padding:4px 12px; border-radius:99px; font-size:12px; font-weight:600; color:#0f172a; cursor:pointer; opacity:0.8; transition:opacity 0.2s; }
  .chip:hover, .chip.active { opacity:1; ring:2px solid #fff; }

  /* Vistas: Tabla y Grid */
  .wrap { margin:0 20px; border:1px solid var(--panel-border); border-radius:10px; overflow:hidden; background:var(--panel); }
  table { width:100%; border-collapse:collapse; font-size:14px; }
  th { text-align:left; padding:12px 16px; background:#0f172a; color:var(--muted); font-weight:600; font-size:13px; text-transform:uppercase; }
  td { padding:12px 16px; color:#0f172a; font-weight:600; }
  td.num { text-align:right; font-variant-numeric: tabular-nums; }
  tr.row { cursor:pointer; border-top:1px solid var(--panel-border); }
  tr.row.sel { outline:3px solid #ffffff; outline-offset:-3px; }

  .grid-view { display:grid; grid-template-columns: repeat(auto-fill, minmax(240px, 1fr)); gap:16px; margin:0 20px; }
  .ws-card { background:var(--panel); border:2px solid var(--panel-border); border-radius:10px; padding:16px; cursor:pointer; transition:transform 0.1s; }
  .ws-card:hover { transform: translateY(-2px); }
  .ws-card.sel { border-color:#ffffff !important; }
  .ws-header { display:flex; justify-content:space-between; align-items:center; margin-bottom:12px; }
  .ws-id { font-weight:700; font-size:16px; color:var(--txt); }
  .ws-badge { font-size:11px; padding:3px 8px; border-radius:6px; font-weight:700; color:#0f172a; }
  .ws-body { font-size:13px; color:var(--muted); line-height:1.6; }
  .ws-body strong { color:var(--txt); }

  /* Colores de Estado */
  .DISPONIBLE { background:#4ade80 !important; }
  .REGANDO { background:#22c55e !important; animation: blink 1s steps(2, start) infinite; }
  .FUGA { background:#f87171 !important; }
  .FUERA_DE_SERVICIO { background:#fbbf24 !important; }
  .DESCONECTADA { background:#94a3b8 !important; }
  @keyframes blink { 50% { opacity:0.75; } }

  h2 { margin:24px 20px 12px; font-size:14px; text-transform:uppercase; letter-spacing:.08em; color:var(--muted); }
  #log { margin:0 20px 20px; height:200px; overflow:auto; padding:12px; background:#020617; border:1px solid var(--panel-border); border-radius:10px; font:12.5px/1.5 Consolas, monospace; color:#cbd5e1; white-space:pre-wrap; }
  #ops td { color: var(--txt); }
  #ops tr { border-top:1px solid var(--panel-border); }
  .on { color:#4ade80; font-weight:600; }

  footer { padding:12px 20px 24px; font-size:12px; color:var(--muted); text-align:center; }
</style>
</head>
<body>

<header>
  <h1>WaterManagement <span>· Panel de Control</span></h1>
  <div class="live-tag"><div class="dot"></div> EN VIVO</div>
</header>

<!-- KPIs Globales -->
<div class="kpi-grid">
  <div class="kpi-card">
    <div class="kpi-title">Caudal Global</div>
    <div class="kpi-value" id="kpiFlow">0.0 <span style="font-size:14px">L/min</span></div>
  </div>
  <div class="kpi-card">
    <div class="kpi-title">Agua Suministrada</div>
    <div class="kpi-value" id="kpiVolume">0.0 <span style="font-size:14px">L</span></div>
  </div>
  <div class="kpi-card">
    <div class="kpi-title">Estaciones Activas</div>
    <div class="kpi-value" id="kpiActive">0 / 0</div>
  </div>
</div>

<!-- Filtros y Estado -->
<div class="bar">
  <div class="bar-left">
    <div id="chips"></div>
  </div>
  <div class="bar-right">
    <input type="text" id="search" placeholder="🔍 Buscar estación..." oninput="refreshUI()">
    <button id="btnView" onclick="toggleView()">📱 Vista Tarjetas</button>
  </div>
</div>

<!-- Acciones de Comando -->
<div class="bar" style="background:var(--panel); padding:12px; border-radius:10px; border:1px solid var(--panel-border);">
  <div class="bar-left">
    <button class="primary" id="bStart">▶ Iniciar Riego</button>
    <button id="bBlock" style="border-color:#f87171; color:#f87171;">⛔ Bloquear WS</button>
    <button id="bAct" style="border-color:#4ade80; color:#4ade80;">✔ Activar WS</button>
    <label style="font-size:14px; color:var(--muted); cursor:pointer;"><input type="checkbox" id="all"> Aplicar a todas (ALL)</label>
  </div>
  <div id="sel" style="font-size:14px; font-weight:600; color:var(--accent);">Ninguna estación seleccionada</div>
</div>

<!-- Vista Tabla -->
<div class="wrap" id="tableView">
<table>
  <thead><tr>
    <th>ID</th><th>Ubicación</th><th>Estado</th>
    <th style="text-align:right">Caudal (L/min)</th>
    <th style="text-align:right">Volumen (L)</th><th>Operario</th>
  </tr></thead>
  <tbody id="rows"></tbody>
</table>
</div>

<!-- Vista Grid (Tarjetas) -->
<div class="grid-view" id="gridView" style="display:none;"></div>

<h2>Operarios</h2>
<div class="wrap">
<table>
  <thead><tr><th>ID</th><th>Nombre</th><th>Tipo</th><th>Estado</th></tr></thead>
  <tbody id="ops"></tbody>
</table>
</div>

<h2>Registro de Eventos (Logs)</h2>
<pre id="log"></pre>

<footer id="foot">Conectando con CENTRAL...</footer>

<script>
const LABEL = { DISPONIBLE:"Disponible", REGANDO:"Regando", FUGA:"Fuga",
                FUERA_DE_SERVICIO:"Fuera de Servicio", DESCONECTADA:"Desconectada" };
const COLOR = { DISPONIBLE:"#4ade80", REGANDO:"#22c55e", FUGA:"#f87171",
                FUERA_DE_SERVICIO:"#fbbf24", DESCONECTADA:"#94a3b8" };

let selected = null;
let currentData = null;
let isGridView = false;
let statusFilter = "ALL";

function toggleView() {
  isGridView = !isGridView;
  document.getElementById("tableView").style.display = isGridView ? "none" : "block";
  document.getElementById("gridView").style.display = isGridView ? "grid" : "none";
  document.getElementById("btnView").textContent = isGridView ? "📋 Vista Tabla" : "📱 Vista Tarjetas";
  render(currentData);
}

function formatUptime(secs) {
  const h = Math.floor(secs / 3600);
  const m = Math.floor((secs % 3600) / 60);
  const s = secs % 60;
  return `${h > 0 ? h + 'h ' : ''}${m}m ${s}s`;
}

function cell(text, cls) {
  const td = document.createElement("td");
  td.textContent = text;
  if (cls) td.className = cls;
  return td;
}

function render(d) {
  if (!d) return;
  currentData = d;

  // Calculo de Métricas Globales
  let totalFlow = 0;
  let totalVolume = 0;
  let onlineCount = 0;

  for (const s of d.stations) {
    if (s.status !== "DESCONECTADA") {
      totalFlow += s.flow || 0;
      totalVolume += s.volume || 0;
      onlineCount++;
    }
  }

  document.getElementById("kpiFlow").innerHTML = `${totalFlow.toFixed(1)} <span style="font-size:14px">L/min</span>`;
  document.getElementById("kpiVolume").innerHTML = `${totalVolume.toFixed(1)} <span style="font-size:14px">L</span>`;
  document.getElementById("kpiActive").textContent = `${onlineCount} / ${d.stations.length}`;

  // Filtro por búsqueda de texto
  const searchText = document.getElementById("search").value.toLowerCase();
  const filteredStations = d.stations.filter(s => {
    const matchesText = s.id.toLowerCase().includes(searchText) || s.location.toLowerCase().includes(searchText);
    const matchesFilter = (statusFilter === "ALL") || (s.status === statusFilter);
    return matchesText && matchesFilter;
  });

  // Renderizar Chips de Filtro
  const chips = document.getElementById("chips");
  chips.replaceChildren();

  const allChip = document.createElement("span");
  allChip.className = "chip" + (statusFilter === "ALL" ? " active" : "");
  allChip.style.background = "#e2e8f0";
  allChip.textContent = `Todas (${d.stations.length})`;
  allChip.onclick = () => { statusFilter = "ALL"; render(d); };
  chips.appendChild(allChip);

  for (const st of Object.keys(LABEL)) {
    const count = d.summary[st] || 0;
    if (count > 0 || st === statusFilter) {
      const c = document.createElement("span");
      c.className = "chip" + (statusFilter === st ? " active" : "");
      c.style.background = COLOR[st];
      c.textContent = `${count} ${LABEL[st]}`;
      c.onclick = () => { statusFilter = (statusFilter === st) ? "ALL" : st; render(d); };
      chips.appendChild(c);
    }
  }

  // Renderizar Tabla
  const tbody = document.getElementById("rows");
  tbody.replaceChildren();
  for (const s of filteredStations) {
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

  // Renderizar Tarjetas (Grid View)
  const grid = document.getElementById("gridView");
  grid.replaceChildren();
  for (const s of filteredStations) {
    const card = document.createElement("div");
    card.className = "ws-card" + (s.id === selected ? " sel" : "");
    card.onclick = () => { selected = s.id; render(d); };
    
    card.innerHTML = `
      <div class="ws-header">
        <span class="ws-id">${s.id}</span>
        <span class="ws-badge ${s.status}">${LABEL[s.status] || s.status}</span>
      </div>
      <div class="ws-body">
        📍 <strong>${s.location}</strong><br>
        💧 Caudal: <strong>${s.status !== 'DESCONECTADA' ? s.flow.toFixed(1) + ' L/min' : '—'}</strong><br>
        📊 Volumen: <strong>${s.status !== 'DESCONECTADA' ? s.volume.toFixed(1) + ' L' : '—'}</strong><br>
        👤 Operario: <strong>${s.operator || '—'}</strong>
      </div>
    `;
    grid.appendChild(card);
  }

  document.getElementById("sel").textContent = selected ? "Seleccionada: " + selected : "Ninguna estación seleccionada";

  // Operarios
  const ops = document.getElementById("ops");
  ops.replaceChildren();
  for (const o of d.operators) {
    const tr = document.createElement("tr");
    const busy = o.watering_at && o.watering_at.length > 0;
    tr.appendChild(cell(o.id));
    tr.appendChild(cell(o.name || "—"));
    tr.appendChild(cell(o.system ? "Sistema" : "Operario"));
    tr.appendChild(cell(busy ? "Regando en " + o.watering_at.join(", ") : "Libre", busy ? "on" : ""));
    ops.appendChild(tr);
  }

  // Logs
  const log = document.getElementById("log");
  const atBottom = log.scrollTop + log.clientHeight >= log.scrollHeight - 20;
  log.textContent = d.log.join("\n");
  if (atBottom) log.scrollTop = log.scrollHeight;

  document.getElementById("foot").textContent =
    `Broker: ${d.broker || "sin broker"} · Uptime: ${formatUptime(d.uptime)} · ${d.stations.length} Estaciones registradas`;
}

function refreshUI() {
  if (currentData) render(currentData);
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