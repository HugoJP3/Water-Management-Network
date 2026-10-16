"""
logic.py - Lógica de negocio de WM_Central.

Aquí está TODO lo que Central "decide": validar peticiones de riego,
llevar las sesiones de riego en curso, cortar riegos por fuga/bloqueo,
resúmenes finales, órdenes manuales...

No sabe nada de Kafka ni de sockets: cuando necesita enviar un mensaje
llama a send(topic, mensaje). Quien arranca Central (wm_central.py)
decide qué hay detrás de send():
  - con broker  -> el productor real de Kafka (kafka_io.py)
  - sin broker  -> solo escribe en el log
Esto permite probar la lógica sin Kafka (ver test_logic.py).

Mensajes (texto con campos separados por '#'):
  fo-requests       FO -> Central   REQUEST#FO-12#WS-01#60
  fo-notifications  Central -> FO   NOTIFY#FO-12#WS-01#texto
                                    REPLY#FO-12#WS-01#ACCEPTED
                                    REPLY#FO-12#WS-01#DENIED#motivo
                                    SUMMARY#FO-12#WS-01#litros#segundos#motivo
  ws-commands       Central -> WS   START#WS-01#FO-12#60 | STOP#WS-01
                                    BLOCK#WS-01 | ACTIVATE#WS-01   (ws puede ser ALL)
  ws-telemetry      WS -> Central   TELEMETRY#WS-01#FO-12#caudal#volumen
                                    END_WATERING#WS-01#litros#segundos#motivo
  ws-status         Central -> todos STATIONS#WS-01|ubicacion|ESTADO#WS-02|...
"""
import threading
import time
from collections import deque
from datetime import datetime

import database as db
import protocol as proto

# ----------------------------------------------------------------- topics
T_FO_REQUESTS = "fo-requests"
T_FO_NOTIFICATIONS = "fo-notifications"
T_WS_COMMANDS = "ws-commands"
T_WS_TELEMETRY = "ws-telemetry"
T_WS_STATUS = "ws-status"
ALL_TOPICS = [T_FO_REQUESTS, T_FO_NOTIFICATIONS, T_WS_COMMANDS,
              T_WS_TELEMETRY, T_WS_STATUS]

# Si durante un riego no llega telemetría en este tiempo (segundos), Central
# da el riego por perdido y libera la estación. La estación manda una
# telemetría cada segundo, así que 5 s deja margen.
STALL_TIMEOUT = 5


# -------------------------------------------------------------------- log
_log_lock = threading.Lock()
# Últimas líneas del log, para mostrarlas en el panel web. maxlen evita que
# crezca sin límite: al llenarse, descarta las más antiguas.
_log_buffer = deque(maxlen=300)


def log(msg: str):
    """Mensajes de Central en pantalla (lo exige el enunciado)."""
    line = f"[{datetime.now():%H:%M:%S}] {msg}"
    with _log_lock:
        print(line, flush=True)
        _log_buffer.append(line)


def get_log() -> list:
    """Copia de las últimas líneas del log (la usa el panel web)."""
    with _log_lock:
        return list(_log_buffer)


# ------------------------------------------------------------------ envío
_sender = None


def set_sender(fn):
    """Registra la función que envía de verdad: fn(topic, mensaje)."""
    global _sender
    _sender = fn


def has_sender() -> bool:
    return _sender is not None


def send(topic: str, message: str):
    if _sender is None:
        log(f"(sin Kafka) {topic} <- {message}")
    else:
        _sender(topic, message)


def notify(operator_id: str, ws_id: str, text: str):
    """Paso informativo para el operario (y también en pantalla de Central)."""
    log(f"[{operator_id} / {ws_id}] {text}")
    send(T_FO_NOTIFICATIONS, proto.pack("NOTIFY", operator_id, ws_id, text))


def _deny(operator_id: str, ws_id: str, motivo: str):
    log(f"Riego DENEGADO a {operator_id} en {ws_id}: {motivo}")
    send(T_FO_NOTIFICATIONS,
         proto.pack("REPLY", operator_id, ws_id, "DENIED", motivo))


# --------------------------------------------------------------- sesiones
# Un riego en curso = una "sesión". Vive en memoria (si Central se reinicia,
# init_db() deja las estaciones DESCONECTADAS y no hay riegos que recuperar).
#   _sessions["WS-01"] = {"operator": "FO-12", "start": t0, "last_tele": t}
_sessions = {}
_lock = threading.Lock()   # sockets, Kafka y housekeeping tocan _sessions a la vez


def _open_session(ws_id: str, operator_id: str):
    now = time.time()
    with _lock:
        _sessions[ws_id] = {"operator": operator_id, "start": now,
                            "last_tele": now}


def abort_session(ws_id: str, motivo: str, send_stop: bool = True) -> bool:
    """Corta el riego en curso de una estación (fuga, bloqueo, desconexión,
    silencio de telemetría...). Avisa al operario con NOTIFY + SUMMARY.

    Devuelve False si no había ningún riego en curso en esa estación.
    """
    with _lock:
        s = _sessions.pop(ws_id, None)
    if s is None:
        return False

    st = db.get_station(ws_id)
    litros = st["volume"] if st else 0
    segundos = int(time.time() - s["start"])
    db.end_watering(ws_id)          # la BD deja de ver la estación como REGANDO

    if send_stop:
        send(T_WS_COMMANDS, proto.pack("STOP", ws_id))
    op = s["operator"]
    notify(op, ws_id, f"Riego interrumpido: {motivo}")
    send(T_FO_NOTIFICATIONS,
         proto.pack("SUMMARY", op, ws_id, litros, segundos, motivo))
    log(f"{ws_id} -> riego de {op} interrumpido ({motivo}): "
        f"{litros} L en {segundos} s")
    return True


def check_stalled(now=None):
    """Libera las estaciones que llevan demasiado sin enviar telemetría
    estando en riego. Se llama cada segundo desde wm_central.py."""
    now = now if now is not None else time.time()
    with _lock:
        colgadas = [ws for ws, s in _sessions.items()
                    if now - s["last_tele"] > STALL_TIMEOUT]
    for ws_id in colgadas:
        abort_session(ws_id, "sin telemetría de la estación")


# ---------------------------------------------------- mensajes de Kafka
def handle_request(fields):
    """fo-requests: REQUEST#operario#estacion#segundos."""
    if len(fields) != 4 or fields[0] != "REQUEST":
        log(f"Petición mal formada ignorada: {fields}")
        return
    _, op, ws_id, secs_txt = fields

    log(f"Petición de riego: {op} -> {ws_id} ({secs_txt} s)")
    notify(op, ws_id, "Petición recibida por CENTRAL")

    try:
        secs = int(secs_txt)
        if secs <= 0:
            raise ValueError
    except ValueError:
        _deny(op, ws_id, "Tiempo límite inválido")
        return

    if not db.operator_exists(op):
        _deny(op, ws_id, "Operario no registrado")
        return

    notify(op, ws_id, "Comprobando el estado de la estación...")
    # start_watering comprueba y reserva la estación en una sola operación
    # atómica (ver database.py): dos operarios a la vez no pueden coincidir.
    if not db.start_watering(ws_id, op):
        _, motivo = db.can_water(ws_id)
        _deny(op, ws_id, motivo or "No se pudo iniciar el riego")
        return

    _open_session(ws_id, op)
    notify(op, ws_id, "Estación disponible. Autorizando el riego")
    send(T_FO_NOTIFICATIONS, proto.pack("REPLY", op, ws_id, "ACCEPTED"))
    send(T_WS_COMMANDS, proto.pack("START", ws_id, op, secs))
    log(f"{ws_id} -> START enviado ({op}, {secs} s)")


def handle_telemetry(fields):
    """ws-telemetry: TELEMETRY#... (cada segundo) y END_WATERING#..."""
    kind = fields[0] if fields else ""

    if kind == "TELEMETRY" and len(fields) == 5:
        _, ws_id, _op, flow, volume = fields
        try:
            flow, volume = float(flow), float(volume)
        except ValueError:
            log(f"Telemetría con números inválidos: {fields}")
            return
        with _lock:
            s = _sessions.get(ws_id)
            if s is not None:
                s["last_tele"] = time.time()
        if s is None:
            return      # telemetría rezagada de un riego que ya cerramos
        db.update_telemetry(ws_id, flow, volume)

    elif kind == "END_WATERING" and len(fields) == 5:
        _, ws_id, litros, segundos, motivo = fields
        with _lock:
            s = _sessions.pop(ws_id, None)
        if s is None:
            log(f"{ws_id} -> END_WATERING ignorado (no había riego en curso)")
            return
        db.end_watering(ws_id)
        op = s["operator"]
        log(f"{ws_id} -> riego terminado ({motivo}): {litros} L en {segundos} s")
        send(T_FO_NOTIFICATIONS,
             proto.pack("SUMMARY", op, ws_id, litros, segundos, motivo))

    else:
        log(f"Mensaje de telemetría no reconocido: {fields}")


# --------------------------------------------- órdenes manuales de CENTRAL
def _targets(ws_id: str):
    """'ALL' = todas las estaciones conocidas."""
    if ws_id == "ALL":
        return [s["id"] for s in db.list_stations()]
    return [ws_id]


def manual_start(ws_id: str, seconds: int = 60):
    """Inicia un riego desde CENTRAL (como si fuera un operario).
    Devuelve [(estacion, ok, motivo)] para que el panel lo muestre."""
    resultados = []
    for w in _targets(ws_id):
        if db.start_watering(w, "CENTRAL"):
            _open_session(w, "CENTRAL")
            send(T_WS_COMMANDS, proto.pack("START", w, "CENTRAL", seconds))
            log(f"{w} -> riego manual iniciado desde CENTRAL ({seconds} s)")
            resultados.append((w, True, ""))
        else:
            _, motivo = db.can_water(w)
            log(f"{w} -> no se puede iniciar riego manual: {motivo}")
            resultados.append((w, False, motivo))
    return resultados


def block_station(ws_id: str):
    """Pone la(s) estación(es) FUERA DE SERVICIO por orden de CENTRAL."""
    for w in _targets(ws_id):
        db.set_blocked(w, True)
        # La estación corta sola al recibir BLOCK, por eso send_stop=False
        abort_session(w, "estación bloqueada por CENTRAL", send_stop=False)
        send(T_WS_COMMANDS, proto.pack("BLOCK", w))
        log(f"{w} -> FUERA DE SERVICIO (orden de CENTRAL)")


def activate_station(ws_id: str):
    """Devuelve la(s) estación(es) a DISPONIBLE."""
    for w in _targets(ws_id):
        db.set_blocked(w, False)
        send(T_WS_COMMANDS, proto.pack("ACTIVATE", w))
        log(f"{w} -> ACTIVADA (orden de CENTRAL)")


def sync_station(ws_id: str):
    """Al (re)conectar una estación: si CENTRAL la tenía bloqueada, se lo
    recordamos, porque la estación no guarda ese estado al reiniciarse."""
    st = db.get_station(ws_id)
    if st and st["blocked"]:
        send(T_WS_COMMANDS, proto.pack("BLOCK", ws_id))
        log(f"{ws_id} -> reconectada: se reenvía BLOCK (sigue fuera de servicio)")


def broadcast_stations():
    """Publica el estado de todas las estaciones para que los operarios
    puedan verlas (punto 10 del enunciado)."""
    if not has_sender():
        return
    partes = [f"{s['id']}|{s['location']}|{s['status']}"
              for s in db.list_stations()]
    send(T_WS_STATUS, proto.pack("STATIONS", *partes))