"""
logic.py - Lógica de negocio de WM_Central.

Módulo de WM_Central: Lógica de negocio.
    - Validar estación.
    - Reservar estación.
    - Cortar un riego.

    No sabe nada de Kafka.
    Cuando necesita enviar algo llama a send(topic, msg) (que está en kafka_io.py).

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
from datetime import datetime

import database as db
import protocol as proto

# ----------------------------------------------------------------- topics
T_FO_REQUESTS = "fo-requests"               # FO -> Central   REQUEST#FO-12#WS-01#60
T_FO_NOTIFICATIONS = "fo-notifications"     # Central -> FO   NOTIFY#FO-12#WS-01#texto
T_WS_COMMANDS = "ws-commands"               # Central -> WS   START#WS-01#FO-12#60 | STOP#WS-01
T_WS_TELEMETRY = "ws-telemetry"             # WS -> Central   TELEMETRY#WS-01#FO-12#caudal#volumen
T_WS_END_WATERING = "ws-end-watering"       # WS -> Central   END_WATERING#WS-01#litros#segundos#motivo
T_WS_STATUS = "ws-status"                   # Central -> todos STATIONS#WS-01|ubicacion|ESTADO#WS-02|...

# Si durante un riego no llega telemetría en este tiempo, se corta el riego y se libera la estación.
STALL_TIMEOUT = 5


# -------------------------------------------------------------------- log
_log_lock = threading.Lock()      # para que los mensajes de distintos hilos no se mezclen

def log(msg: str):
    """Escribe un mensaje con la hora. Más adelante irá también al panel."""
    with _log_lock:
        print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


# ------------------------------------------------------------------ envío
_sender = None

def set_sender(sender):
    """Se llama desde kafka_io.py para pasar la función de envío de topics."""
    global _sender
    _sender = sender

def has_sender() -> bool:
    return _sender is not None

def send(topic: str, msg: str):
    """Envía un mensaje a un topic de Kafka. Escribe en terminal si no hay sender."""
    if _sender is None:
        log(f"(sin Kafka) {topic} <- {msg}")
    else:
        _sender(topic, msg)

def notify(operator_id: str, ws_id: str, text: str):
    """Envía un NOTIFY al operario y en pantalla de Central."""
    log(f"[{operator_id} / {ws_id}] {text}")
    send(T_FO_NOTIFICATIONS, proto.pack("NOTIFY", operator_id, ws_id, text))

def _deny(operator_id: str, ws_id: str, reason: str):
    """Envía un Riego denegado al operario e informa en pantalla de Central."""
    log(f"[{operator_id} / {ws_id}] Riego denegado: {reason}")
    send(T_FO_NOTIFICATIONS, proto.pack("REPLY", operator_id, ws_id, "DENIED", reason))



# --------------------------------------------------------------- sesiones
# Un riego en curso = una "sesión". Vive en memoria (si Central se reinicia,
# init_db() deja las estaciones DESCONECTADAS y no hay riegos que recuperar).
#   _sessions["WS-01"] = {"operator": "FO-12", "start": t0, "last_tele": t}
_sessions = {}
_lock = threading.Lock()  # sockets, Kafka y housekeeping tocan _sessions: hay que protegerlo

def _open_session(ws_id: str, operator_id: str):
    """Crea la sesión en memoria de una estación que se está regando."""
    now = time.time()
    with _lock:
        _sessions[ws_id] = {"operator": operator_id, "start": now, "last_tele": now}

def abort_session(ws_id: str, reason: str, send_stop: bool = True) -> bool:
    """Corta un riego en curso de una estación (fuga, bloqueo, desconexión,
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
    db.end_watering(ws_id)     # la BD deja de ver la estación como REGANDO

    if send_stop:
        send(T_WS_COMMANDS, proto.pack("STOP", ws_id))
    op = s["operator"]
    notify(op, ws_id, f"Riego interrumpido ({reason})")
    send(T_FO_NOTIFICATIONS, proto.pack("SUMMARY", op, ws_id, litros, segundos, reason))
    log(f"Riego interrumpido de {op} ({reason}): "
        f"{litros} L en {segundos} segundos")

    return True


def check_stalled(now=None):
    """Comprueba si hay riegos en curso que no han mandado telemetría
    durante STALL_TIMEOUT segundos. Si los hay, los aborta.
    - stalled = colgadas

    Función que se llama cada segundo desde wm_central.py
    """
    if now is None:
        now = time.time()
    with _lock:
        stalled = [ws_id for ws_id, s in _sessions.items()
                   if now - s["last_tele"] > STALL_TIMEOUT]
    for ws_id in stalled:
        abort_session(ws_id, "sin telemetría en la estación")


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
