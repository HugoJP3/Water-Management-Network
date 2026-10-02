"""
WM_Central - Sistema central de WaterManagement (versión 1: solo sockets).

Qué hace esta versión:
  - Arranca un servidor de sockets y espera conexiones de Monitores (WM_WS_M).
  - Atiende REGISTER (alta/autenticación de una estación).
  - Atiende STATUS (latido cada segundo: OK o FUGA).
  - Detecta desconexiones (socket cerrado o sin latidos durante unos segundos).
  - Guarda todo en SQLite a través de database.py.

Qué falta (se añade después): Kafka (peticiones de riego, órdenes a las
estaciones, telemetría) y el panel de monitorización.

Uso:
    python central.py <puerto_escucha> [--broker IP:PUERTO]
"""
import argparse
import socket
import threading
from datetime import datetime

import database as db
import protocol as proto

# Si no llega un STATUS en este tiempo, damos la estación por desconectada.
# El Monitor manda uno cada segundo, así que 3 s deja margen para retrasos.
HEARTBEAT_TIMEOUT = 3

# Conexión activa de cada estación: {"WS-01": <socket>}.
# Sirve para saber qué hilo es el "vigente" si una estación reconecta.
_active = {}
_active_lock = threading.Lock()   # varios hilos tocan _active: hay que protegerlo
_log_lock = threading.Lock()      # para que los mensajes de distintos hilos no se mezclen


def log(msg: str):
    """Escribe un mensaje con la hora. Más adelante irá también al panel."""
    with _log_lock:
        print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


# ------------------------------------------------------------ lógica de STATUS
def process_status(ws_id: str, state: str):
    """Procesa un latido STATUS#ws_id#OK|FUGA de una estación."""
    db.touch_station(ws_id)
    station = db.get_station(ws_id)

    if state == "FUGA":
        # Solo actuamos en la primera vez; el Monitor repite el mensaje cada segundo
        if not station["leak"]:
            riego_cortado = db.set_leak(ws_id, True)
            log(f"{ws_id} -> FUGA detectada")
            if riego_cortado:
                # TODO (Kafka): enviar STOP a la estación y NOTIFY al operario
                log(f"{ws_id} -> riego en curso cortado por la fuga")

    elif state == "OK":
        if station["leak"]:
            db.set_leak(ws_id, False)
            log(f"{ws_id} -> avería resuelta, vuelve a estar DISPONIBLE")

    else:
        log(f"{ws_id} -> estado desconocido en STATUS: {state!r}")


# ------------------------------------------------------- hilo de cada Monitor
def handle_monitor(conn: socket.socket, addr):
    """Atiende a un Monitor durante toda su conexión (corre en su propio hilo)."""
    ws_id = None
    try:
        conn.settimeout(HEARTBEAT_TIMEOUT)

        # 1) Handshake ENQ/ACK
        if not proto.server_handshake(conn):
            log(f"{addr} -> handshake inválido, se cierra")
            return

        # 2) El primer mensaje debe ser REGISTER#id#ubicacion
        fields = proto.unpack(proto.recv_message(conn))
        if len(fields) != 3 or fields[0] != "REGISTER" or not fields[1] or not fields[2]:
            proto.send_message(conn, proto.pack(
                "ACK_REGISTER", "KO", "Mensaje de registro inválido"))
            log(f"{addr} -> registro inválido: {fields}")
            return
        _, ws_id, location = fields

        # 3) Si esa estación ya tenía una conexión abierta (por ejemplo, el
        #    Monitor se reinició y la anterior aún no ha caducado), la nueva
        #    sustituye a la vieja.
        with _active_lock:
            old = _active.get(ws_id)
            _active[ws_id] = conn
        if old is not None:
            try:
                old.close()
            except OSError:
                pass

        # 4) Alta/reconexión en la BD y confirmación al Monitor
        db.register_station(ws_id, location)
        proto.send_message(conn, proto.pack("ACK_REGISTER", "OK"))
        log(f"{ws_id} -> registrada/conectada ({location}) desde {addr[0]}")

        # 5) Bucle de latidos. recv_message bloquea hasta HEARTBEAT_TIMEOUT;
        #    si no llega nada salta la excepción de timeout (OSError).
        while True:
            fields = proto.unpack(proto.recv_message(conn))
            if len(fields) == 3 and fields[0] == "STATUS" and fields[1] == ws_id:
                process_status(ws_id, fields[2])
            else:
                log(f"{ws_id} -> mensaje no esperado: {fields}")

    except (OSError, ValueError) as e:
        # OSError cubre: timeout sin latidos, conexión cerrada, EOT (ConnectionError)
        # ValueError: trama mal formada
        motivo = "sin latidos" if isinstance(e, socket.timeout) else str(e)
        if ws_id is None:
            log(f"{addr} -> conexión terminada antes de registrarse ({motivo})")
    finally:
        # Solo marcamos DESCONECTADA si esta conexión sigue siendo la vigente.
        # Si la estación ya reconectó con otro hilo, no hay que tocar nada.
        if ws_id is not None:
            with _active_lock:
                es_vigente = _active.get(ws_id) is conn
                if es_vigente:
                    del _active[ws_id]
            if es_vigente:
                db.set_disconnected(ws_id)
                log(f"{ws_id} -> DESCONECTADA")
        try:
            conn.close()
        except OSError:
            pass


# ------------------------------------------------------------------- arranque
def seed_operators():
    """Operarios de prueba FO-01..FO-20. Temporal: más adelante se podrá
    dar de alta desde un fichero o desde el propio panel."""
    for i in range(1, 21):
        db.add_operator(f"FO-{i:02d}")


def main():
    parser = argparse.ArgumentParser(description="WM_Central")
    parser.add_argument("port", type=int,
                        help="Puerto de escucha del servidor de sockets (Monitores)")
    parser.add_argument("--broker", default=None,
                        help="IP:PUERTO del broker de Kafka (se usará más adelante)")
    args = parser.parse_args()

    # Al arrancar: crear tablas y dejar todas las estaciones DESCONECTADAS
    db.init_db()
    seed_operators()
    log(f"Estaciones conocidas: {[(s['id'], s['status']) for s in db.list_stations()]}")

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # SO_REUSEADDR permite reiniciar Central sin esperar a que el SO libere el puerto
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", args.port))
    server.listen()
    # Timeout en accept() para que Ctrl+C funcione también en Windows
    server.settimeout(1.0)
    log(f"WM_Central escuchando sockets en el puerto {args.port}")

    try:
        while True:
            try:
                conn, addr = server.accept()
            except socket.timeout:
                continue
            # Un hilo por Monitor: así atendemos varios a la vez
            threading.Thread(target=handle_monitor, args=(conn, addr),
                             daemon=True).start()
    except KeyboardInterrupt:
        log("Apagando WM_Central...")
    finally:
        server.close()


if __name__ == "__main__":
    main()