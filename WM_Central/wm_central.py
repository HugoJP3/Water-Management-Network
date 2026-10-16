"""
WM_Central - Sistema central de WaterManagement.

Piezas:
  - Servidor de sockets: atiende a los Monitores (REGISTER y STATUS).
  - Hilo consumidor de Kafka (kafka_io.py): peticiones de riego y telemetría.
  - Hilo de mantenimiento: libera riegos sin telemetría y publica la lista
    de estaciones para los operarios.
  - Panel web (panel_web.py): monitorización y órdenes de CENTRAL.
  - La lógica de negocio está en logic.py; la BD en database.py.

Uso:
    python wm_central.py <puerto_sockets> [IP:PUERTO_broker_kafka] [--web-port N]
Ejemplos:
    python wm_central.py 5000 localhost:9092
    python wm_central.py 5000              (solo sockets, sin Kafka)

Variables de entorno útiles (para Docker/Railway):
    WM_DB_PATH      ruta del fichero SQLite
    PORT            puerto HTTP del panel web (Railway lo define solo)
    PANEL_PASSWORD  contraseña del panel (y PANEL_USER, por defecto "admin")
"""
import argparse
import os
import socket
import threading
import traceback

import database as db
import logic
import panel_web
import protocol as proto

log = logic.log

# Si no llega un STATUS en este tiempo, damos la estación por desconectada.
HEARTBEAT_TIMEOUT = 3

# Conexión vigente de cada estación: {"WS-01": <socket>}
_active = {}
_active_lock = threading.Lock()


# ------------------------------------------------------------ lógica de STATUS
def process_status(ws_id: str, state: str):
    """Procesa un latido STATUS#ws_id#OK|FUGA de una estación."""
    db.touch_station(ws_id)
    station = db.get_station(ws_id)

    if state == "FUGA":
        # Solo actuamos la primera vez; el Monitor repite el mensaje cada segundo
        if not station["leak"]:
            db.set_leak(ws_id, True)
            log(f"{ws_id} -> FUGA detectada")
            # Si había un riego en curso: STOP a la estación + aviso al operario
            logic.abort_session(ws_id, "fuga detectada en la estación")

    elif state == "OK":
        if station["leak"]:
            db.set_leak(ws_id, False)
            log(f"{ws_id} -> avería resuelta, vuelve a estar DISPONIBLE")

    else:
        log(f"{ws_id} -> estado desconocido en STATUS: {state!r}")


# ------------------------------------------------------- hilo de cada Monitor
def handle_monitor(conn: socket.socket, addr):
    """Atiende a un Monitor durante toda su conexión (un hilo por Monitor)."""
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

        # 3) Si ya había una conexión de esa estación, la nueva la sustituye
        with _active_lock:
            old = _active.get(ws_id)
            _active[ws_id] = conn
        if old is not None:
            try:
                old.close()
            except OSError:
                pass

        # 4) Alta/reconexión en la BD y confirmación al Monitor
        logic.abort_session(ws_id, "estación reconectada")   # por si quedaba un riego colgado
        db.register_station(ws_id, location)
        proto.send_message(conn, proto.pack("ACK_REGISTER", "OK"))
        log(f"{ws_id} -> registrada/conectada ({location}) desde {addr[0]}")
        logic.sync_station(ws_id)        # si estaba bloqueada, se lo recordamos

        # 5) Bucle de latidos
        while True:
            fields = proto.unpack(proto.recv_message(conn))
            if len(fields) == 3 and fields[0] == "STATUS" and fields[1] == ws_id:
                process_status(ws_id, fields[2])
            else:
                log(f"{ws_id} -> mensaje no esperado: {fields}")

    except (OSError, ValueError) as e:
        if ws_id is None:
            log(f"{addr} -> conexión terminada antes de registrarse ({e})")
    finally:
        # Solo marcamos DESCONECTADA si esta conexión sigue siendo la vigente
        if ws_id is not None:
            with _active_lock:
                es_vigente = _active.get(ws_id) is conn
                if es_vigente:
                    del _active[ws_id]
            if es_vigente:
                db.set_disconnected(ws_id)
                log(f"{ws_id} -> DESCONECTADA")
                logic.abort_session(ws_id, "estación desconectada")
        try:
            conn.close()
        except OSError:
            pass


# --------------------------------------------------------------- mantenimiento
def housekeeping(stop_event: threading.Event):
    """Cada segundo: libera riegos sin telemetría. Cada 2 s: publica la lista
    de estaciones en Kafka para los operarios."""
    n = 0
    while not stop_event.wait(1.0):
        n += 1
        try:
            logic.check_stalled()
            if n % 2 == 0:
                logic.broadcast_stations()
        except Exception:
            log("ERROR en el hilo de mantenimiento:\n" + traceback.format_exc())


# ------------------------------------------------------------------- arranque
def load_operators(path: str = "operators.txt"):
    """Da de alta los operarios desde un fichero de texto (una línea por
    operario: 'FO-01' o 'FO-01,Nombre'; las líneas con # se ignoran).
    Si el fichero no existe, crea FO-01..FO-20 para poder probar."""
    if not os.path.exists(path):
        log(f"AVISO: no existe {path}; se crean FO-01..FO-20 de prueba")
        for i in range(1, 21):
            db.add_operator(f"FO-{i:02d}")
        return
    count = 0
    with open(path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            op_id, _, name = line.partition(",")
            db.add_operator(op_id.strip(), name.strip() or None)
            count += 1
    log(f"{count} operarios cargados desde {path}")


def main():
    parser = argparse.ArgumentParser(description="WM_Central")
    parser.add_argument("port", type=int,
                        help="Puerto de escucha del servidor de sockets (Monitores)")
    parser.add_argument("broker", nargs="?", default=None,
                        help="IP:PUERTO del broker de Kafka (si se omite, sin Kafka)")
    parser.add_argument("--web-port", type=int,
                        default=int(os.environ.get("PORT", 8080)),
                        help="Puerto HTTP del panel web (por defecto $PORT o 8080)")
    parser.add_argument("--no-panel", action="store_true",
                        help="No arrancar el panel web")
    args = parser.parse_args()

    # Al arrancar: crear tablas y dejar todas las estaciones DESCONECTADAS
    db.init_db()
    load_operators()
    log(f"Estaciones conocidas: {[(s['id'], s['status']) for s in db.list_stations()]}")

    stop_event = threading.Event()

    # Kafka (import tardío: así Central también arranca sin confluent-kafka
    # instalado cuando se ejecuta en modo solo sockets)
    kafka_io = None
    if args.broker:
        import kafka_io
        kafka_io.connect(args.broker)
        threading.Thread(target=kafka_io.consume_forever,
                         args=(args.broker, stop_event), daemon=True).start()
    else:
        log("AVISO: sin broker Kafka. Solo funcionan los sockets.")

    threading.Thread(target=housekeeping, args=(stop_event,), daemon=True).start()

    if not args.no_panel:
        panel_web.start(args.web_port, args.broker)

    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(("0.0.0.0", args.port))
    server.listen()
    server.settimeout(1.0)   # para que Ctrl+C funcione también en Windows
    log(f"WM_Central escuchando sockets en el puerto {args.port}")

    try:
        while True:
            try:
                conn, addr = server.accept()
            except socket.timeout:
                continue
            threading.Thread(target=handle_monitor, args=(conn, addr),
                             daemon=True).start()
    except KeyboardInterrupt:
        log("Apagando WM_Central...")
    finally:
        stop_event.set()
        server.close()
        if kafka_io is not None:
            kafka_io.close()


if __name__ == "__main__":
    main()