"""
monitor.py  (WM_WS_M) - Monitor de una estación de riego.

Uso:
    python monitor.py <puerto_monitor> <ip_central:puerto_central> <id_ws> [ubicacion]

Ejemplo:
    python monitor.py 6001 localhost:5000 WS-01 Parque_Retiro

Qué hace (todo con sockets, usando protocol.py):
  1. Abre un servidor en <puerto_monitor> para que se conecte su Engine.
  2. Se conecta a CENTRAL y se registra:   REGISTER#WS-01#Parque_Retiro
  3. Espera al Engine y le dice cuál es su id:   ID#WS-01   (el Engine contesta OK)
  4. Cada segundo:
        - Pregunta al Engine:  HEALTH  ->  OK / KO
        - Informa a CENTRAL:   STATUS#WS-01#OK   o   STATUS#WS-01#FUGA
     Si el Engine contesta KO, no contesta a tiempo o se desconecta,
     el Monitor informa de FUGA.

Sin hilos: todo va en un único bucle que da una vuelta por segundo.
Los ficheros protocol.py y monitor.py deben estar en la misma carpeta.
"""
import socket
import sys
import time

import protocol

INTERVALO = 1.0         # segundos entre comprobaciones
TIMEOUT_ENGINE = 2.0    # segundos esperando respuesta del Engine
TIMEOUT_CENTRAL = 3.0   # segundos esperando respuesta de CENTRAL

# Errores de red que pueden saltar al hablar con el otro extremo
ERRORES_RED = (socket.timeout, ConnectionError, protocol.ProtocolError, OSError)


class RegistroRechazado(Exception):
    """CENTRAL ha contestado ACK_REGISTER#KO."""


def log(texto):
    """Mensaje por pantalla con la hora."""
    print(f"[{time.strftime('%H:%M:%S')}] {texto}", flush=True)


# ------------------------------------------------------------------ argumentos
def leer_argumentos():
    if len(sys.argv) < 4:
        print(__doc__)
        sys.exit(1)
    try:
        puerto = int(sys.argv[1])
        host_central, puerto_central = sys.argv[2].rsplit(":", 1)
        puerto_central = int(puerto_central)
    except ValueError:
        print("Argumentos incorrectos. Ejemplo: python monitor.py 6001 localhost:5000 WS-01 Parque_Retiro")
        sys.exit(1)
    ws_id = sys.argv[3]
    ubicacion = sys.argv[4] if len(sys.argv) > 4 else "Desconocida"
    return puerto, host_central, puerto_central, ws_id, ubicacion


# --------------------------------------------------------------------- CENTRAL
def conectar_central(host, puerto, ws_id, ubicacion):
    """Conecta con CENTRAL y se registra. Devuelve el socket abierto.
    Lanza RegistroRechazado si CENTRAL dice KO, o un error de red."""
    sock = socket.create_connection((host, puerto), timeout=TIMEOUT_CENTRAL)
    try:
        if not protocol.client_handshake(sock):
            raise protocol.ProtocolError("CENTRAL rechazó el ENQ")
        protocol.send_message(sock, protocol.pack("REGISTER", ws_id, ubicacion))
        campos = protocol.unpack(protocol.recv_message(sock))
    except Exception:
        sock.close()
        raise

    if campos[0] == "ACK_REGISTER" and len(campos) > 1 and campos[1] == "OK":
        return sock
    motivo = campos[2] if len(campos) > 2 else "sin motivo"
    protocol.close_connection(sock)
    raise RegistroRechazado(motivo)


def cerrar(sock):
    """Cierra un socket sin que falle si ya estaba roto."""
    if sock is not None:
        try:
            protocol.close_connection(sock)
        except OSError:
            pass


# ---------------------------------------------------------------------- ENGINE
def esperar_engine(servidor, ws_id, pendiente=None):
    """Espera (como mucho INTERVALO segundos) a que se conecte el Engine.
    Devuelve el socket del Engine ya identificado, o None si no hay nadie."""
    try:
        conn, addr = servidor.accept()
    except socket.timeout:
        return None

    conn.settimeout(TIMEOUT_ENGINE)
    try:
        if not protocol.server_handshake(conn):
            raise protocol.ProtocolError("El Engine no empezó con ENQ")
        # El Monitor le dice al Engine qué estación es; el Engine confirma con OK
        protocol.send_message(conn, protocol.pack("ID", ws_id))
        respuesta = protocol.recv_message(conn)
        if respuesta == "OK":
            log(f"Engine conectado desde {addr[0]}:{addr[1]}")
            if pendiente is not None:
                protocol.send_message(conn, protocol.pack("PENDING", *pendiente))
                protocol.recv_message(conn)
                log(f"Enviado al Engine el riego interrumpido por su caída "
                    f"({pendiente[0]}: {pendiente[1]} L en {pendiente[2]} s)")
            return conn
        log(f"Engine rechazado: contestó {respuesta!r} al ID")
    except ERRORES_RED as e:
        log(f"Fallo al conectar el Engine: {e}")
    conn.close()
    return None


def comprobar_engine(conn, central_ok=True):
    """Pregunta HEALTH al Engine.
    Devuelve:  "OK"     -> el Engine está bien
               "KO"     -> el Engine informa de avería (sigue conectado)
               "CAIDO"  -> no contesta a tiempo o se ha desconectado"""
    try:
        protocol.send_message(conn, protocol.pack(
            "HEALTH", "CENTRAL_OK" if central_ok else "CENTRAL_KO"))
        respuesta = protocol.recv_message(conn)
    except ERRORES_RED:
        return "CAIDO", None
    campos = protocol.unpack(respuesta)
    estado = "OK" if campos[0] == "OK" else "KO"
    riego = tuple(campos[2:5]) if len(campos) >= 5 and campos[1] == "RIEGO" else None
    return estado, riego


# ------------------------------------------------------------------------ main
def main():
    puerto, host_central, puerto_central, ws_id, ubicacion = leer_argumentos()

    # Servidor para el Engine. timeout = INTERVALO, así accept() también
    # marca el ritmo del bucle cuando no hay Engine conectado.
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    servidor.bind(("0.0.0.0", puerto))
    servidor.listen(1)
    servidor.settimeout(INTERVALO)
    log(f"Monitor de {ws_id} ({ubicacion}) escuchando al Engine en el puerto {puerto}")

    central = None
    engine = None
    estado_anterior = None
    central_avisada = False   # para no repetir el mensaje "sin CENTRAL" cada segundo
    riego_engine = None
    pendiente = None

    try:
        while True:
            inicio = time.time()

            # 1. Asegurar la conexión con CENTRAL (registro incluido)
            if central is None:
                try:
                    central = conectar_central(host_central, puerto_central, ws_id, ubicacion)
                    log(f"Registrado en CENTRAL ({host_central}:{puerto_central})")
                    central_avisada = False
                except RegistroRechazado as e:
                    log(f"CENTRAL rechazó el registro: {e}. Cierro el monitor.")
                    break
                except ERRORES_RED as e:
                    if not central_avisada:
                        log(f"No hay conexión con CENTRAL ({e}). Reintentando cada segundo...")
                        central_avisada = True

            # 2. Asegurar la conexión con el Engine
            if engine is None:
                engine = esperar_engine(servidor, ws_id, pendiente)   # puede tardar hasta 1 s
                if engine is not None:
                    pendiente = None

            # 3. Estado de salud
            if engine is None:
                estado = "FUGA"
                motivo = "Engine no conectado"
            else:
                resultado, riego = comprobar_engine(engine, central is not None)
                if resultado != "CAIDO":
                    riego_engine = riego
                elif riego_engine is not None:
                    pendiente = riego_engine
                    riego_engine = None
                    log(f"El Engine se cayó durante un riego de {pendiente[0]} "
                        f"({pendiente[1]} L en {pendiente[2]} s): se le enviará al recuperarse")
                if resultado == "OK":
                    estado, motivo = "OK", ""
                elif resultado == "KO":
                    estado, motivo = "FUGA", "el Engine informa de avería (KO)"
                else:
                    estado, motivo = "FUGA", "el Engine no responde o se ha desconectado"
                    engine.close()
                    engine = None

            if estado != estado_anterior:
                if estado == "OK":
                    log("Estación OK: funcionamiento normal")
                else:
                    log(f"¡FUGA DETECTADA! {motivo}")
                estado_anterior = estado

            # 4. Informar a CENTRAL
            if central is not None:
                try:
                    protocol.send_message(central, protocol.pack("STATUS", ws_id, estado))
                except ERRORES_RED as e:
                    log(f"Se perdió la conexión con CENTRAL ({e})")
                    cerrar(central)
                    central = None

            # 5. Esperar lo que falte para completar el segundo
            resto = INTERVALO - (time.time() - inicio)
            if resto > 0:
                time.sleep(resto)

    except KeyboardInterrupt:
        log("Monitor detenido por el usuario")
    finally:
        cerrar(central)
        cerrar(engine)
        servidor.close()


if __name__ == "__main__":
    main()