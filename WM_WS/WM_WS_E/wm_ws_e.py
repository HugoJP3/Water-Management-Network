"""
wm_ws_e.py  (WM_WS_E) - Engine de una estación de riego.

Uso:
    python wm_ws_e.py <ip_kafka:puerto> <ip_monitor:puerto>

Ejemplo:
    python wm_ws_e.py localhost:9092 localhost:6001

El Engine NO recibe su id por argumento: se lo dice el Monitor al conectarse
(mensaje  ID#WS-01 ), y el Engine contesta OK.

Qué hace, todo a la vez (cada cosa en su hilo):
  - Hilo "monitor":   habla por SOCKETS con el Monitor. Contesta OK o KO
                      a cada HEALTH. KO = avería simulada.
  - Hilo "comandos":  escucha en KAFKA el tema ws-commands (órdenes de CENTRAL).
  - Hilo "riego":     si está regando, cada segundo publica la telemetría
                      en ws-telemetry y comprueba si se acabó el tiempo.
  - Hilo principal:   menú por teclado (riego manual, finalizar, avería...).

Estado de la estación (se calcula a partir de tres cosas):
    averia     -> FUGA
    bloqueada  -> FUERA DE SERVICIO
    riego      -> REGANDO
    nada       -> DISPONIBLE

Los ficheros protocol.py y wm_ws_e.py deben estar en la misma carpeta.
"""
import random
import socket
import sys
import threading
import time

import protocol

TOPIC_COMANDOS = "ws-commands"
TOPIC_TELEMETRIA = "ws-telemetry"
TOPIC_PETICIONES = "fo-requests"
TOPIC_NOTIFICACIONES = "fo-notifications"

CAUDAL_MIN = 4.5   # L/min, el caudal simulado varía entre estos dos valores
CAUDAL_MAX = 6.0
DURACION_MANUAL = 60   # segundos por defecto del riego manual

ERRORES_RED = (ConnectionError, protocol.ProtocolError, OSError)


def log(texto):
    """Mensaje por pantalla con la hora."""
    print(f"\n[{time.strftime('%H:%M:%S')}] {texto}", flush=True)


# ============================================================ KAFKA (aislado)
class KafkaBus:
    """Todo lo de Kafka vive aquí. El resto del programa solo usa
    publish() y escuchar(), así que se puede probar sin Kafka."""

    def __init__(self, bootstrap):
        self.bootstrap = bootstrap
        self.producer = None
        self.consumer = None

    def conectar(self, ws_id):
        from kafka import KafkaProducer, KafkaConsumer
        try:
            from kafka.errors import NoBrokersAvailable as ErrorSinBroker
        except ImportError:
            from kafka.errors import KafkaTimeoutError as ErrorSinBroker    
        while True:
            try:
                self.producer = KafkaProducer(bootstrap_servers=self.bootstrap)
                # group_id ÚNICO por estación y por ejecución: así cada Engine
                # recibe TODAS las órdenes (si compartieran grupo, se las repartirían)
                # y al reiniciar no recibe órdenes antiguas.
                self.consumer = KafkaConsumer(
                    TOPIC_COMANDOS, TOPIC_NOTIFICACIONES,
                    bootstrap_servers=self.bootstrap,
                    group_id=f"engine-{ws_id}-{int(time.time())}",
                    auto_offset_reset="latest",
                )
                self._esperar_asignacion()
                log(f"Conectado a Kafka ({self.bootstrap})")
                return
            except ErrorSinBroker:
                log(f"No hay Kafka en {self.bootstrap}. Reintentando en 3 s...")
                time.sleep(3)

    def _esperar_asignacion(self):
        """Un consumidor nuevo tarda unos segundos en 'apuntarse' a las particiones.
        Si CENTRAL enviara una orden antes, se perdería. Por eso esperamos aquí
        a tenerlas asignadas y fijamos la posición (último mensaje)."""
        limite = time.time() + 20
        while not self.consumer.assignment() and time.time() < limite:
            self.consumer.poll(timeout_ms=500)
        for particion in self.consumer.assignment():
            self.consumer.position(particion)

    def publish(self, topic, mensaje):
        try:
            self.producer.send(topic, mensaje.encode("utf-8"))
            self.producer.flush(timeout=3)
            return True, ""
        except Exception as e:
            log(f"No se pudo publicar en {topic}: {e}")
            return False, str(e)

    def escuchar(self, callback):
        """Bloquea para siempre, llamando a callback(texto) por cada mensaje."""
        for registro in self.consumer:
            try:
                callback(registro.value.decode("utf-8"))
            except Exception as e:
                log(f"Error procesando mensaje de Kafka: {e}")


# ================================================================== ENGINE
class Engine:
    def __init__(self, monitor_host, monitor_port, bus):
        self.monitor_addr = (monitor_host, monitor_port)
        self.bus = bus
        self.ws_id = None                  # lo dice el Monitor
        self.id_listo = threading.Event()  # se activa cuando ya sabemos el id

        # Estado compartido entre hilos. SIEMPRE se toca con self.lock.
        self.lock = threading.RLock()
        self.averia = False       # avería simulada (tecla)
        self.bloqueada = False    # puesta fuera de servicio por CENTRAL
        self.riego = None         # None, o dict con los datos del riego en curso

    # ------------------------------------------------------------- estado
    def estado(self):
        with self.lock:
            if self.averia:
                return "FUGA"
            if self.bloqueada:
                return "FUERA DE SERVICIO"
            if self.riego is not None:
                return "REGANDO"
            return "DISPONIBLE"

    def salud(self):
        """Lo que contestamos a HEALTH."""
        with self.lock:
            return "KO" if self.averia else "OK"

    # -------------------------------------------------------------- riego
    def iniciar_riego(self, operario, segundos):
        """Devuelve (True, "") o (False, motivo del rechazo)."""
        with self.lock:
            if self.averia:
                return False, "la estación tiene una avería"
            if self.bloqueada:
                return False, "la estación está fuera de servicio"
            if self.riego is not None:
                return False, "ya está regando"
            ahora = time.time()
            self.riego = {"operario": operario, "limite": segundos,
                          "inicio": ahora, "ultimo": ahora, "litros": 0.0}
        log(f"RIEGO INICIADO por {operario} durante {segundos} s (electroválvula ABIERTA)")
        return True, ""

    def terminar_riego(self, motivo):
        """Cierra el riego en curso y avisa a CENTRAL. Devuelve False si no había riego.
        IMPORTANTE: no llamar con el lock cogido (publicar en Kafka puede tardar)."""
        with self.lock:
            if self.riego is None:
                return False
            r = self.riego
            self.riego = None
            duracion = time.time() - r["inicio"]
            mensaje = protocol.pack("END_WATERING", self.ws_id,
                                    f"{r['litros']:.1f}", f"{duracion:.0f}", motivo)
        self.bus.publish(TOPIC_TELEMETRIA, mensaje)
        log(f"RIEGO TERMINADO ({motivo}): {r['litros']:.1f} L en {duracion:.0f} s "
            f"(electroválvula CERRADA)")
        return True

    def alternar_averia(self):
        with self.lock:
            self.averia = not self.averia
            hay_averia = self.averia
        if hay_averia:
            log("AVERÍA simulada: a partir de ahora contesto KO al Monitor")
            self.terminar_riego("FUGA")     # un riego en curso se corta al instante
        else:
            log("Avería reparada: vuelvo a contestar OK al Monitor")

    # -------------------------------------------- órdenes de CENTRAL (Kafka)
    def procesar_comando(self, texto):
        """START#WS-01#FO-12#60 | STOP#WS-01 | BLOCK#WS-01 | ACTIVATE#WS-01
        (con ALL en lugar del id, va dirigido a todas las estaciones)."""
        campos = protocol.unpack(texto)
        
        # Respuesta de CENTRAL a peticiones de riego manual: NOTIFY#MANUAL#WS-01#OK
        if (campos[0] in ("NOTIFY", "REPLY", "SUMMARY") and len(campos) > 3
            and campos[1] == "MANUAL" and campos[2] == self.ws_id):
            log("CENTRAL " + " ".join(campos[3:]))
            return

        #----------------------------------------- órdenes de CENTRAL (Kafka)
        if len(campos) < 2 or campos[1] not in (self.ws_id, "ALL"):
            return   # no es para mí
        orden = campos[0]
        log(f"Orden de CENTRAL: {texto}")

        if orden == "START" and len(campos) >= 4:
            try:
                segundos = int(campos[3])
            except ValueError:
                log("START con duración no válida, ignorado")
                return
            ok, motivo = self.iniciar_riego(campos[2], segundos)
            if not ok:
                log(f"No puedo iniciar el riego: {motivo}")
        elif orden == "STOP":
            self.terminar_riego("STOP")
        elif orden == "BLOCK":
            self.terminar_riego("BLOQUEO")
            with self.lock:
                self.bloqueada = True
            log("Estación FUERA DE SERVICIO por orden de CENTRAL")
        elif orden == "ACTIVATE":
            with self.lock:
                self.bloqueada = False
            log("Estación ACTIVADA por orden de CENTRAL")
        else:
            log(f"Orden desconocida: {texto}")

    # ------------------------------------------------------------- HILOS
    def hilo_monitor(self):
        """Sockets con el Monitor. Si se cae, reintenta para siempre."""
        avisado = False
        while True:
            sock = None
            try:
                sock = socket.create_connection(self.monitor_addr, timeout=5)
                sock.settimeout(None)
                if not protocol.client_handshake(sock):
                    raise protocol.ProtocolError("El Monitor rechazó el ENQ")

                # El Monitor nos dice quiénes somos: ID#WS-01
                campos = protocol.unpack(protocol.recv_message(sock))
                if campos[0] != "ID" or len(campos) < 2:
                    protocol.send_message(sock, "KO")
                    raise protocol.ProtocolError(f"Se esperaba ID y llegó {campos}")
                if self.ws_id is not None and self.ws_id != campos[1]:
                    protocol.send_message(sock, "KO")
                    raise protocol.ProtocolError(f"Id distinto al anterior ({campos[1]})")
                self.ws_id = campos[1]
                protocol.send_message(sock, "OK")
                self.id_listo.set()
                log(f"Conectado al Monitor. Soy la estación {self.ws_id}")
                avisado = False

                # Bucle de salud: contestar a cada HEALTH
                while True:
                    mensaje = protocol.recv_message(sock)
                    if mensaje == "HEALTH":
                        protocol.send_message(sock, self.salud())
                    else:
                        log(f"Mensaje inesperado del Monitor: {mensaje}")
            except ERRORES_RED as e:
                if not avisado:
                    log(f"Sin conexión con el Monitor ({e}). Reintentando...")
                    avisado = True
                if sock is not None:
                    sock.close()
                time.sleep(2)

    def hilo_comandos(self):
        self.bus.escuchar(self.procesar_comando)

    def hilo_riego(self):
        """Cada segundo: si hay riego, publica telemetría y vigila el tiempo."""
        while True:
            time.sleep(1)
            mensaje = None
            caducado = False
            with self.lock:
                r = self.riego
                if r is None:
                    continue
                ahora = time.time()
                caudal = random.uniform(CAUDAL_MIN, CAUDAL_MAX)           # L/min
                r["litros"] += caudal * (ahora - r["ultimo"]) / 60.0      # L
                r["ultimo"] = ahora
                mensaje = protocol.pack("TELEMETRY", self.ws_id, r["operario"],
                                        f"{caudal:.1f}", f"{r['litros']:.1f}")
                caducado = (ahora - r["inicio"]) >= r["limite"]
                resumen = f"Regando ({r['operario']}): {caudal:.1f} L/min, {r['litros']:.1f} L"
            self.bus.publish(TOPIC_TELEMETRIA, mensaje)
            log(resumen)
            if caducado:
                self.terminar_riego("TIEMPO")

    # --------------------------------------------------------------- MENÚ
    def menu(self):
        ayuda = ("\n  [1] Iniciar riego manual      [2] Finalizar riego\n"
                 "  [3] Simular / reparar avería  [4] Ver estado\n"
                 "  [h] Ayuda                     [0] Salir")
        print(ayuda)
        while True:
            try:
                opcion = input("> ").strip().lower()
            except EOFError:
                log("Sin teclado (stdin cerrado): sigo sin menú. Ctrl+C para salir.")
                while True:
                    time.sleep(3600)
            except KeyboardInterrupt:
                return

            if opcion == "1":
                dato = input(f"Duración en segundos [{DURACION_MANUAL}]: ").strip()
                try:
                    segundos = int(dato) if dato else DURACION_MANUAL
                except ValueError:
                    print("Número no válido")
                    continue
                ok, motivo = self.bus.publish(TOPIC_PETICIONES, protocol.pack("REQUEST", "MANUAL", self.ws_id, segundos))
                log(f"Solicitud de riego manual enviada a CENTRAL ({segundos} s)")
                if not ok:
                    print(f"No se puede regar: {motivo} (¿Kafka caído?)")

            elif opcion == "2":
                if not self.terminar_riego("MANUAL"):
                    print("No hay ningún riego en curso")
            elif opcion == "3":
                self.alternar_averia()
            elif opcion == "4":
                print(f"Estación {self.ws_id}: {self.estado()}")
            elif opcion == "h":
                print(ayuda)
            elif opcion == "0":
                return
            elif opcion:
                print("Opción no válida (h = ayuda)")

    # ------------------------------------------------------------- ARRANQUE
    def arrancar(self):
        threading.Thread(target=self.hilo_monitor, daemon=True).start()
        log(f"Esperando al Monitor en {self.monitor_addr[0]}:{self.monitor_addr[1]}...")
        self.id_listo.wait()    # hasta que el Monitor nos diga el id

        self.bus.conectar(self.ws_id)
        threading.Thread(target=self.hilo_comandos, daemon=True).start()
        threading.Thread(target=self.hilo_riego, daemon=True).start()
        log(f"Engine de {self.ws_id} listo")
        self.menu()


def leer_direccion(texto):
    try:
        host, puerto = texto.rsplit(":", 1)
        return host, int(puerto)
    except ValueError:
        print(f"Dirección no válida: {texto} (formato ip:puerto)")
        sys.exit(1)


def main():
    if len(sys.argv) != 3:
        print(__doc__)
        sys.exit(1)
    kafka = sys.argv[1]
    leer_direccion(kafka)    # solo para validar el formato
    monitor_host, monitor_port = leer_direccion(sys.argv[2])
    engine = Engine(monitor_host, monitor_port, KafkaBus(kafka))
    try:
        engine.arrancar()
    except KeyboardInterrupt:
        pass
    log("Engine detenido")


if __name__ == "__main__":
    main()