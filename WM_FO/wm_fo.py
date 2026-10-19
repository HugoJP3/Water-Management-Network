"""
wm_fo.py  (WM_FO) - Aplicación del operario de campo (Field Operator).

Uso:
    python wm_fo.py <ip_kafka:puerto> <id_operario> [fichero_peticiones]

Ejemplos:
    python wm_fo.py localhost:9092 FO-12
    python wm_fo.py localhost:9092 FO-12 requests.txt     (ejecuta el fichero al arrancar)

Todo va por KAFKA (este módulo no usa sockets):
    publica en  fo-requests       REQUEST#FO-12#WS-01#60
    escucha en  fo-notifications  NOTIFY#FO-12#WS-01#texto
                                  REPLY#FO-12#WS-01#ACCEPTED   (o DENIED#motivo)
                                  SUMMARY#FO-12#WS-01#litros#segundos#motivo
    escucha en  ws-status         STATIONS#WS-01|Campello|FUGA#WS-02|Retiro|DISPONIBLE

Cómo termina una petición:
    - REPLY DENIED   -> fallo (fin)
    - REPLY ACCEPTED -> se queda esperando el SUMMARY (fin con éxito)
    - Central no contesta a tiempo -> fallo (fin)

Fichero de peticiones: una por línea, estación y segundos, por ejemplo
        WS-01#60
        WS-02#30
    (también vale separar con coma, punto y coma o espacio).
    Se ignoran las líneas vacías y las que empiezan por //
    Se envía la primera y, cuando termina (con éxito o fracaso), se esperan
    4 segundos antes de enviar la siguiente.
"""
import os
import re
import sys
import threading
import time

TOPIC_PETICIONES = "fo-requests"
TOPIC_NOTIFICACIONES = "fo-notifications"
TOPIC_ESTADOS = "ws-status"       # propuesta, ver arriba

ESPERA_REPLY = 15       # segundos esperando que Central autorice o deniegue
MARGEN_RESUMEN = 30     # segundos extra (sobre la duración) esperando el SUMMARY
PAUSA_FICHERO = 4       # segundos entre peticiones del fichero (lo pide el enunciado)
FICHERO_DEFECTO = os.path.join(os.path.dirname(__file__), "requests.txt")

SEP = "#"


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

    def conectar(self, fo_id):
        from kafka import KafkaProducer, KafkaConsumer
        try:
            from kafka.errors import NoBrokersAvailable as ErrorSinBroker
        except ImportError:
            from kafka.errors import KafkaTimeoutError as ErrorSinBroker
        while True:
            try:
                self.producer = KafkaProducer(bootstrap_servers=self.bootstrap)
                # group_id ÚNICO por operario y por ejecución: cada FO recibe
                # TODAS las notificaciones y filtra por su id (si compartieran
                # grupo, Kafka se las repartiría y a algunos no les llegarían).
                self.consumer = KafkaConsumer(
                    TOPIC_NOTIFICACIONES, TOPIC_ESTADOS,
                    bootstrap_servers=self.bootstrap,
                    group_id=f"fo-{fo_id}-{int(time.time())}",
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
        Si enviáramos una petición antes, la respuesta de Central se perdería.
        Por eso esperamos aquí a tenerlas asignadas y fijamos la posición."""
        limite = time.time() + 20
        while not self.consumer.assignment() and time.time() < limite:
            self.consumer.poll(timeout_ms=500)
        for particion in self.consumer.assignment():
            self.consumer.position(particion)

    def publish(self, topic, mensaje):
        try:
            self.producer.send(topic, mensaje.encode("utf-8"))
            self.producer.flush(timeout=3)
        except Exception as e:
            log(f"No se pudo publicar en {topic}: {e}")

    def escuchar(self, callback):
        """Bloquea para siempre, llamando a callback(tema, texto) por cada mensaje."""
        for registro in self.consumer:
            try:
                callback(registro.topic, registro.value.decode("utf-8"))
            except Exception as e:
                log(f"Error procesando mensaje de Kafka: {e}")


# ===================================================================== FO
class FieldOperator:
    def __init__(self, fo_id, bus):
        self.id = fo_id
        self.bus = bus
        # Condition = candado + "avisador": el hilo que pide el riego se duerme
        # esperando y el hilo de Kafka lo despierta cuando llega la respuesta.
        self.cond = threading.Condition()
        self.peticion = None      # petición en curso (dict) o None
        self.estaciones = {}      # ws_id -> (estado, ubicación), del tema ws-status

    # ------------------------------------------- mensajes que llegan por Kafka
    def procesar_mensaje(self, tema, texto):
        if tema == TOPIC_ESTADOS:
            self._procesar_estado(texto)
        else:
            self._procesar_notificacion(texto)

    def _procesar_estado(self, texto):
        campos = texto.split(SEP)
        if campos[0] != "STATIONS":
            return
        nuevas = {}
        for dato in campos[1:]:
            partes = dato.split("|")
            if len(partes) == 3:
                ws, ubicacion, estado = partes
                nuevas[ws] = (estado, ubicacion)
        with self.cond:
            self.estaciones = nuevas

    def _procesar_notificacion(self, texto):
        campos = texto.split(SEP)
        # Solo me interesan los mensajes dirigidos a MI id
        if len(campos) < 3 or campos[1] != self.id:
            return
        tipo, ws = campos[0], campos[2]

        if tipo == "NOTIFY":
            log(f"[{ws}] {SEP.join(campos[3:])}")

        elif tipo == "REPLY":
            veredicto = campos[3] if len(campos) > 3 else ""
            if veredicto == "ACCEPTED":
                log(f"[{ws}] RIEGO AUTORIZADO")
                with self.cond:
                    p = self.peticion
                    if p is not None and p["ws"] == ws and p["fase"] == "REPLY":
                        p["fase"] = "REGANDO"
                        p["limite"] = time.time() + p["segundos"] + MARGEN_RESUMEN
                        self.cond.notify_all()
            else:
                motivo = campos[4] if len(campos) > 4 else "sin motivo"
                log(f"[{ws}] RIEGO DENEGADO: {motivo}")
                self._terminar_peticion(ws, False, f"denegado: {motivo}")

        elif tipo == "SUMMARY":
            litros = campos[3] if len(campos) > 3 else "?"
            segundos = campos[4] if len(campos) > 4 else "?"
            motivo = campos[5] if len(campos) > 5 else "?"
            log(f"[{ws}] RESUMEN FINAL: {litros} L en {segundos} s (motivo: {motivo})")
            self._terminar_peticion(ws, True, f"{litros} L en {segundos} s")

    def _terminar_peticion(self, ws, exito, detalle):
        with self.cond:
            p = self.peticion
            if p is not None and p["ws"] == ws and p["resultado"] is None:
                p["resultado"] = (exito, detalle)
                self.cond.notify_all()

    # ----------------------------------------------------------- pedir riego
    def pedir_riego(self, ws, segundos):
        """Envía la petición y BLOQUEA hasta que termina (resumen, denegación o
        timeout). Devuelve True si el riego se hizo, False si falló."""
        with self.cond:
            if self.peticion is not None:
                print("Ya hay una petición en curso")
                return False
            self.peticion = {"ws": ws, "segundos": segundos, "fase": "REPLY",
                             "limite": time.time() + ESPERA_REPLY, "resultado": None}
        log(f"Pidiendo riego en {ws} durante {segundos} s...")
        self.bus.publish(TOPIC_PETICIONES, SEP.join(["REQUEST", self.id, ws, str(segundos)]))

        with self.cond:
            p = self.peticion
            while p["resultado"] is None:
                restante = p["limite"] - time.time()
                if restante <= 0:
                    if p["fase"] == "REPLY":
                        p["resultado"] = (False, "Central no ha contestado")
                    else:
                        p["resultado"] = (False, "no llegó el resumen final")
                    log(f"[{ws}] {p['resultado'][1]}")
                    break
                self.cond.wait(timeout=restante)
            exito, detalle = p["resultado"]
            self.peticion = None
        log(f"Petición a {ws} terminada: {'ÉXITO' if exito else 'FALLO'} ({detalle})")
        return exito

    # --------------------------------------------------------------- fichero
    def leer_fichero(self, ruta):
        """Devuelve la lista [(ws, segundos), ...]. Las líneas mal formadas se avisan y se saltan."""
        peticiones = []
        with open(ruta, encoding="utf-8") as f:
            for n, linea in enumerate(f, 1):
                linea = linea.strip()
                if not linea or linea.startswith("//"):
                    continue
                partes = [x for x in re.split(r"[#,;\s]+", linea) if x]
                try:
                    peticiones.append((partes[0].upper(), int(partes[1])))
                except (IndexError, ValueError):
                    print(f"Línea {n} ignorada (formato no válido): {linea!r}")
        return peticiones

    def ejecutar_fichero(self, ruta):
        try:
            peticiones = self.leer_fichero(ruta)
        except OSError as e:
            print(f"No se puede leer {ruta}: {e}")
            return
        if not peticiones:
            print("El fichero no tiene peticiones")
            return
        log(f"Ejecutando {len(peticiones)} peticiones de {ruta} (Ctrl+C para parar)")
        try:
            for i, (ws, segundos) in enumerate(peticiones, 1):
                log(f"Petición {i} de {len(peticiones)}")
                self.pedir_riego(ws, segundos)
                if i < len(peticiones):
                    log(f"Esperando {PAUSA_FICHERO} s antes de la siguiente...")
                    time.sleep(PAUSA_FICHERO)
        except KeyboardInterrupt:
            log("Fichero interrumpido")
            return
        log("Fichero completado")

    # ------------------------------------------------------------- estaciones
    def mostrar_estaciones(self):
        with self.cond:
            copia = dict(self.estaciones)
        if not copia:
            print("Sin información de estaciones (Central aún no ha publicado "
                  f"nada en {TOPIC_ESTADOS}).")
            return
        print(f"\n  {'ESTACIÓN':<10}{'ESTADO':<22}UBICACIÓN")
        for ws in sorted(copia):
            estado, ubicacion = copia[ws]
            print(f"  {ws:<10}{estado:<22}{ubicacion}")

    # ------------------------------------------------------------------ menú
    def menu(self):
        ayuda = ("\n  [1] Pedir riego a una estación\n"
                 "  [2] Ejecutar fichero de peticiones\n"
                 "  [3] Ver estaciones\n"
                 "  [h] Ayuda    [0] Salir")
        print(ayuda)
        while True:
            try:
                opcion = input("> ").strip().lower()
            except EOFError:
                log("Sin teclado (stdin cerrado): termino.")
                return
            except KeyboardInterrupt:
                return

            if opcion == "1":
                ws = input("Estación (ej. WS-01): ").strip().upper()
                try:
                    segundos = int(input("Duración en segundos: ").strip())
                except ValueError:
                    print("Número no válido")
                    continue
                if ws:
                    self.pedir_riego(ws, segundos)
            elif opcion == "2":
                ruta = input(f"Fichero [{FICHERO_DEFECTO}]: ").strip() or FICHERO_DEFECTO
                self.ejecutar_fichero(ruta)
            elif opcion == "3":
                self.mostrar_estaciones()
            elif opcion == "h":
                print(ayuda)
            elif opcion == "0":
                return
            elif opcion:
                print("Opción no válida (h = ayuda)")

    # --------------------------------------------------------------- arranque
    def arrancar(self, fichero=None):
        self.bus.conectar(self.id)
        threading.Thread(target=self.bus.escuchar, args=(self.procesar_mensaje,),
                         daemon=True).start()
        log(f"Operario {self.id} listo")
        if fichero:
            self.ejecutar_fichero(fichero)
        self.menu()


def main():
    if len(sys.argv) not in (3, 4):
        print(__doc__)
        sys.exit(1)
    kafka, fo_id = sys.argv[1], sys.argv[2]
    fichero = sys.argv[3] if len(sys.argv) == 4 else None
    if ":" not in kafka:
        print(f"Dirección de Kafka no válida: {kafka} (formato ip:puerto)")
        sys.exit(1)
    fo = FieldOperator(fo_id, KafkaBus(kafka))
    try:
        fo.arrancar(fichero)
    except KeyboardInterrupt:
        pass
    log("Operario detenido")


if __name__ == "__main__":
    main()