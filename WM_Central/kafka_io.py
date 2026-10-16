"""
kafka_io.py - Conexión de WM_Central con Kafka (librería confluent-kafka).

Conceptos:
  - Producer: escribe mensajes en un topic. Es seguro usarlo desde varios hilos.
  - Consumer: lee de uno o varios topics. Pertenece a un 'grupo' (group.id):
    Kafka reparte los mensajes entre los consumidores del MISMO grupo.
    Central es un único proceso con su grupo 'wm-central'.
  - Admin: crea los topics al arrancar, para que existan antes de que nadie
    los use.
"""
import time
import traceback

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

import logic
import protocol as proto

_producer = None


# ----------------------------------------------------------- arranque
def _wait_for_broker(bootstrap: str, attempts: int = 15) -> AdminClient:
    """Espera a que Kafka responda (en Docker/Railway puede tardar en arrancar)."""
    admin = AdminClient({"bootstrap.servers": bootstrap})
    for i in range(1, attempts + 1):
        try:
            admin.list_topics(timeout=5)
            return admin
        except KafkaException:
            logic.log(f"Esperando al broker Kafka en {bootstrap} ({i}/{attempts})...")
            time.sleep(2)
    raise SystemExit(f"No se pudo conectar con Kafka en {bootstrap}")


def _create_topics(admin: AdminClient):
    nuevos = [NewTopic(t, num_partitions=1, replication_factor=1)
              for t in logic.ALL_TOPICS]
    for topic, future in admin.create_topics(nuevos).items():
        try:
            future.result()
            logic.log(f"Topic creado: {topic}")
        except KafkaException as e:
            # Si ya existía no es un error
            if e.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise


def connect(bootstrap: str):
    """Comprueba el broker, crea los topics y registra el productor en logic."""
    global _producer
    admin = _wait_for_broker(bootstrap)
    _create_topics(admin)
    _producer = Producer({"bootstrap.servers": bootstrap})

    def on_delivery(err, msg):
        if err is not None:
            logic.log(f"ERROR enviando a Kafka ({msg.topic()}): {err}")

    def sender(topic: str, message: str):
        try:
            _producer.produce(topic, value=message.encode("utf-8"),
                              callback=on_delivery)
        except BufferError:           # cola interna llena: dejamos que se vacíe
            _producer.poll(1.0)
            _producer.produce(topic, value=message.encode("utf-8"),
                              callback=on_delivery)
        _producer.poll(0)             # atiende los callbacks pendientes

    logic.set_sender(sender)
    logic.log(f"Conectado a Kafka ({bootstrap}). Topics: {logic.ALL_TOPICS}")


def close():
    """Vacía lo pendiente antes de apagar."""
    if _producer is not None:
        _producer.flush(5)


# ----------------------------------------------------------- consumidor
def consume_forever(bootstrap: str, stop_event):
    """Bucle del hilo consumidor: lee fo-requests y ws-telemetry y llama a
    la lógica. Se ejecuta en su propio hilo."""
    consumer = Consumer({
        "bootstrap.servers": bootstrap,
        "group.id": "wm-central",
        # 'latest': si Central estuvo caído, no reprocesa peticiones antiguas
        # (no queremos abrir riegos de hace horas).
        "auto.offset.reset": "latest",
        "enable.auto.commit": True,
    })
    consumer.subscribe([logic.T_FO_REQUESTS, logic.T_WS_TELEMETRY])
    logic.log("Consumidor Kafka escuchando fo-requests y ws-telemetry")

    try:
        while not stop_event.is_set():
            msg = consumer.poll(1.0)        # espera hasta 1 s un mensaje
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    logic.log(f"Error del consumidor Kafka: {msg.error()}")
                continue

            try:
                fields = proto.unpack(msg.value().decode("utf-8"))
                if msg.topic() == logic.T_FO_REQUESTS:
                    logic.handle_request(fields)
                elif msg.topic() == logic.T_WS_TELEMETRY:
                    logic.handle_telemetry(fields)
            except Exception:
                # Un mensaje roto no debe tumbar a Central
                logic.log("ERROR procesando un mensaje de Kafka:\n"
                          + traceback.format_exc())
    finally:
        consumer.close()