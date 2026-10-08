"""
    Módulo de WM_Central: Lógica de negocio.
    - Validar estación.
    - Reservar estación.
    - Cortar un riego.

    No sabe nada de Kafka.
    Cuando necesita enviar algo llama a send(topic, msg) (que está en kafka_io.py).
    
"""