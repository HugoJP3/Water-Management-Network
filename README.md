# Water Management Network
### SISTEMAS DISTRIBUIDOS
> Práctica no guiada: Sockets, Streaming de Eventos, Colas y modularidad.


# Información clave Despliegue

> Direcciones

| Quién se conecta | Dirección |
|--------------|--------------|
| Central, dentro de Docker       | broker:29092       |
| Monitor, Engine y FO, fuera de Docker       | localhost:9092       |



> Lanzar Docker

```
docker compose down             # (opcional) Limpiar lo anterior
docker compose build central    # Generar la imagen
docker compose up -d
docker compose logs -f central  # Ctrl+C para salir de los logs
```