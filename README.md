# Water Management Network
### SISTEMAS DISTRIBUIDOS
> Práctica no guiada: Sockets, Streaming de Eventos, Colas y modularidad.


# Información clave Despliegue

---
### Direcciones

| Quién se conecta | Dirección |
|--------------|--------------|
| Central, dentro de Docker             | broker:29092       |
| Monitor, Engine y FO, fuera de Docker | localhost:9092       |


---
### Lanzar Docker

```
docker compose down             # (opcional) Limpiar lo anterior
docker compose build central    # Generar la imagen
docker compose up -d
docker compose logs -f central  # Ctrl+C para salir de los logs
```

---
### Arrancar programas ( versión beta antes de despliegue en Railway )

| Programa      | comando |
|-------------- |--------------|
| **Central**       | Lanzar Docker (arriba) |
| **Monitor**       | python .\WM_WS\WM_WS_M\wm_ws_m.py *puerto_monitor* localhost:5000 ID_ws *ubicacion*      |
| **Engine**        | python .\WM_WS\WM_WS_E\wm_ws_e.py localhost:9092 localhost:*puerto_monitor* (puerto) |
| **FO**            | python .\WM_FO\wm_fo.py localhost:9092 *ID_operario* |

---