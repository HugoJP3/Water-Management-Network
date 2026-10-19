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
### Añadir operarios
Añade operarios al archivo operators.txt que se carga al iniciar central. Si no se carga ningún archivo, se crearán IDs de 01 a 20.
Por ejemplo:
```
# (Un operario por línea: ID o ID,Nombre)
# Operarios dados de alta en CENTRAL
FO-01,Operario 1
FO-02,Operario 2
FO-03,Operario 3
FO-04,Operario 4
FO-05,Operario 5
FO-12,Operario 12
```

---
### Requirements.txt
Archivo en WM_FO pensado para ejecutar una serie de instrucciones sobre Water Stations sin necesidad de ejecutar manualmente todas ellas. Las instrucciones se ejecutan de forma secuencial dejando un margen de una a otra.

Edita el archivo requirements.txt en WM_FO para añadir instrucciones. Por ejemplo:

```
// estación#segundos
WS-01#10
WS-02#5
WS-01#8
```