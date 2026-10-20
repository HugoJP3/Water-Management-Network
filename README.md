# Water Management Network
## SISTEMAS DISTRIBUIDOS
> Práctica no guiada: Sockets, Streaming de Eventos, Colas y modularidad.


# Información Despliegue

---
### Direcciones

| Quién se conecta | Dirección |
|--------------|--------------|
| Central, dentro de Docker             | broker:29092       |
| Monitor, Engine y FO, fuera de Docker | localhost:9092       |


---
### Lanzar Docker
Cómo generar y lanzar las imágenes de FO, Central, WS_E y WS_M

```
docker compose down                         # Limpiar

docker compose build central                # Central
docker build -t wm_fo ./WM_FO               # FO
docker build -t wm_ws_m ./WM_WS/WM_WS_M     # Monitor
docker build -t wm_ws_e ./WM_WS/WM_WS_E     # Engine

docker compose up -d                        # Lanzar

docker compose logs -f central  # Ctrl+C para salir
```

```
# Probar un operario
docker run -it --rm --network water-management-network_default wm_fo broker:29092 FO-01
```

```
# Probar un monitor en puerto local 6001
docker run -it --rm --name ws_m1 --network water-management-network_default -p 6001:6001 wm_ws_m 6001 wm_central:5000 WS-01 Campello
```

```
# Probar un engine conectado a ese monitor
docker run -it --rm --network water-management-network_default wm_ws_e broker:29092 ws_m1:6001
```

```
# Exportar (portabilidad futura)
docker save wm_fo wm_ws_m wm_ws_e -o /ruta_usb/wm.tar

#Cargar paquetes en máquina
docker load -i wm.tar
```

---
### Arrancar programas ( versión Local )
Cómo arranzar los programas en local para pruebas, sin Docker (solo para central) y sin Railway

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
Es necesario rehacer la imagen de Docker de central tras modificar operators.txt para que se apliquen los cambios.

---
### requirements.txt
Archivo en WM_FO pensado para ejecutar una serie de instrucciones sobre Water Stations sin necesidad de ejecutar manualmente todas ellas. Las instrucciones se ejecutan de forma secuencial dejando un margen de una a otra (margen de 4 segundos).

Edita el archivo requirements.txt en WM_FO para añadir instrucciones. Por ejemplo:

```
// estación#segundos
WS-01#10
WS-02#5
WS-01#8
```
Para ejecutar requirements.txt es necesario logearse como Field Operator y seleccionar la opción 