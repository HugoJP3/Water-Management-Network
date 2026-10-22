# Water Management Network

**Sistemas Distribuidos:** Práctica no guiada de Sockets, Streaming de Eventos, Colas y modularidad. Herramientas usadas: Docker Desktop, Apache Kafka, AWS (EC2) y Python.


# Información Despliegue

> Nota: Los comandos están pensados para ejecución en Windows (salvo AWS EC2 y especificación puntual).

### Requisitos previos y Notas

- Herramientas: Docker, kafka-python, kafka-compose,git y python.
- Configurar el .env con PANEL_PASSWORD y EC2_PUBLIC_IP.
- Security Group: Puertos 5000, 9092 y 8080 abiertos.
- Red UA: La red wifi de la UA puede bloquear puertos. Intenta utilizar Ethernet de la UA o conectarte a una red auxiliar para pruebas en la universidad.

---
### Direcciones

| Quién se conecta | Dirección |
|--------------|--------------|
| Central, dentro de Docker             | broker:29092       |
| Monitor, Engine y FO, fuera de Docker | <IP_PUBLICA>:9092       |

---
### AWS EC2

Modificar .env para tomar nueva IP. En la terminal de la máquina:

```
sed -i '/^EC2_PUBLIC_IP=/d' .env
echo "EC2_PUBLIC_IP=$(curl -s ifconfig.me)" >> .env
```

Modificar .env para modificar la contraseña o el usuario:

```
nano .env # Desde aquí, edita PANEL_PASSWORD y PANEL_USER
```

Despliegue en EC2:

```
git pull
docker compose build central
docker compose up -d
```

---


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
docker run -it --rm wm_fo <IP_PUBLICA>:9092 FO-01
```

```
# Despliegue de estaciones
# > El puerto de la derecha del -p de Monitor tiene que ser el mismo que el primer argumento del Monitor.

# Estación 1 en puerto local 6001
docker run -it --rm --name ws_m1 -p 6001:6001 wm_ws_m 6001 <IP_PUBLICA>:5000 WS-01 Campello

docker run -it --rm --add-host=host.docker.internal:host-gateway wm_ws_e <IP_PUBLICA>:9092 host.docker.internal:6001

# Estación 2 en puerto local 6002
docker run -it --rm --name ws_m2 -p 6002:6002 wm_ws_m 6002 <IP_PUBLICA>:5000 WS-02 Retiro

docker run -it --rm --add-host=host.docker.internal:host-gateway wm_ws_e <IP_PUBLICA>:9092 host.docker.internal:6002

```

```
# Exportar (portabilidad futura)
docker save wm_fo wm_ws_m wm_ws_e -o /ruta_usb/wm.tar

#Cargar paquetes en máquina
docker load -i wm.tar
```

---
### Arrancar programas ( versión Local )
Cómo arrancar los programas en local para pruebas, sin Docker (solo para central) y sin EC2.

| Programa      | comando (Windows PowerShell) |
|-------------- |--------------|
| **Central**       | Lanzar Docker (arriba) |
| **Monitor**       | python .\WM_WS\WM_WS_M\wm_ws_m.py *puerto_monitor* localhost:5000 ID_ws *ubicacion*      |
| **Engine**        | python .\WM_WS\WM_WS_E\wm_ws_e.py localhost:9092 localhost:*puerto_monitor* |
| **FO**            | python .\WM_FO\wm_fo.py localhost:9092 *ID_operario* |

Recuerda modificar el .env para asignar la IP de despliegue para tu central.
```
EC2_PUBLIC_IP=localhost # O tu IP personal.
```

---
### Añadir operarios: operators.txt
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
> NOTA: El arquivo operators.txt se copia dentro de la imagen de wm_central. Modificarlo no influirá en la BBDD a menos que se reconstruya la imagen y se relance.


---
### Añadir instrucciones: requests.txt
Archivo en WM_FO pensado para ejecutar una serie de instrucciones sobre Water Stations sin necesidad de ejecutar manualmente todas ellas. Las instrucciones se ejecutan de forma secuencial dejando un margen de una a otra (margen de 4 segundos).

> NOTA: El arquivo requests.txt se copia dentro de la imagen de wm_fo. Modificarlo no influirá en la salidad a menos que se reconstruya la imagen y se relance. (Esto no afecta a ejecuciones locales con python)

Edita el archivo requests.txt en WM_FO para añadir instrucciones. Por ejemplo:

```
// estación#segundos
WS-01#10
WS-02#5
WS-01#8
```
Para ejecutar requests.txt es necesario ejecutar una sesión como Field Operator y seleccionar la opción 2 (Ejecutar fichero de peticiones).

---

### Consultar el panel web

Accede a la URL *http://IP_PUBLICA:8080*
Deberás sustituir IP_PUBLICA por la IP que utilices según tu máquina y que tendrá que constar en el .env.