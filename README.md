# Water Management Network

**Sistemas Distribuidos:** Práctica no guiada de Sockets, Streaming de Eventos, Colas y modularidad. Herramientas usadas: Docker Desktop, Apache Kafka, AWS (EC2) y Python.

## 📁 Registro de Prompts de IA

Siguiendo la normativa de la asignatura, el historial de consultas y apoyos de Inteligencia Artificial utilizados durante el desarrollo de la práctica se encuentra documentado en la carpeta `/prompts` de este repositorio.

## ⌨️ Uso de las aplicaciones

### Field Operator

    [1] Pedir riego a una estación
    [2] Ejecutar fichero de peticiones
    [3] Ver estaciones
    [4] Ver último resultado de servicio
    [h] Ayuda    [0] Salir

> 1. Pedir riego a una estación indicando el tiempo en segundos de riego y el identificador de la estación.
> 2. Ejecutar fichero de peticiones:
>    - Si no introduces nada, se ejecuta el fichero requests.txt por defecto asignado a la imagen del FO.
>    - Si introduces algo, buscará en el espacio de trabajo asociado a la imagen del operario (consulta más abajo cómo).
> 3. Ver el estado de las estaciones.

### Engine

    [1] Iniciar riego manual        [2] Finalizar riego
    [3] Simular / reparar avería    [4] Ver estado
    [h] Ayuda                       [0] Salir

> 1. Iniciar riego manual indicando el tiempo en segundos del mismo.
> 2. Finalizar un riego activo.
> 3. ⚠️ Simular o reparar una avería: Simula respuesta KO / Activa respuesta OK hacia el Monitor.
> 4. Consulta el estado de mi WS.

### Central

Accede al panel web desde la URL `http://IP_PUBLICA:8080`. Deberás sustituir IP_PUBLICA por la IP que utilices según tu máquina y que tendrá que constar en el `.env`.

Deberás introducir la contraseña y el usuario que figure en tu `.env` de despliegue.
- admin / SDcontraseña_

Utilidades:
- Iniciar riego.
- Bloquear/Activar WS.
- Consultar el estado de las WS.
- Consultar la lista de FOs registrados y su estado (Libre/Regando).
- Consultar información del registro de eventos del sistema.

## 🚀 Información Despliegue

> Nota: Los comandos están pensados para ejecución en Windows (salvo AWS EC2 y especificación puntual).

### Requisitos previos y Notas

- **Herramientas:** Docker Desktop, Python 3.x, Git.
- **Configuración:** Crear archivo `.env` en la raíz con `PANEL_USER`, `PANEL_PASSWORD` y `EC2_PUBLIC_IP`.
- **Security Group (AWS):** Puertos **5000/TCP** (Sockets Central), **9092/TCP** (Kafka) y **8080/TCP** (Panel Web) abiertos a `0.0.0.0/0`.
- **Red de la Universidad (UA):** La Wi-Fi eduroam/UA puede bloquear los puertos 5000 o 9092. Se recomienda usar la red Ethernet del laboratorio o un punto de acceso móvil.

### Direcciones de Red

| Componente | Quién se conecta | Dirección |
|---|---|---|
| **Central** | Interno dentro de Docker (EC2) | `broker:29092` |
| **Monitor, Engine y FO** | Clientes externos (Fuera de Docker) | `<IP_PUBLICA_EC2>:9092` |

### ☁️ Servidor AWS EC2

> NOTA: Nuestro despliegue en AWS EC2 se realizó con ubuntu y t3.micro. Fue necesario añadir memoria SWAP adicional para evitar colapsos al levantar o compilar Kafka.

```
sudo fallocate -l 2G /swapfile
sudo chmod 600 /swapfile
sudo mkswap /swapfile
sudo swapon /swapfile
```

Modificar .env para tomar nueva IP. En la terminal de la máquina:

```
sed -i '/^EC2_PUBLIC_IP=/d' .env
echo "EC2_PUBLIC_IP=$(curl -s ifconfig.me)" >> .env
```

Modificar `.env` para modificar la contraseña o el usuario:

```
nano .env # Desde aquí, edita PANEL_PASSWORD y PANEL_USER
```

Despliegue y actualización del servidor:

```
git pull
docker compose down                         # Limpiar
docker compose build central                # Central
docker compose up -d                        # Lanzar 

docker compose logs -f central      # Ctrl+C para salir
```


### 🐳 Construir las imágenes con Docker en Clientes
Cómo generar las imágenes de FO, WS_E y WS_M.

```
# Desde un equipo

docker build -t wm_fo ./WM_FO               # FO
docker build -t wm_ws_m ./WM_WS/WM_WS_M     # Monitor
docker build -t wm_ws_e ./WM_WS/WM_WS_E     # Engine

# Exportar a USB (opcional)
docker save wm_fo wm_ws_m wm_ws_e -o wm_images.tar

#Cargar paquetes en máquina desde USB
docker load -i wm_images.tar
```


### 💻 Lanzar las imágenes Cliente

#### 1. Field Operator

> NOTA: Móntale un espacio de trabajo al operario para poder añadir, modificar y ejecutar archivos de peticiones (requests.txt) sin necesidad de rehacer la imagen del FO.

```
# Probar un operario (sin volumen):
docker run -it --rm wm_fo <IP_PUBLICA>:9092 FO-01 [requests.txt]

# Modo producción (carpeta montada para peticiones):
docker run -it --rm --name fo1 -v "$(pwd)/peticiones:/app/peticiones" wm_fo 16.171.132.242:9092 FO-01 [requests.txt]

```

#### 2. Water Stations

> NOTA: El puerto de la derecha del -p de Monitor tiene que ser el mismo que el primer argumento del Monitor.

```
# Despliegue de estaciones

# Estación 1 en puerto local 6001
docker run -it --rm --name ws_m1 -p 6001:6001 wm_ws_m 6001 <IP_PUBLICA>:5000 WS-01 Campello

docker run -it --rm --add-host=host.docker.internal:host-gateway wm_ws_e <IP_PUBLICA>:9092 host.docker.internal:6001

# Estación 2 en puerto local 6002
docker run -it --rm --name ws_m2 -p 6002:6002 wm_ws_m 6002 <IP_PUBLICA>:5000 WS-02 Retiro

docker run -it --rm --add-host=host.docker.internal:host-gateway wm_ws_e <IP_PUBLICA>:9092 host.docker.internal:6002

```

### ⚡ Arrancar programas ( versión Local / sólo desarrollo)
Cómo arrancar los programas en local para pruebas sin construir su imagen en Docker.

| Programa      | comando (PowerShell) |
|-------------- |--------------|
| **Monitor**       | python .\WM_WS\WM_WS_M\wm_ws_m.py *puerto_monitor* localhost:5000 ID_ws *ubicacion*      |
| **Engine**        | python .\WM_WS\WM_WS_E\wm_ws_e.py localhost:9092 localhost:*puerto_monitor* |
| **FO**            | python .\WM_FO\wm_fo.py localhost:9092 *ID_operario* |

Recuerda modificar el .env para asignar la IP de despliegue para tu central.
```
EC2_PUBLIC_IP=localhost # O tu IP personal.
```

### 👥 Gestión de operarios: operators.txt
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
> NOTA: El arquivo operators.txt se ejecuta al arranque de central. Toda la base de datos se sincroniza con su contenido al inicio.

> PRECAUCIÓN: Todos los operarios que no aparezcan en operators.txt serán eliminados o modificados.

Para aplicar cambios en la lista de operarios sin rehacer la imagen:

```
docker compose restart central
```

### 👥 Añadir instrucciones: requests.txt
Archivo en WM_FO pensado para ejecutar una serie de instrucciones sobre Water Stations sin necesidad de ejecutar manualmente todas ellas. Las instrucciones se ejecutan de forma secuencial dejando un margen de una a otra (margen de 4 segundos).

> NOTA: El archivo requests.txt se copia dentro de la imagen de wm_fo. Modificarlo no influirá en la salida a menos que se reconstruya la imagen y se relance. (Esto no afecta a ejecuciones locales con python)

Edita el archivo requests.txt en WM_FO para añadir instrucciones. Por ejemplo:

```
// estación#segundos
WS-01#10
WS-02#5
WS-01#8
```
Para ejecutar requests.txt es necesario ejecutar una sesión como Field Operator y seleccionar la opción 2 (Ejecutar fichero de peticiones).