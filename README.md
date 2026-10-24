# 🌊 Water Management Network

**Sistemas Distribuidos:** Práctica no guiada de Sockets TCP, Streaming de Eventos (Apache Kafka), Colas y Arquitectura Modular.

---

## 📋 Índice
1. [Visión General y Requisitos](#-visión-general-y-requisitos)
2. [Direcciones de Red y Configuración (.env)](#-direcciones-de-red-y-configuración-env)
3. [Despliegue del Servidor Central (AWS EC2)](#-despliegue-del-servidor-central-aws-ec2)
4. [Construcción de Imágenes Docker](#-construcción-de-imágenes-docker)
5. [Ejecución de Clientes (Lanzador Inteligente)](#-ejecución-de-clientes-lanzador-inteligente)
6. [Ejecución Alternativa (Manual Docker y Local)](#-ejecución-alternativa-manual-docker-y-local)
7. [Manual de Uso de las Aplicaciones](#-manual-de-uso-de-las-aplicaciones)
8. [Configuración Avanzada (`operators.txt` y `requests.txt`)](#-configuración-avanzada)
9. [Registro de Prompts de IA](#-registro-de-prompts-de-ia)

---

## ⚙️ Visión General y Requisitos

### Tecnologías Utilizadas
* **Lenguaje:** Python 3.x
* **Contenerización:** Docker Desktop
* **Mensajería & Streaming:** Apache Kafka
* **Infraestructura Cloud:** AWS EC2 (Ubuntu)

### Requisitos Previos
* **Security Group (AWS):** Abrir puertos **5000/TCP** (Sockets Central), **9092/TCP** (Kafka) y **8080/TCP** (Panel Web Central) habilitados a `0.0.0.0/0`.
* ⚠️ **Nota sobre Redes Universitarias (UA):** La red Wi-Fi *eduroam/UA* suele bloquear puertos no estándar (5000 / 9092). Se recomienda utilizar la red Ethernet del laboratorio o un punto de acceso móvil.

---

## 🌐 Direcciones de Red y Configuración (`.env`)

Crea o edita un archivo `.env` en la raíz del proyecto para definir el entorno:

```env
PANEL_USER=admin
PANEL_PASSWORD=SDcontraseña_
EC2_PUBLIC_IP=198.51.100.24  # IP pública del servidor AWS EC2 o 'localhost' para pruebas
```

### Tabla de Conexiones
| Componente | Quién se conecta | Dirección / Puerto |
|---|---|---|
| **Central Web Panel** | Navegador Web | `http://<EC2_PUBLIC_IP>:8080` |
| **Central Sockets** | Monitores WS | `<EC2_PUBLIC_IP>:5000` |
| **Kafka Broker** | Clientes externos (FO, Engine) | `<EC2_PUBLIC_IP>:9092` |
| **Kafka Interno** | Comunicación interna en Docker (EC2) | `broker:29092` |

---

## ☁️ Despliegue del Servidor Central (AWS EC2)

> **Optimización en EC2:** En instancias `t3.micro`, habilita memoria SWAP para evitar colapsos de memoria al compilar/levantar Kafka:
> ```bash
> sudo fallocate -l 2G /swapfile && sudo chmod 600 /swapfile
> sudo mkswap /swapfile && sudo swapon /swapfile
> ```

### Pasos para desplegar o actualizar el servidor Central:

```bash
# 1. Actualizar repositorio
git pull

# 2. Configurar la IP pública actual en el .env
sed -i '/^EC2_PUBLIC_IP=/d' .env
echo "EC2_PUBLIC_IP=$(curl -s ifconfig.me)" >> .env

# 3. Levantar servicios con Docker Compose
docker compose down
docker compose build central
docker compose up -d

# 4. Ver registros en tiempo real (Ctrl+C para salir)
docker compose logs -f central
```

---

## 🐳 Construcción de Imágenes Docker

Para generar o distribuir las imágenes de los clientes (`wm_fo`, `wm_ws_m`, `wm_ws_e`):

```bash
# Construir imágenes en la máquina cliente
docker build -t wm_fo ./WM_FO               # Field Operator
docker build -t wm_ws_m ./WM_WS/WM_WS_M     # WS Monitor
docker build -t wm_ws_e ./WM_WS/WM_WS_E     # WS Engine

# (Opcional) Exportar / Cargar mediante paquete tar
docker save wm_fo wm_ws_m wm_ws_e -o wm_images.tar
docker load -i wm_images.tar
```

---

## 🚀 Ejecución de Clientes (Lanzador Inteligente)

El script `lanzar` gestiona automáticamente la detección de puertos libres (`6001+`), reutilización de IDs de instancias cerradas y asignación de terminales independientes.

### Sintaxis
* **Windows (CMD / PowerShell):** `.\lanzar.bat [CANTIDAD] <TIPO> [EXTRA]`
* **Linux / macOS / Git Bash:** `./lanzar.sh [CANTIDAD] <TIPO> [EXTRA]`

### Comandos Frecuentes
| Comando | Descripción |
|---|---|
| `.\Lanzar\lanzar.bat 10 fo` | Lanza 10 Field Operators (`FO-01` a `FO-10`) |
| `.\Lanzar\lanzar.bat fo FO-12 peticiones/prueba1.txt` | Lanza un Field Operator con ID `FO-12` y ejecuta el archivo peticiones/prueba1.txt buscado desde el path actual |
| `.\Lanzar\lanzar.bat 5 WS` | Lanza 5 Estaciones completas (10 terminales: 5 Monitores + 5 Engines) |
| `.\Lanzar\lanzar.bat WS Campello` | Lanza 1 WS completa asignando la ubicación "Campello" |
| `.\Lanzar\lanzar.bat WS WS-02 Campello` | Lanza 1 WS completa con ID `WS-02` |
| `.\Lanzar\lanzar.bat 2 monitor` | Lanza solo la parte Monitor de 2 estaciones |
| `.\Lanzar\lanzar.bat 2 engine` | Lanza solo la parte Engine (se asocia automáticamente a monitores libres) |
| `.\Lanzar\lanzar.bat status` | Muestra el estado de las instancias activas en el equipo |
| `.\Lanzar\lanzar.bat clean` | Detiene y elimina todos los contenedores de prueba activos |

---

## 💻 Ejecución Alternativa (Manual Docker y Local)

### 1. Despliegue Manual con Docker

```bash
# Field Operator (con volumen para peticiones)
docker run -it --rm --name fo1 -v "$(pwd)/peticiones:/app/peticiones" wm_fo <EC2_PUBLIC_IP>:9092 FO-01 [requests.txt]

# Water Station 01 (Monitor en puerto 6001 + Engine)
docker run -it --rm --name ws_m1 -p 6001:6001 wm_ws_m 6001 <EC2_PUBLIC_IP>:5000 WS-01 Campello
docker run -it --rm --name ws_e1 --add-host=host.docker.internal:host-gateway wm_ws_e <EC2_PUBLIC_IP>:9092 host.docker.internal:6001
```

### 2. Ejecución Local Directa (Python - Entorno de Desarrollo)

```powershell
# Definir EC2_PUBLIC_IP=localhost en el .env antes de ejecutar:

# Monitor
python .\WM_WS\WM_WS_M\wm_ws_m.py 6001 localhost:5000 WS-01 Alicante

# Engine
python .\WM_WS\WM_WS_E\wm_ws_e.py localhost:9092 localhost:6001

# Field Operator
python .\WM_FO\wm_fo.py localhost:9092 FO-01
```

---

## ⌨️ Manual de Uso de las Aplicaciones

### 1. Field Operator (FO)
* **[1] Pedir riego a una estación:** Especifica tiempo (segundos) e ID de la estación destino.
* **[2] Ejecutar fichero de peticiones:** Ejecuta `requests.txt` por defecto o un archivo personalizado presente en la carpeta montada `/peticiones`.
* **[3] Ver estaciones:** Muestra la lista de estaciones disponibles y su estado.
* **[4] Ver último resultado de servicio:** Muestra el último mensaje recibido por Central.

### 2. Engine (WS)
* **[1] Iniciar riego manual:** Activa el riego indicando la duración en segundos.
* **[2] Finalizar riego:** Detiene el riego activo en el momento.
* **[3] Simular / reparar avería:** Alterna entre estado de fallo (respuestas KO) u operativo (respuestas OK) ante la Central.
* **[4] Ver estado:** Consulta el estado actual de la Estación de Agua.

### 3. Panel Web Central (`http://<IP>:8080`)
* **Autenticación:** Usuario y contraseña definidos en `.env` desde AWS EC2.
* **Funcionalidades:** Iniciar/detener riego global, bloquear o activar estaciones, consultar el estado de FOs y examinar la traza completa del registro de eventos del sistema.

---

## 👥 Configuración Avanzada

### Gestión de Operarios (`WM_Central/operators.txt`)
Define el listado de operarios autorizados en la Central al iniciar el sistema:
```text
# Formato: ID o ID,Nombre
FO-01,Operario Alicante
FO-02,Operario Elche
FO-03,Operario San Vicente
```
> ⚠️ **Nota:** Al arrancar o reiniciarse la Central (`docker compose restart central`), la base de datos sincronizará su estado con este fichero. No hay otra forma de inscribir/eliminar operarios en la base de datos.

### Gestión de Estaciones (`WM_Central/stations.txt`)
Define el listado de estaciones autorizados en la Central al iniciar el sistema:
```text
# Formato: ID o ID,Ubicacion
WS-01,Campello
WS-02,Retiro
WS-03,San Juan
```
> ⚠️ **Nota:** Al arrancar o reiniciarse la Central (`docker compose restart central`), la base de datos sincronizará su estado con este fichero. No hay otra forma de inscribir/eliminar estaciones en la base de datos.

### Fichero de Peticiones (`requests.txt`)
Permite automatizar secuencias de riego. Cada línea especifica una orden de la forma `ESTACIÓN#SEGUNDOS`:
```text
// Formato: WS-ID#Duracion
WS-01#10
WS-02#5
WS-01#8
```
> ⚠️ **Nota:** `lanzar` monta automáticamente una carpeta `peticiones/` de tu equipo con los Field Operators. Así, podrás crear dentro de ella tantos `requests.txt` como desees para automatizar procesos. Si usas un método manual de despliegue, deberás asignarla como se ha especificado anteriormente.

---

## 📁 Registro de Prompts de IA

En cumplimiento con la normativa de la asignatura, el historial completo de consultas y soporte mediante Inteligencia Artificial utilizado durante la arquitectura y desarrollo de este proyecto se encuentra documentado en la carpeta `/prompts`.