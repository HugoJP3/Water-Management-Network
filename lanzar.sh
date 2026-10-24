#!/usr/bin/env bash
# ==============================================================================
# Script para lanzar instancias en Docker en terminales independientes
# Water Management Network - Sistemas Distribuidos
# ==============================================================================

# Obtener directorio base del script
DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"
cd "$DIR" || exit 1

# ------------------------------------------------------------------------------
# 1. Cargar variables de entorno desde .env
# ------------------------------------------------------------------------------
if [ -f "$DIR/.env" ]; then
    # Leer variables exportando las necesarias sin ejecutar código arbitrario
    EC2_PUBLIC_IP=$(grep -E '^EC2_PUBLIC_IP=' "$DIR/.env" | cut -d '=' -f2- | tr -d '\r' | tr -d '"' | tr -d "'")
fi

# Fallback si no está definida en .env
EC2_PUBLIC_IP="${EC2_PUBLIC_IP:-localhost}"
CENTRAL_PORT="5000"
KAFKA_PORT="9092"
PUERTO_BASE_WS=6000

# ------------------------------------------------------------------------------
# 2. Función de ayuda
# ------------------------------------------------------------------------------
mostrar_ayuda() {
    echo "================================================================="
    echo "  Lanzador de instancias - Water Management Network"
    echo "================================================================="
    echo "Uso: ./lanzar.sh [CANTIDAD] <TIPO> [EXTRA]"
    echo ""
    echo "Tipos disponibles:"
    echo "  fo | FO          : Field Operator (operario de campo)"
    echo "  ws | WS          : Water Station completa (Monitor + Engine)"
    echo "  engine | e       : Solo Engine(s) de Water Station"
    echo "  monitor | m      : Solo Monitor(es) de Water Station"
    echo "  clean | stop     : Detiene y elimina los contenedores lanzados"
    echo ""
    echo "Ejemplos:"
    echo "  ./lanzar.sh 20 fo            -> Lanza 20 FOs en terminales (FO-01 a FO-20)"
    echo "  ./lanzar.sh 5 WS             -> Lanza 5 WS completas (Monitor + Engine)"
    echo "  ./lanzar.sh WS Campello      -> Lanza 1 WS completa con ubicación Campello"
    echo "  ./lanzar.sh Engine           -> Lanza 1 Engine (para WS-01)"
    echo "  ./lanzar.sh 3 Engine         -> Lanza 3 Engines (WS-01 a WS-03)"
    echo "  ./lanzar.sh fo FO-01 test.txt-> Lanza 1 FO con fichero de peticiones"
    echo "  ./lanzar.sh clean            -> Detiene todos los contenedores fo/ws"
    echo ""
    echo "Configuración actual:"
    echo "  IP Servidor (EC2 / Local) : $EC2_PUBLIC_IP"
    echo "  Kafka                     : $EC2_PUBLIC_IP:$KAFKA_PORT"
    echo "  Central Sockets           : $EC2_PUBLIC_IP:$CENTRAL_PORT"
    echo "================================================================="
}

# ------------------------------------------------------------------------------
# 3. Función para abrir una nueva terminal interactiva
# ------------------------------------------------------------------------------
lanzar_terminal() {
    local titulo="$1"
    local comando="$2"

    # Caso 1: En Windows (Git Bash, MINGW, MSYS, Cygwin)
    if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then
        # Usar PowerShell en ventana separada con título personalizado
        cmd.exe /c start "$titulo" powershell.exe -NoExit -Command "\$Host.UI.RawUI.WindowTitle = '$titulo'; Write-Host '=== $titulo ===' -ForegroundColor Cyan; $comando"
        return
    fi

    # Caso 2: macOS
    if [[ "$OSTYPE" == "darwin"* ]]; then
        osascript -e "tell application \"Terminal\" to do script \"printf '\\e]2;$titulo\\a'; echo '=== $titulo ==='; $comando\""
        return
    fi

    # Caso 3: Linux (probar emuladores de terminal comunes)
    if command -v gnome-terminal >/dev/null 2>&1; then
        gnome-terminal --title="$titulo" -- bash -c "echo '=== $titulo ==='; $comando; exec bash"
    elif command -v x-terminal-emulator >/dev/null 2>&1; then
        x-terminal-emulator -T "$titulo" -e bash -c "echo '=== $titulo ==='; $comando; exec bash"
    elif command -v konsole >/dev/null 2>&1; then
        konsole --new-tab -p tabtitle="$titulo" -e bash -c "echo '=== $titulo ==='; $comando; exec bash"
    elif command -v xterm >/dev/null 2>&1; then
        xterm -T "$titulo" -e bash -c "echo '=== $titulo ==='; $comando; exec bash"
    else
        echo "[ERROR] No se detectó un emulador de terminal compatible en Linux."
        echo "Comando a ejecutar para '$titulo':"
        echo "  $comando"
    fi
}

# ------------------------------------------------------------------------------
# 4. Obtener ubicación de una estación según stations.txt
# ------------------------------------------------------------------------------
obtener_ubicacion() {
    local id="$1"
    local fallback="Alicante"
    local file="$DIR/WM_Central/stations.txt"
    if [ -f "$file" ]; then
        local u
        u=$(grep "^$id," "$file" | head -n 1 | cut -d ',' -f2 | tr -d '\r')
        if [ -n "$u" ]; then
            echo "$u"
            return
        fi
    fi
    echo "$fallback"
}

# ------------------------------------------------------------------------------
# 5. Parseo de argumentos flexibles
# ------------------------------------------------------------------------------
if [ $# -eq 0 ] || [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "help" ]; then
    mostrar_ayuda
    exit 0
fi

# Comando especial de limpieza
if [ "$1" == "clean" ] || [ "$1" == "stop" ]; then
    echo "Deteniendo y eliminando contenedores de fo y ws..."
    docker ps -a --filter "name=fo_" --filter "name=ws_" -q | xargs -r docker stop
    docker ps -a --filter "name=fo_" --filter "name=ws_" -q | xargs -r docker rm
    echo "Listo. Contenedores detenidos."
    exit 0
fi

CANTIDAD=""
TIPO=""
EXTRA=""

# Identificar cantidad y tipo sin importar si el número va primero o segundo
for arg in "$@"; do
    if [[ "$arg" =~ ^[0-9]+$ ]] && [ -z "$CANTIDAD" ]; then
        CANTIDAD="$arg"
    elif [ -z "$TIPO" ]; then
        TIPO="$arg"
    else
        if [ -z "$EXTRA" ]; then
            EXTRA="$arg"
        else
            EXTRA="$EXTRA $arg"
        fi
    fi
done

CANTIDAD="${CANTIDAD:-1}"
TIPO=$(echo "$TIPO" | tr '[:upper:]' '[:lower:]')

echo "-----------------------------------------------------------------"
echo "Servidor Central/Kafka: $EC2_PUBLIC_IP"
echo "Lanzando $CANTIDAD instancia(s) de tipo '$TIPO'..."
echo "-----------------------------------------------------------------"

# Carpeta de peticiones (ruta absoluta compatible con Docker)
PETICIONES_DIR="$DIR/peticiones"
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" ]]; then
    # Formato de ruta Windows para volumen de Docker
    PETICIONES_MOUNT="$(cygpath -w "$PETICIONES_DIR" 2>/dev/null || echo "$PETICIONES_DIR")"
else
    PETICIONES_MOUNT="$PETICIONES_DIR"
fi

# ------------------------------------------------------------------------------
# 6. Ejecución según tipo
# ------------------------------------------------------------------------------
case "$TIPO" in
    fo|operario)
        for ((i=1; i<=CANTIDAD; i++)); do
            FO_ID=$(printf "FO-%02d" "$i")
            CONT_NAME=$(printf "fo_%02d" "$i")

            # Si se pasó un fichero específico como argumento extra
            FICHERO_ARG=""
            if [ -n "$EXTRA" ]; then
                FICHERO_ARG=" $EXTRA"
            fi

            CMD="docker run -it --rm --name $CONT_NAME -v \"$PETICIONES_MOUNT:/app/peticiones\" wm_fo $EC2_PUBLIC_IP:$KAFKA_PORT $FO_ID$FICHERO_ARG"
            echo "-> Lanzando $FO_ID en terminal..."
            lanzar_terminal "[$FO_ID] Field Operator" "$CMD"
            sleep 0.2
        done
        ;;

    ws|estacion)
        for ((i=1; i<=CANTIDAD; i++)); do
            WS_ID=$(printf "WS-%02d" "$i")
            PUERTO=$((PUERTO_BASE_WS + i))
            NAME_M=$(printf "ws_m_%02d" "$i")
            NAME_E=$(printf "ws_e_%02d" "$i")

            # Ubicación: usar EXTRA si se pasó para la primera, o leer de stations.txt
            if [ -n "$EXTRA" ] && [ "$CANTIDAD" -eq 1 ]; then
                UBICACION="$EXTRA"
            else
                UBICACION=$(obtener_ubicacion "$WS_ID")
            fi

            CMD_MONITOR="docker run -it --rm --name $NAME_M -p $PUERTO:$PUERTO wm_ws_m $PUERTO $EC2_PUBLIC_IP:$CENTRAL_PORT $WS_ID $UBICACION"
            CMD_ENGINE="docker run -it --rm --name $NAME_E --add-host=host.docker.internal:host-gateway wm_ws_e $EC2_PUBLIC_IP:$KAFKA_PORT host.docker.internal:$PUERTO"

            echo "-> Lanzando $WS_ID ($UBICACION) - Monitor en puerto $PUERTO..."
            lanzar_terminal "[$WS_ID Monitor] :$PUERTO" "$CMD_MONITOR"
            
            # Pequeña pausa para que el Monitor inicialice su socket antes de que el Engine intente conectar
            sleep 1.2

            echo "-> Lanzando $WS_ID - Engine..."
            lanzar_terminal "[$WS_ID Engine]" "$CMD_ENGINE"
            sleep 0.3
        done
        ;;

    engine|e)
        for ((i=1; i<=CANTIDAD; i++)); do
            WS_ID=$(printf "WS-%02d" "$i")
            PUERTO=$((PUERTO_BASE_WS + i))
            NAME_E=$(printf "ws_e_%02d" "$i")

            CMD_ENGINE="docker run -it --rm --name $NAME_E --add-host=host.docker.internal:host-gateway wm_ws_e $EC2_PUBLIC_IP:$KAFKA_PORT host.docker.internal:$PUERTO"
            echo "-> Lanzando Engine para $WS_ID (Monitor :$PUERTO)..."
            lanzar_terminal "[$WS_ID Engine]" "$CMD_ENGINE"
            sleep 0.3
        done
        ;;

    monitor|m)
        for ((i=1; i<=CANTIDAD; i++)); do
            WS_ID=$(printf "WS-%02d" "$i")
            PUERTO=$((PUERTO_BASE_WS + i))
            NAME_M=$(printf "ws_m_%02d" "$i")

            if [ -n "$EXTRA" ] && [ "$CANTIDAD" -eq 1 ]; then
                UBICACION="$EXTRA"
            else
                UBICACION=$(obtener_ubicacion "$WS_ID")
            fi

            CMD_MONITOR="docker run -it --rm --name $NAME_M -p $PUERTO:$PUERTO wm_ws_m $PUERTO $EC2_PUBLIC_IP:$CENTRAL_PORT $WS_ID $UBICACION"
            echo "-> Lanzando Monitor $WS_ID ($UBICACION) en puerto $PUERTO..."
            lanzar_terminal "[$WS_ID Monitor] :$PUERTO" "$CMD_MONITOR"
            sleep 0.3
        done
        ;;

    *)
        echo "[ERROR] Tipo desconocido '$TIPO'."
        mostrar_ayuda
        exit 1
        ;;
esac

echo ""
echo "Todas las terminales han sido lanzadas."
