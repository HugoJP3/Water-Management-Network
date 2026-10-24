#!/usr/bin/env bash
# ==============================================================================
# Script para lanzar instancias en Docker en terminales independientes
# Water Management Network - Sistemas Distribuidos
# ==============================================================================

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" >/dev/null 2>&1 && pwd)"

# Si estamos en Windows (Git Bash, MINGW, MSYS, Cygwin), delegar en lanzar.ps1
# con ExecutionPolicy Bypass para máxima compatibilidad con terminales desacopladas
if [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then
    # Convertir ruta si fuera necesario
    PS1_PATH="$(cygpath -w "$DIR/lanzar.ps1" 2>/dev/null || echo "$DIR/lanzar.ps1")"
    powershell.exe -NoProfile -ExecutionPolicy Bypass -File "$PS1_PATH" "$@"
    exit $?
fi

# ------------------------------------------------------------------------------
# Lógica nativa para Linux / macOS
# ------------------------------------------------------------------------------
cd "$DIR" || exit 1

if [ -f "$DIR/.env" ]; then
    EC2_PUBLIC_IP=$(grep -E '^EC2_PUBLIC_IP=' "$DIR/.env" | cut -d '=' -f2- | tr -d '\r' | tr -d '"' | tr -d "'")
fi

EC2_PUBLIC_IP="${EC2_PUBLIC_IP:-localhost}"
CENTRAL_PORT="5000"
KAFKA_PORT="9092"
PUERTO_BASE_WS=6000

mostrar_ayuda() {
    echo "================================================================="
    echo "  Lanzador Inteligente - Water Management Network"
    echo "================================================================="
    echo "Uso: ./lanzar.sh [CANTIDAD] <TIPO> [EXTRA]"
    echo ""
    echo "Tipos disponibles:"
    echo "  fo | FO          : Field Operator (operario de campo)"
    echo "  ws | WS          : Water Station completa (Monitor + Engine)"
    echo "  engine | e       : Solo Engine(s) de Water Station"
    echo "  monitor | m      : Solo Monitor(es) de Water Station"
    echo "  status           : Muestra instancias actualmente en marcha"
    echo "  clean | stop     : Detiene y elimina los contenedores lanzados"
    echo "================================================================="
}

obtener_activos() {
    docker ps --format "{{.Names}}" 2>/dev/null || true
}

test_puerto() {
    local port="$1"
    nc -z 127.0.0.1 "$port" 2>/dev/null && return 0 || return 1
}

lanzar_terminal() {
    local titulo="$1"
    local comando="$2"

    if [[ "$OSTYPE" == "darwin"* ]]; then
        osascript -e "tell application \"Terminal\" to do script \"printf '\\e]2;$titulo\\a'; echo '=== $titulo ==='; $comando\""
        return
    fi

    if command -v gnome-terminal >/dev/null 2>&1; then
        gnome-terminal --title="$titulo" -- bash -c "echo '=== $titulo ==='; $comando; exec bash" &
    elif command -v x-terminal-emulator >/dev/null 2>&1; then
        x-terminal-emulator -T "$titulo" -e bash -c "echo '=== $titulo ==='; $comando; exec bash" &
    elif command -v konsole >/dev/null 2>&1; then
        konsole --new-tab -p tabtitle="$titulo" -e bash -c "echo '=== $titulo ==='; $comando; exec bash" &
    elif command -v xterm >/dev/null 2>&1; then
        xterm -T "$titulo" -e bash -c "echo '=== $titulo ==='; $comando; exec bash" &
    else
        echo "[ERROR] No se detectó un emulador de terminal compatible."
        echo "Ejecutando en segundo plano:"
        echo "  $comando"
        bash -c "$comando" &
    fi
}

obtener_ubicacion() {
    local id="$1"
    local file="$DIR/WM_Central/stations.txt"
    if [ -f "$file" ]; then
        local u
        u=$(grep "^$id," "$file" | head -n 1 | cut -d ',' -f2 | tr -d '\r')
        if [ -n "$u" ]; then echo "$u"; return; fi
    fi
    echo "Alicante"
}

if [ $# -eq 0 ] || [ "$1" == "-h" ] || [ "$1" == "--help" ] || [ "$1" == "help" ]; then
    mostrar_ayuda
    exit 0
fi

if [ "$1" == "clean" ] || [ "$1" == "stop" ]; then
    echo "Deteniendo contenedores creados..."
    docker ps -a --filter "name=fo_" --filter "name=ws_" -q | xargs -r docker stop
    docker ps -a --filter "name=fo_" --filter "name=ws_" -q | xargs -r docker rm
    echo "Listo."
    exit 0
fi

if [ "$1" == "status" ]; then
    echo "Contenedores de prueba activos:"
    docker ps --filter "name=fo_" --filter "name=ws_" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"
    exit 0
fi

CANTIDAD=""
TIPO=""
TARGET_ID=""
EXTRA=""
TIPOS_VALIDOS="fo operario ws estacion engine e monitor m"

for arg in "$@"; do
    arg_lower=$(echo "$arg" | tr '[:upper:]' '[:lower:]')
    if [[ "$arg" =~ ^[0-9]+$ ]] && [ -z "$CANTIDAD" ]; then
        CANTIDAD="$arg"
    elif [ -z "$TIPO" ] && echo "$TIPOS_VALIDOS" | grep -qw "$arg_lower"; then
        TIPO="$arg_lower"
    elif [ -z "$TARGET_ID" ] && [[ "$arg" =~ ^(FO|WS|fo|ws)-[0-9]+$ ]]; then
        TARGET_ID=$(echo "$arg" | tr '[:lower:]' '[:upper:]')
    else
        if [ -z "$EXTRA" ]; then EXTRA="$arg"; else EXTRA="$EXTRA $arg"; fi
    fi
done

CANTIDAD="${CANTIDAD:-1}"

ACTIVOS=$(obtener_activos)

case "$TIPO" in
    fo|operario)
        if [ -n "$TARGET_ID" ]; then
            num=$(echo "$TARGET_ID" | sed 's/[^0-9]//g' | sed 's/^0*//')
            [ -z "$num" ] && num=1
            asignados=("$num")
            echo "ID especificado: $TARGET_ID"
        else
            asignados=()
            for ((i=1; i<=100; i++)); do
                cname=$(printf "fo_%02d" "$i")
                if ! echo "$ACTIVOS" | grep -q -w "$cname"; then
                    asignados+=("$i")
                    if [ ${#asignados[@]} -ge "$CANTIDAD" ]; then break; fi
                fi
            done
            echo "Lanzando $CANTIDAD FO(s) con IDs libres..."
        fi

        PETICIONES_DIR="$DIR/peticiones"
        REQUESTS_DIR="$DIR/requests"
        REQUESTS_FILE="$DIR/requests/requests.txt"
        [ ! -f "$REQUESTS_FILE" ] && REQUESTS_FILE="$DIR/WM_FO/requests.txt"

        VOLS=""
        [ -d "$PETICIONES_DIR" ] && VOLS="$VOLS -v \"$PETICIONES_DIR:/app/peticiones\""
        [ -d "$REQUESTS_DIR" ] && VOLS="$VOLS -v \"$REQUESTS_DIR:/app/requests\""
        [ -f "$REQUESTS_FILE" ] && VOLS="$VOLS -v \"$REQUESTS_FILE:/app/requests.txt\""

        for num in "${asignados[@]}"; do
            FO_ID=$(printf "FO-%02d" "$num")
            CONT_NAME=$(printf "fo_%02d" "$num")
            FICHERO_ARG=""
            if [ -n "$EXTRA" ]; then FICHERO_ARG=" $EXTRA"; fi

            CMD="docker run -it --rm --name $CONT_NAME $VOLS wm_fo $EC2_PUBLIC_IP:$KAFKA_PORT $FO_ID$FICHERO_ARG"
            echo "-> Lanzando $FO_ID..."
            lanzar_terminal "[$FO_ID] Field Operator" "$CMD"
            sleep 0.2
        done
        ;;

    ws|estacion)
        if [ -n "$TARGET_ID" ]; then
            num=$(echo "$TARGET_ID" | sed 's/[^0-9]//g' | sed 's/^0*//')
            [ -z "$num" ] && num=1
            asignados=("$num")
            echo "Estacion especificada: $TARGET_ID (puerto $((PUERTO_BASE_WS + num)))"
        else
            asignados=()
            for ((i=1; i<=100; i++)); do
                mname=$(printf "ws_m_%02d" "$i")
                ename=$(printf "ws_e_%02d" "$i")
                puerto=$((PUERTO_BASE_WS + i))
                if ! echo "$ACTIVOS" | grep -q -w "$mname" && ! echo "$ACTIVOS" | grep -q -w "$ename"; then
                    if ! test_puerto "$puerto"; then
                        asignados+=("$i")
                        if [ ${#asignados[@]} -ge "$CANTIDAD" ]; then break; fi
                    fi
                fi
            done
            echo "Lanzando $CANTIDAD WS completas (Monitor + Engine)..."
        fi

        for num in "${asignados[@]}"; do
            WS_ID=$(printf "WS-%02d" "$num")
            PUERTO=$((PUERTO_BASE_WS + num))
            NAME_M=$(printf "ws_m_%02d" "$num")
            NAME_E=$(printf "ws_e_%02d" "$num")

            if [ -n "$EXTRA" ]; then
                UBICACION="$EXTRA"
            else
                UBICACION=$(obtener_ubicacion "$WS_ID")
            fi

            CMD_MONITOR="docker run -it --rm --name $NAME_M -p $PUERTO:$PUERTO wm_ws_m $PUERTO $EC2_PUBLIC_IP:$CENTRAL_PORT $WS_ID \"$UBICACION\""
            CMD_ENGINE="docker run -it --rm --name $NAME_E --add-host=host.docker.internal:host-gateway wm_ws_e $EC2_PUBLIC_IP:$KAFKA_PORT host.docker.internal:$PUERTO"

            echo "-> Abriendo Monitor $WS_ID ($UBICACION) :$PUERTO..."
            lanzar_terminal "[$WS_ID Monitor] :$PUERTO" "$CMD_MONITOR"
            sleep 1.2
            echo "-> Abriendo Engine $WS_ID..."
            lanzar_terminal "[$WS_ID Engine]" "$CMD_ENGINE"
            sleep 0.3
        done
        ;;

    engine|e)
        if [ -n "$TARGET_ID" ]; then
            num=$(echo "$TARGET_ID" | sed 's/[^0-9]//g' | sed 's/^0*//')
            [ -z "$num" ] && num=1
            asignados=("$num")
            echo "Engine especificado para: $TARGET_ID"
        else
            asignados=()
            for ((i=1; i<=100; i++)); do
                ename=$(printf "ws_e_%02d" "$i")
                if ! echo "$ACTIVOS" | grep -q -w "$ename"; then
                    asignados+=("$i")
                    if [ ${#asignados[@]} -ge "$CANTIDAD" ]; then break; fi
                fi
            done
        fi

        for num in "${asignados[@]}"; do
            WS_ID=$(printf "WS-%02d" "$num")
            PUERTO=$((PUERTO_BASE_WS + num))
            NAME_E=$(printf "ws_e_%02d" "$num")
            CMD_ENGINE="docker run -it --rm --name $NAME_E --add-host=host.docker.internal:host-gateway wm_ws_e $EC2_PUBLIC_IP:$KAFKA_PORT host.docker.internal:$PUERTO"
            echo "-> Lanzando Engine para $WS_ID (: $PUERTO)..."
            lanzar_terminal "[$WS_ID Engine]" "$CMD_ENGINE"
            sleep 0.3
        done
        ;;

    monitor|m)
        if [ -n "$TARGET_ID" ]; then
            num=$(echo "$TARGET_ID" | sed 's/[^0-9]//g' | sed 's/^0*//')
            [ -z "$num" ] && num=1
            asignados=("$num")
            echo "Monitor especificado para: $TARGET_ID"
        else
            asignados=()
            for ((i=1; i<=100; i++)); do
                mname=$(printf "ws_m_%02d" "$i")
                puerto=$((PUERTO_BASE_WS + i))
                if ! echo "$ACTIVOS" | grep -q -w "$mname" && ! test_puerto "$puerto"; then
                    asignados+=("$i")
                    if [ ${#asignados[@]} -ge "$CANTIDAD" ]; then break; fi
                fi
            done
        fi

        for num in "${asignados[@]}"; do
            WS_ID=$(printf "WS-%02d" "$num")
            PUERTO=$((PUERTO_BASE_WS + num))
            NAME_M=$(printf "ws_m_%02d" "$num")
            UBICACION="${EXTRA:-$(obtener_ubicacion "$WS_ID")}"
            CMD_MONITOR="docker run -it --rm --name $NAME_M -p $PUERTO:$PUERTO wm_ws_m $PUERTO $EC2_PUBLIC_IP:$CENTRAL_PORT $WS_ID \"$UBICACION\""
            echo "-> Lanzando Monitor $WS_ID ($UBICACION) en puerto :$PUERTO..."
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
echo "Instancias solicitadas en ejecución."
