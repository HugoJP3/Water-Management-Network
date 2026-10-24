<#
.SYNOPSIS
    Script para lanzar instancias en Docker en terminales independientes de PowerShell.
.DESCRIPTION
    Water Management Network - Sistemas Distribuidos
    Lanza automáticamente instancias de FO, Monitor y Engine con IDs y puertos secuenciales.
.EXAMPLE
    .\lanzar.ps1 20 fo
    .\lanzar.ps1 5 WS
    .\lanzar.ps1 WS Campello
    .\lanzar.ps1 Engine
    .\lanzar.ps1 clean
#>

param(
    [Parameter(Position=0, Mandatory=$false)]
    [string]$Arg1,

    [Parameter(Position=1, Mandatory=$false)]
    [string]$Arg2,

    [Parameter(Position=2, Mandatory=$false, ValueFromRemainingArguments=$true)]
    [string[]]$RestArgs
)

$PSScriptRoot = Split-Path -Parent -Path $MyInvocation.MyCommand.Definition
Set-Location $PSScriptRoot

# ------------------------------------------------------------------------------
# 1. Cargar variables de entorno desde .env
# ------------------------------------------------------------------------------
$envFile = Join-Path $PSScriptRoot ".env"
$EC2_PUBLIC_IP = "localhost"

if (Test-Path $envFile) {
    Get-Content $envFile | ForEach-Object {
        $line = $_.Trim()
        if ($line -match "^EC2_PUBLIC_IP=(.*)$") {
            $val = $matches[1].Trim().Trim('"').Trim("'")
            if ($val) { $EC2_PUBLIC_IP = $val }
        }
    }
}

# Variable de entorno de sesión tiene prioridad si existe
if ($env:EC2_PUBLIC_IP) {
    $EC2_PUBLIC_IP = $env:EC2_PUBLIC_IP
}

$CENTRAL_PORT = 5000
$KAFKA_PORT = 9092
$PUERTO_BASE_WS = 6000

# ------------------------------------------------------------------------------
# 2. Función de ayuda
# ------------------------------------------------------------------------------
function Mostrar-Ayuda {
    Write-Host "=================================================================" -ForegroundColor Cyan
    Write-Host "  Lanzador de instancias - Water Management Network (PowerShell)" -ForegroundColor Cyan
    Write-Host "=================================================================" -ForegroundColor Cyan
    Write-Host "Uso: .\lanzar.ps1 [CANTIDAD] <TIPO> [EXTRA]"
    Write-Host ""
    Write-Host "Tipos disponibles:"
    Write-Host "  fo | FO          : Field Operator (operario de campo)"
    Write-Host "  ws | WS          : Water Station completa (Monitor + Engine)"
    Write-Host "  engine | e       : Solo Engine(s) de Water Station"
    Write-Host "  monitor | m      : Solo Monitor(es) de Water Station"
    Write-Host "  clean | stop     : Detiene y elimina los contenedores lanzados"
    Write-Host ""
    Write-Host "Ejemplos:"
    Write-Host "  .\lanzar.ps1 20 fo           -> Lanza 20 FOs en terminales (FO-01 a FO-20)"
    Write-Host "  .\lanzar.ps1 5 WS            -> Lanza 5 WS completas (Monitor + Engine)"
    Write-Host "  .\lanzar.ps1 WS Campello     -> Lanza 1 WS completa con ubicación Campello"
    Write-Host "  .\lanzar.ps1 Engine          -> Lanza 1 Engine (para WS-01)"
    Write-Host "  .\lanzar.ps1 3 Engine        -> Lanza 3 Engines (WS-01 a WS-03)"
    Write-Host "  .\lanzar.ps1 clean           -> Detiene todos los contenedores fo/ws"
    Write-Host ""
    Write-Host "Configuración actual:"
    Write-Host "  IP Servidor (EC2 / Local) : $EC2_PUBLIC_IP"
    Write-Host "  Kafka                     : $EC2_PUBLIC_IP`:$KAFKA_PORT"
    Write-Host "  Central Sockets           : $EC2_PUBLIC_IP`:$CENTRAL_PORT"
    Write-Host "=================================================================" -ForegroundColor Cyan
}

# ------------------------------------------------------------------------------
# 3. Limpieza de contenedores
# ------------------------------------------------------------------------------
if ($Arg1 -in @("clean", "stop")) {
    Write-Host "Deteniendo contenedores creados (fo_* y ws_*)..." -ForegroundColor Yellow
    $ids = docker ps -a --filter "name=fo_" --filter "name=ws_" -q
    if ($ids) {
        $ids | ForEach-Object { docker stop $_; docker rm $_ }
        Write-Host "Contenedores detenidos y eliminados con éxito." -ForegroundColor Green
    } else {
        Write-Host "No se encontraron contenedores activos de fo o ws." -ForegroundColor Gray
    }
    exit 0
}

if (-not $Arg1 -or $Arg1 -in @("-h", "--help", "help", "/?")) {
    Mostrar-Ayuda
    exit 0
}

# ------------------------------------------------------------------------------
# 4. Parseo inteligente de argumentos
# ------------------------------------------------------------------------------
$Cantidad = 1
$Tipo = ""
$Extra = ""

$allArgs = @()
if ($Arg1) { $allArgs += $Arg1 }
if ($Arg2) { $allArgs += $Arg2 }
if ($RestArgs) { $allArgs += $RestArgs }

foreach ($a in $allArgs) {
    if ($a -match "^\d+$" -and -not $CantidadDetectada) {
        $Cantidad = [int]$a
        $CantidadDetectada = $true
    } elseif (-not $Tipo) {
        $Tipo = $a.ToLower()
    } else {
        if (-not $Extra) {
            $Extra = $a
        } else {
            $Extra += " $a"
        }
    }
}

if (-not $Tipo) {
    Mostrar-Ayuda
    exit 1
}

# ------------------------------------------------------------------------------
# 5. Función para obtener ubicación de stations.txt
# ------------------------------------------------------------------------------
function Obtener-Ubicacion([string]$id) {
    $stationsFile = Join-Path $PSScriptRoot "WM_Central\stations.txt"
    if (Test-Path $stationsFile) {
        foreach ($line in Get-Content $stationsFile) {
            $line = $line.Trim()
            if ($line.StartsWith("$id,")) {
                return $line.Split(",")[1].Trim()
            }
        }
    }
    return "Alicante"
}

# ------------------------------------------------------------------------------
# 6. Función para abrir ventana de terminal
# ------------------------------------------------------------------------------
function Abrir-Terminal([string]$titulo, [string]$dockerCmd) {
    $commandBlock = "`$Host.UI.RawUI.WindowTitle = '$titulo'; Write-Host '=== $titulo ===' -ForegroundColor Cyan; $dockerCmd"
    Start-Process powershell.exe -ArgumentList "-NoExit", "-Command", $commandBlock
}

Write-Host "-----------------------------------------------------------------" -ForegroundColor Green
Write-Host "Servidor Central/Kafka: $EC2_PUBLIC_IP"
Write-Host "Lanzando $Cantidad instancia(s) de tipo '$Tipo'..."
Write-Host "-----------------------------------------------------------------" -ForegroundColor Green

$peticionesDir = Join-Path $PSScriptRoot "peticiones"

# ------------------------------------------------------------------------------
# 7. Ejecución
# ------------------------------------------------------------------------------
switch ($Tipo) {
    { $_ -in @("fo", "operario") } {
        for ($i = 1; $i -le $Cantidad; $i++) {
            $foId = "FO-{0:D2}" -f $i
            $contName = "fo_{0:D2}" -f $i
            $extraArg = if ($Extra) { " $Extra" } else { "" }

            $cmd = "docker run -it --rm --name $contName -v `"${peticionesDir}:/app/peticiones`" wm_fo ${EC2_PUBLIC_IP}:${KAFKA_PORT} $foId$extraArg"
            Write-Host "-> Lanzando $foId en nueva terminal..." -ForegroundColor Yellow
            Abrir-Terminal "[$foId] Field Operator" $cmd
            Start-Sleep -Milliseconds 200
        }
    }

    { $_ -in @("ws", "estacion") } {
        for ($i = 1; $i -le $Cantidad; $i++) {
            $wsId = "WS-{0:D2}" -f $i
            $puerto = $PUERTO_BASE_WS + $i
            $nameM = "ws_m_{0:D2}" -f $i
            $nameE = "ws_e_{0:D2}" -f $i

            $ubicacion = if ($Extra -and $Cantidad -eq 1) { $Extra } else { Obtener-Ubicacion $wsId }

            $cmdM = "docker run -it --rm --name $nameM -p ${puerto}:${puerto} wm_ws_m $puerto ${EC2_PUBLIC_IP}:${CENTRAL_PORT} $wsId `"$ubicacion`""
            $cmdE = "docker run -it --rm --name $nameE --add-host=host.docker.internal:host-gateway wm_ws_e ${EC2_PUBLIC_IP}:${KAFKA_PORT} host.docker.internal:$puerto"

            Write-Host "-> Lanzando $wsId ($ubicacion) - Monitor en puerto $puerto..." -ForegroundColor Yellow
            Abrir-Terminal "[$wsId Monitor] :$puerto" $cmdM
            
            # Pausa para que el Monitor levante el socket antes de que el Engine intente conectar
            Start-Sleep -Milliseconds 1200

            Write-Host "-> Lanzando $wsId - Engine..." -ForegroundColor Yellow
            Abrir-Terminal "[$wsId Engine]" $cmdE
            Start-Sleep -Milliseconds 300
        }
    }

    { $_ -in @("engine", "e") } {
        for ($i = 1; $i -le $Cantidad; $i++) {
            $wsId = "WS-{0:D2}" -f $i
            $puerto = $PUERTO_BASE_WS + $i
            $nameE = "ws_e_{0:D2}" -f $i

            $cmdE = "docker run -it --rm --name $nameE --add-host=host.docker.internal:host-gateway wm_ws_e ${EC2_PUBLIC_IP}:${KAFKA_PORT} host.docker.internal:$puerto"
            Write-Host "-> Lanzando Engine para $wsId (Monitor :$puerto)..." -ForegroundColor Yellow
            Abrir-Terminal "[$wsId Engine]" $cmdE
            Start-Sleep -Milliseconds 300
        }
    }

    { $_ -in @("monitor", "m") } {
        for ($i = 1; $i -le $Cantidad; $i++) {
            $wsId = "WS-{0:D2}" -f $i
            $puerto = $PUERTO_BASE_WS + $i
            $nameM = "ws_m_{0:D2}" -f $i

            $ubicacion = if ($Extra -and $Cantidad -eq 1) { $Extra } else { Obtener-Ubicacion $wsId }

            $cmdM = "docker run -it --rm --name $nameM -p ${puerto}:${puerto} wm_ws_m $puerto ${EC2_PUBLIC_IP}:${CENTRAL_PORT} $wsId `"$ubicacion`""
            Write-Host "-> Lanzando Monitor $wsId ($ubicacion) en puerto $puerto..." -ForegroundColor Yellow
            Abrir-Terminal "[$wsId Monitor] :$puerto" $cmdM
            Start-Sleep -Milliseconds 300
        }
    }

    Default {
        Write-Host "[ERROR] Tipo desconocido '$Tipo'." -ForegroundColor Red
        Mostrar-Ayuda
        exit 1
    }
}

Write-Host ""
Write-Host "Todas las terminales han sido lanzadas." -ForegroundColor Green
