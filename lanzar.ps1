<#
.SYNOPSIS
    Script para lanzar instancias en Docker en terminales independientes de PowerShell.
.DESCRIPTION
    Water Management Network - Sistemas Distribuidos
    Detecta automáticamente qué IDs y puertos están en uso y asigna los siguientes libres.
    Si se cierra una instancia, reutiliza los huecos libres.
    Utiliza -EncodedCommand para garantizar compatibilidad con rutas que tengan espacios y acentos.
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

if ($env:EC2_PUBLIC_IP) {
    $EC2_PUBLIC_IP = $env:EC2_PUBLIC_IP
}

$CENTRAL_PORT = 5000
$KAFKA_PORT = 9092
$PUERTO_BASE_WS = 6000

# ------------------------------------------------------------------------------
# 2. Funciones de ayuda y utilidades de red / docker
# ------------------------------------------------------------------------------
function Mostrar-Ayuda {
    Write-Host "=================================================================" -ForegroundColor Cyan
    Write-Host "  Lanzador Inteligente - Water Management Network" -ForegroundColor Cyan
    Write-Host "=================================================================" -ForegroundColor Cyan
    Write-Host "Uso: .\lanzar.bat [CANTIDAD] <TIPO> [EXTRA]"
    Write-Host "  (O en PowerShell: .\lanzar.ps1 [CANTIDAD] <TIPO> [EXTRA])"
    Write-Host ""
    Write-Host "Tipos disponibles:"
    Write-Host "  fo | FO          : Field Operator (operario de campo)"
    Write-Host "  ws | WS          : Water Station completa (Monitor + Engine en terminales separadas)"
    Write-Host "  engine | e       : Solo Engine(s) de Water Station"
    Write-Host "  monitor | m      : Solo Monitor(es) de Water Station"
    Write-Host "  status | info    : Muestra qué estaciones y operarios están corriendo"
    Write-Host "  clean | stop     : Detiene y elimina todos los contenedores lanzados"
    Write-Host ""
    Write-Host "Comportamiento inteligente de IDs y Puertos:"
    Write-Host "  * Si ya hay 10 WS corriendo y pides 5 WS, parte del puerto 6011 (WS-11 a WS-15)."
    Write-Host "  * Si cierras la WS-03, la próxima WS ocupará el hueco libre (WS-03 / :6003)."
    Write-Host "  * Igual con los FOs: reutiliza huecos cerrados o asigna los siguientes."
    Write-Host ""
    Write-Host "Ejemplos:"
    Write-Host "  .\lanzar.bat 20 fo           -> Lanza 20 FOs en terminales (FO-01 a FO-20)"
    Write-Host "  .\lanzar.bat 5 WS            -> Lanza 5 WS completas (10 terminales: 5 Mon + 5 Eng)"
    Write-Host "  .\lanzar.bat WS Campello     -> Lanza 1 WS completa con ubicación 'Campello'"
    Write-Host "  .\lanzar.bat Engine          -> Lanza 1 Engine (para la primera WS huérfana o libre)"
    Write-Host "  .\lanzar.bat status          -> Ver qué instancias están en marcha"
    Write-Host "  .\lanzar.bat clean           -> Detener todas las instancias de prueba"
    Write-Host ""
    Write-Host "Configuración actual:"
    Write-Host "  IP Servidor (EC2 / Local) : $EC2_PUBLIC_IP"
    Write-Host "  Kafka                     : $EC2_PUBLIC_IP`:$KAFKA_PORT"
    Write-Host "  Central Sockets           : $EC2_PUBLIC_IP`:$CENTRAL_PORT"
    Write-Host "=================================================================" -ForegroundColor Cyan
}

function Obtener-ContenedoresActivos {
    try {
        $raw = docker ps --format "{{.Names}}" 2>$null
        if ($raw) {
            return @($raw | ForEach-Object { $_.Trim() })
        }
    } catch {
        # Docker API inaccesible
    }
    return @()
}

function Test-PuertoOcupado([int]$port) {
    try {
        $tcp = New-Object System.Net.Sockets.TcpClient
        $iar = $tcp.BeginConnect("127.0.0.1", $port, $null, $null)
        $wait = $iar.AsyncWaitHandle.WaitOne(80, $false)
        if ($wait) {
            $tcp.EndConnect($iar)
            $tcp.Close()
            return $true
        }
        $tcp.Close()
    } catch {
        # Puerto libre
    }
    return $false
}

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
# Lanzador de terminal 100% inmune a rutas con espacios o caracteres especiales
# ------------------------------------------------------------------------------
function Abrir-Terminal([string]$titulo, [string[]]$cmdArgs) {
    # Escapar cada argumento individualmente entre comillas simples
    $escapedArgs = $cmdArgs | ForEach-Object {
        "'" + ($_ -replace "'", "''") + "'"
    }
    $cmdStr = "& " + ($escapedArgs -join " ")

    # Construir el bloque que correrá en la nueva ventana
    $childScript = @"
`$Host.UI.RawUI.WindowTitle = '$titulo'
Set-Location '$PSScriptRoot'
Write-Host '=====================================================' -ForegroundColor Cyan
Write-Host '  $titulo' -ForegroundColor Cyan
Write-Host '=====================================================' -ForegroundColor Cyan
$cmdStr
if (`$LASTEXITCODE -ne 0) {
    Write-Host ''
    Write-Host "El proceso termino con codigo de error: `$LASTEXITCODE" -ForegroundColor Red
    Read-Host 'Presiona Enter para cerrar la terminal...'
}
"@

    # Codificar en UTF-16LE Base64 (-EncodedCommand de PowerShell)
    $bytes = [System.Text.Encoding]::Unicode.GetBytes($childScript)
    $encoded = [Convert]::ToBase64String($bytes)

    Start-Process powershell.exe -WorkingDirectory $PSScriptRoot -ArgumentList "-NoExit", "-ExecutionPolicy", "Bypass", "-EncodedCommand", $encoded
}

# ------------------------------------------------------------------------------
# 3. Comandos de control (clean / status)
# ------------------------------------------------------------------------------
if ($Arg1 -in @("clean", "stop")) {
    Write-Host "Deteniendo y eliminando contenedores fo_* y ws_*..." -ForegroundColor Yellow
    $activos = Obtener-ContenedoresActivos
    $objetivos = $activos | Where-Object { $_ -match "^(fo_\d+|ws_m_\d+|ws_e_\d+)$" }
    if ($objetivos) {
        foreach ($c in $objetivos) {
            Write-Host "  -> Deteniendo $c..." -ForegroundColor Gray
            docker stop $c 2>$null | Out-Null
            docker rm $c 2>$null | Out-Null
        }
        Write-Host "Todos los contenedores de prueba han sido detenidos y eliminados." -ForegroundColor Green
    } else {
        Write-Host "No se encontraron contenedores activos de fo o ws." -ForegroundColor Gray
    }
    exit 0
}

if ($Arg1 -in @("status", "info")) {
    $activos = Obtener-ContenedoresActivos
    Write-Host "=== ESTADO DE CONTENEDORES ACTIVOS ===" -ForegroundColor Cyan
    $fos = $activos | Where-Object { $_ -match "^fo_(\d+)$" } | Sort-Object
    $wsm = $activos | Where-Object { $_ -match "^ws_m_(\d+)$" } | Sort-Object
    $wse = $activos | Where-Object { $_ -match "^ws_e_(\d+)$" } | Sort-Object

    Write-Host "Field Operators activos ($($fos.Count)): " -NoNewline -ForegroundColor Yellow
    if ($fos) { Write-Host ($fos -join ", ") -ForegroundColor White } else { Write-Host "Ninguno" -ForegroundColor Gray }

    Write-Host "Water Station Monitores ($($wsm.Count)): " -NoNewline -ForegroundColor Yellow
    if ($wsm) { Write-Host ($wsm -join ", ") -ForegroundColor White } else { Write-Host "Ninguno" -ForegroundColor Gray }

    Write-Host "Water Station Engines   ($($wse.Count)): " -NoNewline -ForegroundColor Yellow
    if ($wse) { Write-Host ($wse -join ", ") -ForegroundColor White } else { Write-Host "Ninguno" -ForegroundColor Gray }
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
$CantidadDetectada = $false

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
# 5. Algoritmos de asignación de IDs y Puertos Libres
# ------------------------------------------------------------------------------
$activos = Obtener-ContenedoresActivos

function Obtener-IdsFoLibres([int]$pedidos, [string[]]$contenedoresActivos) {
    $resultado = @()
    for ($i = 1; $i -le 100; $i++) {
        $contName = "fo_{0:D2}" -f $i
        if ($contName -notin $contenedoresActivos) {
            $resultado += $i
            if ($resultado.Count -ge $pedidos) { break }
        }
    }
    return $resultado
}

function Obtener-IdsWsLibres([int]$pedidos, [string[]]$contenedoresActivos) {
    $resultado = @()
    for ($i = 1; $i -le 100; $i++) {
        $nameM = "ws_m_{0:D2}" -f $i
        $nameE = "ws_e_{0:D2}" -f $i
        $puerto = $PUERTO_BASE_WS + $i

        $contOcupado = ($nameM -in $contenedoresActivos) -or ($nameE -in $contenedoresActivos)
        $puertoOcupado = Test-PuertoOcupado $puerto

        if (-not $contOcupado -and -not $puertoOcupado) {
            $resultado += $i
            if ($resultado.Count -ge $pedidos) { break }
        }
    }
    return $resultado
}

# ------------------------------------------------------------------------------
# 6. Lanzamiento según tipo
# ------------------------------------------------------------------------------
$peticionesDir = Join-Path $PSScriptRoot "peticiones"
$requestsDir   = Join-Path $PSScriptRoot "requests"
$requestsFile  = Join-Path $requestsDir "requests.txt"
if (-not (Test-Path $requestsFile)) {
    $requestsFile = Join-Path $PSScriptRoot "WM_FO\requests.txt"
}

Write-Host "-----------------------------------------------------------------" -ForegroundColor Green
Write-Host "Servidor Central/Kafka: $EC2_PUBLIC_IP"
Write-Host "Solicitado: $Cantidad instancia(s) de '$Tipo'"

switch ($Tipo) {
    { $_ -in @("fo", "operario") } {
        $ids = Obtener-IdsFoLibres $Cantidad $activos
        Write-Host "IDs asignados (libres): $( ($ids | ForEach-Object { 'FO-{0:D2}' -f $_ }) -join ', ' )" -ForegroundColor Cyan
        Write-Host "-----------------------------------------------------------------" -ForegroundColor Green

        foreach ($num in $ids) {
            $foId = "FO-{0:D2}" -f $num
            $contName = "fo_{0:D2}" -f $num

            $argsList = @("docker", "run", "-it", "--rm", "--name", $contName)
            if (Test-Path $peticionesDir) {
                $argsList += @("-v", "${peticionesDir}:/app/peticiones")
            }
            if (Test-Path $requestsDir) {
                $argsList += @("-v", "${requestsDir}:/app/requests")
            }
            if (Test-Path $requestsFile) {
                $argsList += @("-v", "${requestsFile}:/app/requests.txt")
            }
            $argsList += @("wm_fo", "${EC2_PUBLIC_IP}:${KAFKA_PORT}", $foId)
            if ($Extra) {
                $argsList += $Extra
            }

            Write-Host "-> Abriendo terminal para $foId ($contName)..." -ForegroundColor Yellow
            Abrir-Terminal "[$foId] Field Operator" $argsList
            Start-Sleep -Milliseconds 250
        }
    }

    { $_ -in @("ws", "estacion") } {
        $ids = Obtener-IdsWsLibres $Cantidad $activos
        $listaWs = $ids | ForEach-Object { "WS-{0:D2} (puerto {1})" -f $_, ($PUERTO_BASE_WS + $_) }
        Write-Host "Estaciones asignadas (libres): $($listaWs -join ', ')" -ForegroundColor Cyan
        Write-Host "Se abriran $($Cantidad * 2) terminales en total ($Cantidad Monitores + $Cantidad Engines)..." -ForegroundColor Cyan
        Write-Host "-----------------------------------------------------------------" -ForegroundColor Green

        foreach ($num in $ids) {
            $wsId = "WS-{0:D2}" -f $num
            $puerto = $PUERTO_BASE_WS + $num
            $nameM = "ws_m_{0:D2}" -f $num
            $nameE = "ws_e_{0:D2}" -f $num

            $ubicacion = if ($Extra -and $Cantidad -eq 1) { $Extra } else { Obtener-Ubicacion $wsId }

            $cmdArgsM = @(
                "docker", "run", "-it", "--rm",
                "--name", $nameM,
                "-p", "${puerto}:${puerto}",
                "wm_ws_m",
                "$puerto",
                "${EC2_PUBLIC_IP}:${CENTRAL_PORT}",
                $wsId,
                $ubicacion
            )

            $cmdArgsE = @(
                "docker", "run", "-it", "--rm",
                "--name", $nameE,
                "--add-host=host.docker.internal:host-gateway",
                "wm_ws_e",
                "${EC2_PUBLIC_IP}:${KAFKA_PORT}",
                "host.docker.internal:$puerto"
            )

            Write-Host "  -> [1/2] Abriendo Monitor $wsId ($ubicacion) en puerto :$puerto..." -ForegroundColor Yellow
            Abrir-Terminal "[$wsId Monitor] :$puerto" $cmdArgsM

            # Pausa de 1.2 segundos para asegurar que el socket del Monitor levanta antes de que el Engine conecte
            Start-Sleep -Milliseconds 1200

            Write-Host "  -> [2/2] Abriendo Engine $wsId (conectando a Monitor :$puerto)..." -ForegroundColor Yellow
            Abrir-Terminal "[$wsId Engine]" $cmdArgsE
            Start-Sleep -Milliseconds 300
        }
    }

    { $_ -in @("engine", "e") } {
        $huerfanos = @()
        for ($i = 1; $i -le 100; $i++) {
            $nameM = "ws_m_{0:D2}" -f $i
            $nameE = "ws_e_{0:D2}" -f $i
            if (($nameM -in $activos) -and ($nameE -notin $activos)) {
                $huerfanos += $i
            }
        }

        $ids = @()
        if ($huerfanos.Count -gt 0) {
            $ids += $huerfanos | Select-Object -First $Cantidad
        }
        if ($ids.Count -lt $Cantidad) {
            $faltan = $Cantidad - $ids.Count
            for ($i = 1; $i -le 100; $i++) {
                $nameE = "ws_e_{0:D2}" -f $i
                if ($i -notin $ids -and $nameE -notin $activos) {
                    $ids += $i
                    if ($ids.Count -ge $Cantidad) { break }
                }
            }
        }

        Write-Host "Engines asignados: $( ($ids | ForEach-Object { 'WS-{0:D2} (: {1})' -f $_, ($PUERTO_BASE_WS + $_) }) -join ', ' )" -ForegroundColor Cyan
        Write-Host "-----------------------------------------------------------------" -ForegroundColor Green

        foreach ($num in $ids) {
            $wsId = "WS-{0:D2}" -f $num
            $puerto = $PUERTO_BASE_WS + $num
            $nameE = "ws_e_{0:D2}" -f $num

            $cmdArgsE = @(
                "docker", "run", "-it", "--rm",
                "--name", $nameE,
                "--add-host=host.docker.internal:host-gateway",
                "wm_ws_e",
                "${EC2_PUBLIC_IP}:${KAFKA_PORT}",
                "host.docker.internal:$puerto"
            )
            Write-Host "-> Abriendo Engine para $wsId (Monitor :$puerto)..." -ForegroundColor Yellow
            Abrir-Terminal "[$wsId Engine]" $cmdArgsE
            Start-Sleep -Milliseconds 300
        }
    }

    { $_ -in @("monitor", "m") } {
        $ids = @()
        for ($i = 1; $i -le 100; $i++) {
            $nameM = "ws_m_{0:D2}" -f $i
            $puerto = $PUERTO_BASE_WS + $i
            if ($nameM -notin $activos -and -not (Test-PuertoOcupado $puerto)) {
                $ids += $i
                if ($ids.Count -ge $Cantidad) { break }
            }
        }

        Write-Host "Monitores asignados: $( ($ids | ForEach-Object { 'WS-{0:D2} (: {1})' -f $_, ($PUERTO_BASE_WS + $_) }) -join ', ' )" -ForegroundColor Cyan
        Write-Host "-----------------------------------------------------------------" -ForegroundColor Green

        foreach ($num in $ids) {
            $wsId = "WS-{0:D2}" -f $num
            $puerto = $PUERTO_BASE_WS + $num
            $nameM = "ws_m_{0:D2}" -f $num
            $ubicacion = if ($Extra -and $Cantidad -eq 1) { $Extra } else { Obtener-Ubicacion $wsId }

            $cmdArgsM = @(
                "docker", "run", "-it", "--rm",
                "--name", $nameM,
                "-p", "${puerto}:${puerto}",
                "wm_ws_m",
                "$puerto",
                "${EC2_PUBLIC_IP}:${CENTRAL_PORT}",
                $wsId,
                $ubicacion
            )
            Write-Host "-> Abriendo Monitor $wsId ($ubicacion) en puerto :$puerto..." -ForegroundColor Yellow
            Abrir-Terminal "[$wsId Monitor] :$puerto" $cmdArgsM
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
Write-Host "Proceso completado. Todas las terminales solicitadas se estan ejecutando." -ForegroundColor Green
