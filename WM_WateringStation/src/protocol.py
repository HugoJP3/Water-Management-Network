"""
protocol.py - Protocolo de sockets <STX><DATA><ETX><LRC>

Este mismo fichero lo usan CENTRAL, el Monitor y el Engine (cada módulo
tiene su propia copia en su carpeta src/). Si se cambia algo, hay que cambiarlo
en todas las copias.

Flujo de una conversación (ver Figura del enunciado):

    Cliente                         Servidor
    connect()  ------------------>  accept()
    <ENQ>      ------------------>
               <------------------  <ACK> / <NACK>
    <STX><DATA><ETX><LRC> ------->
               <------------------  <ACK> / <NACK>   (LRC correcto o no)
    ... (los mensajes que haga falta, en cualquier sentido) ...
    <EOT>      ------------------>  close()

DATA es texto con los campos separados por '#':
    "REGISTER#WS-01#Parque_Retiro"
"""
import socket

# --- Caracteres de control (bytes ASCII estándar) ---
STX = b"\x02"   # Start of Text: empieza un mensaje
ETX = b"\x03"   # End of Text:   termina el mensaje
ENQ = b"\x05"   # Enquiry:       "¿estás ahí?" (inicio de conversación)
EOT = b"\x04"   # End of Transmission: fin de conversación
ACK = b"\x06"   # Acknowledge:   "recibido correctamente"
NACK = b"\x15"  # Negative ACK:  "recibido con errores, repítelo"

SEP = "#"        # separador de campos dentro de DATA
ENC = "utf-8"    # codificación del texto


# ---------------------------------------------------------------- LRC
def calc_lrc(data: bytes) -> bytes:
    """Calcula el LRC: XOR byte a byte de DATA + ETX.

    El emisor lo calcula y lo envía al final; el receptor lo recalcula
    con lo que ha recibido. Si no coinciden, el mensaje llegó corrupto
    y se responde NACK.
    """
    lrc = 0
    for b in data + ETX:
        lrc ^= b
    return bytes([lrc])


def build_frame(message: str) -> bytes:
    """Construye la trama completa: STX + DATA + ETX + LRC."""
    data = message.encode(ENC)
    return STX + data + ETX + calc_lrc(data)


# ------------------------------------------------------- lectura de bajo nivel
def _recv_exact(sock: socket.socket, n: int) -> bytes:
    """Lee exactamente n bytes. recv() puede devolver menos de los pedidos,
    por eso hay que repetir hasta completar. Si el otro extremo cierra la
    conexión, recv() devuelve b'' y lanzamos ConnectionError."""
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise ConnectionError("Conexión cerrada por el otro extremo")
        buf += chunk
    return buf


def _read_frame(sock: socket.socket):
    """Lee una trama y devuelve (data, lrc_recibido)."""
    first = _recv_exact(sock, 1)
    if first == EOT:
        # El otro extremo termina la conversación de forma limpia
        raise ConnectionError("Recibido EOT: fin de la conversación")
    if first != STX:
        raise ValueError("Trama inválida: se esperaba STX")
    data = b""
    while True:
        b = _recv_exact(sock, 1)
        if b == ETX:
            break
        data += b
    lrc = _recv_exact(sock, 1)
    return data, lrc


# ------------------------------------------------------- envío y recepción
def send_message(sock: socket.socket, message: str, retries: int = 3) -> bool:
    """Envía un mensaje y espera el ACK del receptor.

    Si recibe NACK reenvía (hasta 'retries' veces).
    Devuelve True si el receptor lo confirmó con ACK.
    """
    frame = build_frame(message)
    for _ in range(retries):
        sock.sendall(frame)
        if _recv_exact(sock, 1) == ACK:
            return True
    return False


def recv_message(sock: socket.socket) -> str:
    """Espera un mensaje, valida su LRC y responde ACK o NACK.

    Si el LRC es incorrecto responde NACK y sigue esperando el reenvío.
    Devuelve el texto de DATA (sin STX/ETX/LRC).
    Lanza ConnectionError si el otro extremo cierra o manda EOT.
    """
    while True:
        data, lrc = _read_frame(sock)
        if lrc == calc_lrc(data):
            sock.sendall(ACK)
            return data.decode(ENC)
        sock.sendall(NACK)


# ------------------------------------------------- inicio y fin de conversación
def client_handshake(sock: socket.socket) -> bool:
    """Lado cliente: envía ENQ y espera ACK. True si el servidor acepta."""
    sock.sendall(ENQ)
    return _recv_exact(sock, 1) == ACK


def server_handshake(sock: socket.socket) -> bool:
    """Lado servidor: espera ENQ y responde ACK (o NACK si no es ENQ)."""
    if _recv_exact(sock, 1) == ENQ:
        sock.sendall(ACK)
        return True
    sock.sendall(NACK)
    return False


def close_connection(sock: socket.socket):
    """Cierra la conversación enviando EOT y cerrando el socket."""
    try:
        sock.sendall(EOT)
    except OSError:
        pass  # si el otro extremo ya no está, da igual
    finally:
        sock.close()


# ----------------------------------------------------- campos separados por '#'
def pack(*fields) -> str:
    """pack('REGISTER', 'WS-01', 'Retiro') -> 'REGISTER#WS-01#Retiro'"""
    return SEP.join(str(f) for f in fields)


def unpack(message: str) -> list:
    """'REGISTER#WS-01#Retiro' -> ['REGISTER', 'WS-01', 'Retiro']"""
    return message.split(SEP)