"""
fake_monitor.py - Monitor de pega para probar WM_Central sin esperar al
Monitor real. Simula una estación: se registra, manda latidos OK, una fuga,
la resuelve y se desconecta.

Uso:
    python fake_monitor.py <ip_central> <puerto> <ws_id> <ubicacion>
Ejemplo:
    python fake_monitor.py localhost 5000 WS-01 Parque_Retiro
"""
import socket
import sys
import time

import protocol as proto


def main():
    host, port, ws_id, location = sys.argv[1], int(sys.argv[2]), sys.argv[3], sys.argv[4]

    sock = socket.socket()
    sock.connect((host, port))
    assert proto.client_handshake(sock), "Central rechazó el handshake"

    # Registro
    proto.send_message(sock, proto.pack("REGISTER", ws_id, location))
    print("Respuesta de Central:", proto.unpack(proto.recv_message(sock)))

    # Latidos: 3 OK, 3 FUGA, 3 OK (avería resuelta)
    secuencia = ["OK"] * 3 + ["FUGA"] * 3 + ["OK"] * 3
    for estado in secuencia:
        proto.send_message(sock, proto.pack("STATUS", ws_id, estado))
        print("Enviado STATUS", estado)
        time.sleep(1)

    # Cierre limpio. Para probar la desconexión brusca, comenta esta línea
    # y cierra el programa con Ctrl+C: Central la marcará DESCONECTADA.
    proto.close_connection(sock)


if __name__ == "__main__":
    main()