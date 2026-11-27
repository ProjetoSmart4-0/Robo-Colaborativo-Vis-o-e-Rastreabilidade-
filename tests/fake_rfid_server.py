import socket
import time

srv = socket.socket()
srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
srv.bind(('127.0.0.1', 9000))
srv.listen(1)
print("Servidor RFID falso aguardando conexão na porta 9000...")

conn, _ = srv.accept()
print("Cliente conectado!")

try:
    while True:
        conn.sendall(b'RFID-SMART40-2026-001-A\r\n')
        time.sleep(0.2)
except (BrokenPipeError, ConnectionResetError):
    print("Cliente desconectado.")
finally:
    conn.close()
    srv.close()