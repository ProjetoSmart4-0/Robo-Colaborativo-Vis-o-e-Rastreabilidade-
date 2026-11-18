import socket
import time

def main():
    srv = socket.socket()
    srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    srv.bind(('127.0.0.1', 9000))
    srv.listen(1)
    print("[Fake RFID Server] Ouvindo na porta 9000...")
    conn, addr = srv.accept()
    print(f"[Fake RFID Server] Conexão estabelecida com {addr}")
    try:
        while True:
            conn.sendall(b'RFID-SMART40-2026-001-A\r\n')
            time.sleep(0.2)
    except Exception as e:
        print(f"[Fake RFID Server] Conexão encerrada: {e}")
    finally:
        conn.close()
        srv.close()

if __name__ == '__main__':
    main()
