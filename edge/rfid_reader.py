import os
import time
import socket
import random
from dataclasses import dataclass
from datetime import datetime
from typing import List, Optional


# 1. Estrutura de dados da Leitura
@dataclass
class Leitura:
    tag: str
    rssi: Optional[float]
    ts: str

# 2. Classe Base LeitorRFID
class LeitorRFID:
    def ler(self, timeout_s: float = 1.0) -> List[Leitura]:
        raise NotImplementedError

    def fechar(self):
        pass


# 3. Driver Simulado
class LeitorSimulado(LeitorRFID):
    def __init__(self):
        self.tags_validas_conhecidas = [
            "RFID-SMART40-2026-001-A",
            "RFID-SMART40-2026-001-B",
            "RFID-SMART40-2026-001-C"
        ]
        self.tags_validas_desconhecidas = [
            "RFID-SMART40-2025-999-X",
            "RFID-SMART40-2025-999-Z"
        ]
        self.tags_invalidas = [
            "XYZ123",
            "ERR-INVALID-TAG",
            "INVALID_999"
        ]
        self.ultima_troca = time.time()
        self.intervalo_troca = random.uniform(5.0, 10.0)
        self.tag_atual = self._sortear_tag()

    def _sortear_tag(self) -> str:
        r = random.random()
        if r < 0.70:
            return random.choice(self.tags_validas_conhecidas)
        elif r < 0.85:
            return random.choice(self.tags_validas_desconhecidas)
        else:
            return random.choice(self.tags_invalidas)

    def ler(self, timeout_s: float = 1.0) -> List[Leitura]:
        agora = time.time()
        if agora - self.ultima_troca >= self.intervalo_troca:
            self.tag_atual = self._sortear_tag()
            self.ultima_troca = agora
            self.intervalo_troca = random.uniform(5.0, 10.0)

        leituras = []
        # Gera de 5 a 10 leituras por segundo durante o timeout
        num_leituras = max(1, int(random.randint(5, 10) * timeout_s))
        passo = timeout_s / num_leituras if num_leituras > 0 else timeout_s

        for _ in range(num_leituras):
            time.sleep(passo)
            rssi = float(random.randint(-80, -50))
            ts = datetime.now().isoformat()
            leituras.append(Leitura(tag=self.tag_atual, rssi=rssi, ts=ts))

        return leituras


# 4. Driver TCP Conectado
class LeitorTCP(LeitorRFID):
    def __init__(self, host: str, port: int, frame: str = "text"):
        self.host = host
        self.port = int(port)
        self.frame = frame.lower()
        self.sock: Optional[socket.socket] = None
        self.buffer = ""
        self.backoff = 2

    def _conectar(self):
        while True:
            try:
                print(f"[LeitorTCP] Conectando a {self.host}:{self.port}...")
                s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                s.settimeout(3.0)
                s.connect((self.host, self.port))
                self.sock = s
                self.backoff = 2
                print(f"[LeitorTCP] Conectado com sucesso a {self.host}:{self.port}")
                break
            except Exception as e:
                print(f"[LeitorTCP] Erro ao conectar ({e}). Aguardando {self.backoff}s...")
                time.sleep(self.backoff)
                self.backoff = min(self.backoff * 2, 30)

    def ler(self, timeout_s: float = 1.0) -> List[Leitura]:
        if self.sock is None:
            self._conectar()

        leituras = []
        tempo_inicio = time.time()

        while (time.time() - tempo_inicio) < timeout_s:
            try:
                self.sock.settimeout(max(0.1, timeout_s - (time.time() - tempo_inicio)))
                data = self.sock.recv(1024)
                if not data:
                    print("[LeitorTCP] Conexão encerrada pelo servidor. Reconectando...")
                    self.fechar()
                    self._conectar()
                    continue

                ts = datetime.now().isoformat()
                if self.frame == "hex":
                    hex_tag = data.hex().upper()
                    leituras.append(Leitura(tag=hex_tag, rssi=None, ts=ts))
                else:  # Modo texto (\r\n)
                    self.buffer += data.decode("utf-8", errors="ignore")
                    while "\r\n" in self.buffer:
                        tag_str, self.buffer = self.buffer.split("\r\n", 1)
                        tag_str = tag_str.strip()
                        if tag_str:
                            leituras.append(Leitura(tag=tag_str, rssi=None, ts=ts))
            except socket.timeout:
                break
            except Exception as e:
                print(f"[LeitorTCP] Erro na comunicação ({e}). Reconectando...")
                self.fechar()
                self._conectar()
                break

        return leituras

    def fechar(self):
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
        self.buffer = ""


# 5. Função Factory para escolha do driver
def criar_leitor() -> LeitorRFID:
    modo = os.getenv("RFID_MODE", "simulated").lower()
    if modo in ["tcp", "network"]:
        host = os.getenv("RFID_HOST", "127.0.0.1")
        port = int(os.getenv("RFID_PORT", "9000"))
        frame = os.getenv("RFID_FRAME", "text")
        return LeitorTCP(host=host, port=port, frame=frame)
    else:
        return LeitorSimulado()

if __name__ == "__main__":
    leitor = criar_leitor()
    print(f"=== Iniciando Leitor RFID (Modo: {type(leitor).__name__}) ===")
    inicio = time.time()
    try:
        # Executa a leitura por 10 segundos
        while time.time() - inicio < 10.0:
            leituras = leitor.ler(timeout_s=1.0)
            for item in leituras:
                print(f"[{item.ts}] TAG: {item.tag} | RSSI: {item.rssi}")
    except KeyboardInterrupt:
        print("\nLeitura encerrada pelo usuário.")
    finally:
        leitor.fechar()
        print("=== Leitor Encerrado ===")