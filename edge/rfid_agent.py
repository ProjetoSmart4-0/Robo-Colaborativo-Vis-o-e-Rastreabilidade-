import argparse
import json
import logging
import os
import random
import re
import sys
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Optional

import requests

# Garante importação tanto rodando como `python edge/rfid_agent.py` quanto dentro de `edge/`
try:
    from edge.rfid_reader import Leitura, criar_leitor
except ImportError:
    from rfid_reader import Leitura, criar_leitor

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("rfid_agent")

# Configurações via Variáveis de Ambiente
COBOT_API_URL = os.getenv("COBOT_API_URL", "http://127.0.0.1:8000").rstrip("/")
EDGE_TOKEN = os.getenv("EDGE_TOKEN", "dev-token")
BUFFER_PATH = Path(os.getenv("BUFFER_PATH", "edge/buffer_eventos.jsonl"))

RFID_ABSENCE_S = float(os.getenv("RFID_ABSENCE_S", "2.0"))
RFID_MIN_READS = int(os.getenv("RFID_MIN_READS", "3"))
RFID_MIN_RSSI = float(os.getenv("RFID_MIN_RSSI", "-75.0"))

_ordens_env = os.getenv("ORDENS_VALIDAS", "OP-2026-001,OP-2026-002")
ORDENS_VALIDAS = {o.strip() for o in _ordens_env.split(",") if o.strip()}

# Regex de validação da tag
FORMATO = re.compile(r"^RFID-SMART40-(\d{4})-(\d{3})-([A-Z])$")


@dataclass
class Presenca:
    primeira: float
    ultima: float
    leituras: int = 1
    rssis: List[Optional[float]] = field(default_factory=list)
    emitida: bool = False


def ordem_da_tag(tag: str) -> Optional[str]:
    m = FORMATO.match(tag)
    return f"OP-{m.group(1)}-{m.group(2)}" if m else None


def pontuar(tag: str, leituras: int, rssi_medio: Optional[float]) -> int:
    pontos = 0
    if FORMATO.match(tag):
        pontos += 30
    if ordem_da_tag(tag) in ORDENS_VALIDAS:
        pontos += 30
    if leituras >= RFID_MIN_READS:
        pontos += 20
    if rssi_medio is None or rssi_medio >= RFID_MIN_RSSI:
        pontos += 20
    return pontos


class RfidAgent:
    def __init__(self, dry_run: bool = False):
        self.dry_run = dry_run
        self.presencas: Dict[str, Presenca] = {}
        self.backoff = 2

        # Métricas
        self.leituras = 0
        self.eventos_emitidos = 0
        self.duplicatas_descartadas = 0
        self.erros_envio = 0
        self.ultimo_log_metricas = time.time()

        # Garante diretório do buffer
        BUFFER_PATH.parent.mkdir(parents=True, exist_ok=True)

    @property
    def eventos_em_buffer(self) -> int:
        if not BUFFER_PATH.exists():
            return 0
        with BUFFER_PATH.open("r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())

    def _contar_buffer_linhas(self, linhas: List[str]) -> int:
        return sum(1 for l in linhas if l.strip())

    def _salvar_no_buffer(self, evento: dict):
        with BUFFER_PATH.open("a", encoding="utf-8") as f:
            f.write(json.dumps(evento, ensure_ascii=False) + "\n")
        logger.warning(
            f"Evento {evento['correlation_id']} salvo no buffer ({self.eventos_em_buffer} na fila)."
        )

    def _post_evento(self, evento: dict) -> str:
        """
        Envia o evento para a API.
        Retorna:
          - 'OK': sucesso (200/201/202)
          - 'DROP': erro 4xx (não deve ir para o buffer)
          - 'RETRY': falha de rede ou erro 5xx (deve ir/permanecer no buffer)
        """
        if self.dry_run:
            return "OK"

        url = f"{COBOT_API_URL}/api/cobot/inspections"
        headers = {"Authorization": f"Bearer {EDGE_TOKEN}"}

        try:
            resp = requests.post(url, json=evento, headers=headers, timeout=60)
            if resp.status_code in (200, 201, 202):
                self.backoff = 2
                return "OK"
            elif 400 <= resp.status_code < 500:
                self.erros_envio += 1
                logger.error(
                    f"=== ERRO 4XX ({resp.status_code}) AO ENVIAR EVENTO {evento['correlation_id']} === "
                    f"Descartando (não vai para buffer). Resposta: {resp.text}"
                )
                return "DROP"
            else:
                self.erros_envio += 1
                logger.warning(
                    f"Erro servidor {resp.status_code} ao enviar {evento['correlation_id']}."
                )
                return "RETRY"
        except requests.RequestException as e:
            self.erros_envio += 1
            logger.warning(
                f"Falha de rede ao enviar {evento['correlation_id']}: {e}"
            )
            return "RETRY"

    def esvaziar_buffer(self) -> bool:
        """
        Tenta enviar os eventos acumulados no buffer na ordem FIFO.
        Retorna True se o buffer ficou vazio, False se ainda há pendências.
        """
        if self.dry_run or not BUFFER_PATH.exists():
            return True

        with BUFFER_PATH.open("r", encoding="utf-8") as f:
            linhas = [l.strip() for l in f if l.strip()]

        if not linhas:
            return True

        logger.info(f"Tentando reenviar {len(linhas)} evento(s) do buffer...")
        restantes = []
        falhou = False

        for idx, linha in enumerate(linhas):
            if falhou:
                restantes.append(linha)
                continue

            evento = json.loads(linha)
            status = self._post_evento(evento)

            if status == "OK":
                logger.info(
                    f"Evento do buffer entregue: {evento['correlation_id']} (tag={evento.get('tag')})"
                )
            elif status == "DROP":
                # 4xx não volta pro buffer
                continue
            else:
                # Mantém este e todos os próximos na ordem FIFO
                restantes.append(linha)
                falhou = True

        with BUFFER_PATH.open("w", encoding="utf-8") as f:
            for item in restantes:
                f.write(item + "\n")

        if falhou:
            logger.warning(
                f"Buffer ainda possui {len(restantes)} evento(s). Aguardando {self.backoff}s para nova tentativa..."
            )
            time.sleep(self.backoff)
            self.backoff = min(self.backoff * 2, 30)
            return False

        return True

    def montar_evento(
        self,
        tag: str,
        leituras: int,
        rssi_medio: Optional[float],
        fonte_leitura: str,
    ) -> dict:
        score = pontuar(tag, leituras, rssi_medio)
        resultado = "INSPECTED_OK" if score >= 80 else "INSPECTED_NOK"
        ordem = ordem_da_tag(tag)
        ts_utc = datetime.now(timezone.utc).isoformat()

        return {
            "correlation_id": str(uuid.uuid4()),
            "timestamp": ts_utc,
            "tag": tag,
            "ordemProducao": ordem,
            "resultado": resultado,
            "pontuacao": score,
            "leituras": leituras,
            "rssiMedio": round(rssi_medio, 2) if rssi_medio is not None else None,
            "fonteLeitura": fonte_leitura,
        }

    def enviar_evento(self, evento: dict, inicio_presenca: Optional[float] = None):
        # Antes de cada envio novo, esvazia o buffer na ordem em que foi gravado
        buffer_limpo = self.esvaziar_buffer()

        if not buffer_limpo:
            # Se o servidor continua indisponível, enfileira no fim para manter FIFO
            self._salvar_no_buffer(evento)
        else:
            status = self._post_evento(evento)
            if status == "RETRY":
                self._salvar_no_buffer(evento)
                time.sleep(self.backoff)
                self.backoff = min(self.backoff * 2, 30)

        self.eventos_emitidos += 1
        latencia_ms = (
            (time.time() - inicio_presenca) * 1000.0
            if inicio_presenca is not None
            else 0.0
        )
        logger.info(
            f"Evento emitido | tag={evento['tag']} | resultado={evento['resultado']} "
            f"| score={evento['pontuacao']} | latencia={latencia_ms:.1f}ms "
            f"| id={evento['correlation_id']}"
        )

    def emitir_presenca(self, tag: str, p: Presenca, fonte_leitura: str):
        rssis_validos = [r for r in p.rssis if r is not None]
        rssi_medio = (
            sum(rssis_validos) / len(rssis_validos) if rssis_validos else None
        )
        evento = self.montar_evento(
            tag=tag,
            leituras=p.leituras,
            rssi_medio=rssi_medio,
            fonte_leitura=fonte_leitura,
        )
        self.enviar_evento(evento, inicio_presenca=p.primeira)

    def processar_leitura(
        self, leitura: Leitura, fonte_leitura: str, agora: Optional[float] = None
    ):
        self.leituras += 1
        agora = agora if agora is not None else time.time()
        tag = leitura.tag

        p = self.presencas.get(tag)
        if p is None or (agora - p.ultima) > RFID_ABSENCE_S:
            self.presencas[tag] = Presenca(
                primeira=agora,
                ultima=agora,
                leituras=1,
                rssis=[leitura.rssi],
                emitida=False,
            )
            p = self.presencas[tag]
        else:
            p.ultima = agora
            p.leituras += 1
            p.rssis.append(leitura.rssi)

        if not p.emitida and (
            p.leituras >= RFID_MIN_READS or (agora - p.primeira) >= 1.0
        ):
            self.emitir_presenca(tag, p, fonte_leitura)
            p.emitida = True
        elif p.emitida:
            self.duplicatas_descartadas += 1

    def imprimir_metricas(self):
        logger.info(
            f"[MÉTRICAS] leituras={self.leituras} | "
            f"eventos_emitidos={self.eventos_emitidos} | "
            f"duplicatas_descartadas={self.duplicatas_descartadas} | "
            f"eventos_em_buffer={self.eventos_em_buffer} | "
            f"erros_envio={self.erros_envio}"
        )


def executar_teste_dedup():
    agente = RfidAgent(dry_run=True)
    tag = "RFID-SMART40-2026-001-A"
    intervalo = 1.5 / 20.0  # 20 leituras distribuídas em 1,5s

    for _ in range(20):
        leitura = Leitura(
            tag=tag, rssi=-60.0, ts=datetime.now(timezone.utc).isoformat()
        )
        agente.processar_leitura(leitura, fonte_leitura="TEST_DEDUP")
        time.sleep(intervalo)

    if (
        agente.leituras == 20
        and agente.eventos_emitidos == 1
        and agente.duplicatas_descartadas == 19
    ):
        print(
            f"PASS dedup: {agente.leituras} leituras, "
            f"{agente.eventos_emitidos} evento, "
            f"{agente.duplicatas_descartadas} duplicatas descartadas"
        )
    else:
        print(
            f"FAIL dedup: {agente.leituras} leituras, "
            f"{agente.eventos_emitidos} eventos, "
            f"{agente.duplicatas_descartadas} duplicatas descartadas"
        )
        sys.exit(1)


def executar_replay(quantidade: int):
    agente = RfidAgent(dry_run=False)
    ordem_ok = next(iter(ORDENS_VALIDAS), "OP-2026-001")
    _, ano_ok, num_ok = ordem_ok.split("-")
    tag_ok = f"RFID-SMART40-{ano_ok}-{num_ok}-A"
    tags_nok = ["XYZ123", "RFID-SMART40-2025-999-Z", "ERR-INVALID-TAG"]

    logger.info(f"Iniciando replay de {quantidade} eventos sintéticos...")
    try:
        for i in range(quantidade):
            # Metade OK e metade NOK
            if i < (quantidade // 2):
                tag = tag_ok
                leituras = max(RFID_MIN_READS, 5)
                rssi_medio = -60.0
            else:
                tag = random.choice(tags_nok)
                leituras = 1
                rssi_medio = -85.0

            inicio = time.time()
            evento = agente.montar_evento(
                tag=tag,
                leituras=leituras,
                rssi_medio=rssi_medio,
                fonte_leitura="REPLAY",
            )
            agente.enviar_evento(evento, inicio_presenca=inicio)
            time.sleep(1.5)  # Espaçamento para observar quedas de rede e buffer

        # Caso o replay termine com eventos pendentes no buffer, aguarda até esvaziar
        while agente.eventos_em_buffer > 0:
            agente.esvaziar_buffer()

    except KeyboardInterrupt:
        logger.info("Replay interrompido pelo usuário.")
    finally:
        agente.imprimir_metricas()


def executar_normal():
    modo_env = os.getenv("RFID_MODE", "simulated").upper()
    fonte = "TCP" if modo_env in ("TCP", "NETWORK") else "SIMULATED"
    leitor = criar_leitor()
    agente = RfidAgent(dry_run=False)

    logger.info(
        f"Agente RFID iniciado (modo={fonte}, ordens_validas={sorted(ORDENS_VALIDAS)})"
    )
    try:
        while True:
            leituras = leitor.ler(timeout_s=0.5)
            for item in leituras:
                agente.processar_leitura(item, fonte_leitura=fonte)

            # Se não vieram leituras mas há eventos no buffer, aproveita para esvaziar
            if not leituras and agente.eventos_em_buffer > 0:
                agente.esvaziar_buffer()

            if time.time() - agente.ultimo_log_metricas >= 60.0:
                agente.imprimir_metricas()
                agente.ultimo_log_metricas = time.time()
    except KeyboardInterrupt:
        logger.info("Encerrando agente RFID...")
    finally:
        leitor.fechar()
        agente.imprimir_metricas()


def main():
    parser = argparse.ArgumentParser(description="Agente RFID Edge - Cobot UR3e")
    parser.add_argument(
        "--replay",
        type=int,
        metavar="N",
        help="Envia N eventos sintéticos (metade OK, metade NOK) com fonteLeitura REPLAY",
    )
    parser.add_argument(
        "--teste-dedup",
        action="store_true",
        help="Executa o teste de deduplicação (20 leituras em 1,5s -> 1 evento)",
    )
    args = parser.parse_args()

    if args.teste_dedup:
        executar_teste_dedup()
    elif args.replay is not None:
        executar_replay(args.replay)
    else:
        executar_normal()


if __name__ == "__main__":
    main()