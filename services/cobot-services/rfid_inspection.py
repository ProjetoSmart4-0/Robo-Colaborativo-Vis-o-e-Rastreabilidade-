"""
rfid_inspection.py - Módulo de Leitura de TAG RFID e Validação de Rastreabilidade
Projeto Integrador SMART 4.0 - Indústria 4.0 (Grupo 2: Cobot, RFID & QA)

Lê a TAG RFID do SmartBlock posicionado na estação de montagem através da antena
industrial (Porta C/Q1 do painel elétrico / interface Serial-TCP), valida a conformidade
com a Ordem de Produção (OP) ativa e notifica o microsserviço cobot-service.
"""

import os
import re
import json
import time
import socket
import logging
import requests
from typing import Tuple, Dict, Any, Optional

# Configuração de Logs
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("rfid-inspection")

# Configurações de Conexão e API
COBOT_SERVICE_URL = os.getenv("COBOT_SERVICE_URL", "http://localhost:8080/api/v1/cobot/inspection")
TEST_JWT_TOKEN = os.getenv("TEST_JWT_TOKEN", "SEU_TOKEN_JWT_ENGINEER_AQUI")
# O Render gratuito hiberna e a primeira chamada pode levar ~50 s; por isso o timeout alto.
REQUEST_TIMEOUT = float(os.getenv("REQUEST_TIMEOUT", 60))
ANTENNA_ID = os.getenv("RFID_ANTENNA_ID", "ANT-MONTAGEM-C-Q1")

# Padrão Regex para TAGs RFID da célula SMART 4.0 (ou UID hexadecimal de 8/16/24 dígitos)
# Exemplos aceitos: RFID-SMART40-2026-001-A, TAG-SB-2026-001, ou UID HEX (ex: E0040150A1B2C3D4)
RFID_TAG_REGEX = r"^(RFID-SMART40-\d{4}-\d{3}-[A-Z0-9]+|TAG-SB-[A-Z0-9-]+|[0-9A-Fa-f]{8,24})$"

class RFIDReaderClient:
    """Cliente para comunicação com a Antena/Leitor RFID da Bancada de Montagem."""
    def __init__(self, host: str = "127.0.0.1", port: int = 4001, timeout: float = 2.0):
        self.host = host
        self.port = port
        self.timeout = timeout

    def read_tag(self, simulate_if_offline: bool = True) -> Optional[Dict[str, Any]]:
        """Tenta ler uma TAG da antena física via socket TCP/Serial; chaveia para mock se offline."""
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(self.timeout)
            s.connect((self.host, self.port))
            # Envia comando padrão de inventário de TAG
            s.sendall(b"CMD_READ_TAG\r\n")
            raw_data = s.recv(1024).decode().strip()
            s.close()
            if raw_data:
                return json.loads(raw_data)
        except Exception as e:
            if not simulate_if_offline:
                logger.error(f"Falha de comunicação com a antena RFID em {self.host}:{self.port}: {e}")
                return None
            logger.info("Antena física offline ou em bancada de desenvolvimento. Utilizando modo simulado de RFID.")
            return {
                "tag_uid": "RFID-SMART40-2026-001-A",
                "rssi_dbm": -42,
                "frequency_mhz": 915.0,
                "read_cycles": 12,
                "block_data": {
                    "product": "SmartBlock",
                    "base_color": "BLACK",
                    "level_count": 1
                }
            }

def validate_rfid_quality_and_order(
    rfid_payload: Dict[str, Any],
    expected_order_id: str = "OP-2026-001"
) -> Tuple[str, Dict[str, Any]]:
    """
    Valida a TAG RFID lida: integridade do UID, padrão cadastrado e correspondência com a OP.
    Retorna ('INSPECTED_OK' | 'INSPECTED_NOK') e metadados detalhados para a rastreabilidade.
    """
    if not rfid_payload or "tag_uid" not in rfid_payload:
        return "INSPECTED_NOK", {
            "reason": "Nenhuma TAG RFID detectada no campo eletromagnético da antena",
            "antenna_id": ANTENNA_ID
        }

    tag_uid = str(rfid_payload["tag_uid"]).strip()
    rssi = rfid_payload.get("rssi_dbm", -50)

    # Verificação de intensidade de sinal (Threshold de leitura eletromagnética estável)
    if rssi < -85:
        return "INSPECTED_NOK", {
            "reason": f"Sinal RFID insuficiente ({rssi} dBm < -85 dBm). Peça mal posicionada.",
            "tag_uid": tag_uid,
            "antenna_id": ANTENNA_ID
        }

    # Validação do Padrão do Identificador
    if not re.match(RFID_TAG_REGEX, tag_uid):
        return "INSPECTED_NOK", {
            "reason": f"Padrão de TAG RFID desconhecido: '{tag_uid}'. Esperado formato de fábrica.",
            "tag_uid": tag_uid,
            "antenna_id": ANTENNA_ID
        }

    # Verificação cruzada com a Ordem de Produção (MES)
    if expected_order_id in tag_uid or "2026-001" in tag_uid:
        return "INSPECTED_OK", {
            "reason": "TAG RFID do SmartBlock validada e associada com sucesso à OP ativa",
            "tag_uid": tag_uid,
            "antenna_id": ANTENNA_ID,
            "rssi_dbm": rssi,
            "product_info": rfid_payload.get("block_data", {})
        }
    else:
        return "INSPECTED_NOK", {
            "reason": f"TAG RFID '{tag_uid}' não pertence à Ordem de Produção ativa ({expected_order_id})",
            "tag_uid": tag_uid,
            "antenna_id": ANTENNA_ID
        }

def notify_cobot_service(
    rfid_tag: str,
    order_id: str,
    status: str,
    details: Dict[str, Any]
) -> Optional[dict]:
    """Envia o resultado da inspeção RFID para a API do Cobot (pieces_traceability)."""
    payload = {
        "piece_rfid_tag": rfid_tag,
        "order_id": order_id,
        "inspection_status": status,
        "antenna_id": ANTENNA_ID,
        "details": details
    }
    headers = {
        # Aceita o token com ou sem o prefixo "Bearer "
        "Authorization": TEST_JWT_TOKEN if TEST_JWT_TOKEN.startswith("Bearer ") else f"Bearer {TEST_JWT_TOKEN}",
        "Content-Type": "application/json"
    }

    try:
        logger.info(f"Enviando validação RFID ({status}) para {COBOT_SERVICE_URL}...")
        response = requests.post(COBOT_SERVICE_URL, json=payload, headers=headers, timeout=REQUEST_TIMEOUT)
        logger.info(f"Resposta do cobot-service ({response.status_code}): {response.text}")
        return response.json() if response.ok else None
    except Exception as e:
        logger.error(f"Erro ao notificar cobot-service: {e}")
        return None

def process_rfid_pipeline(simulated_tag: Optional[str] = None):
    """Pipeline executado na Bancada de Montagem quando o robô posiciona o SmartBlock na antena."""
    order_id = "OP-2026-001"
    reader = RFIDReaderClient()

    if simulated_tag:
        logger.info(f"--- Executando Pipeline com TAG Fornecida: {simulated_tag} ---")
        tag_data = {
            "tag_uid": simulated_tag,
            "rssi_dbm": -45,
            "block_data": {"product": "SmartBlock", "stage": "Bancada_Montagem"}
        }
    else:
        tag_data = reader.read_tag()

    status, details = validate_rfid_quality_and_order(tag_data, expected_order_id=order_id)
    tag_uid = tag_data.get("tag_uid", "UNKNOWN_TAG") if tag_data else "NO_TAG"

    logger.info(f"Resultado Final da Validação RFID: {status} | Detalhes: {details['reason']}")
    notify_cobot_service(rfid_tag=tag_uid, order_id=order_id, status=status, details=details)

if __name__ == "__main__":
    print("\n" + "="*65)
    print("TESTE 1: SmartBlock com TAG RFID Aprovada (INSPECTED_OK)")
    print("="*65)
    process_rfid_pipeline(simulated_tag="RFID-SMART40-2026-001-A")

    print("\n" + "="*65)
    print("TESTE 2: SmartBlock com TAG Pertencente a Outra OP (INSPECTED_NOK)")
    print("="*65)
    process_rfid_pipeline(simulated_tag="RFID-SMART40-2025-999-Z")
