import logging
import math
import os
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import requests

# REGRA DE SEGURANÇA: Apenas RTDEReceiveInterface pode ser importada.
# É estritamente proibido importar RTDEControlInterface, RTDEIOInterface ou DashboardClient.
try:
    from rtde_receive import RTDEReceiveInterface
except ImportError:
    RTDEReceiveInterface = None  # Permite rodar em modo simulado mesmo sem ur-rtde instalado

# Configuração de Logs
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("rtde_reader")

# Variáveis de Ambiente
COBOT_API_URL = os.getenv("COBOT_API_URL", "http://127.0.0.1:8000").rstrip("/")
EDGE_TOKEN = os.getenv("EDGE_TOKEN", "dev-token")
UR_ROBOT_IP = os.getenv("UR_ROBOT_IP", "192.168.1.100")
RTDE_MODE = os.getenv("RTDE_MODE", "simulado").strip().lower()
RTDE_INTERVAL_S = float(os.getenv("RTDE_INTERVAL_S", "0.5"))


def gerar_estado_simulado(t: float) -> Tuple[Dict[str, float], List[float]]:
    """
    Gera posições de juntas senoidais (0.3 * sin(t + i)) e uma pose cartesiana
    compatível para testes visuais sem robô físico.
    """
    juntas = [round(0.3 * math.sin(t + i), 4) for i in range(6)]
    pose_cartesiana = {
        "x": round(0.123 + 0.05 * math.cos(t), 4),
        "y": round(-0.402 + 0.05 * math.sin(t), 4),
        "z": round(0.310 + 0.02 * math.sin(t * 0.5), 4),
    }
    return pose_cartesiana, juntas


def conectar_rtde(ip: str) -> Optional[Any]:
    """
    Tenta conectar ao robô UR3e exclusivamente em modo leitura (RTDEReceiveInterface).
    Retorna None caso a biblioteca não esteja instalada ou a conexão falhe.
    """
    if RTDEReceiveInterface is None:
        logger.warning(
            "Biblioteca 'ur-rtde' (rtde_receive) não encontrada. "
            "Alternando para modo simulado (simulado=true)."
        )
        return None

    try:
        logger.info(f"Conectando ao UR3e (somente leitura) em {ip}...")
        rtde = RTDEReceiveInterface(ip)
        logger.info(f"Conectado com sucesso ao UR3e em {ip} (simulado=false).")
        return rtde
    except Exception as e:
        logger.warning(
            f"Não foi possível conectar ao robô em {ip} ({e}). "
            "Alternando para modo simulado (simulado=true)."
        )
        return None


def enviar_estado(payload: dict):
    url = f"{COBOT_API_URL}/api/cobot/robot-state"
    headers = {"Authorization": f"Bearer {EDGE_TOKEN}"}

    try:
        resp = requests.post(url, json=payload, headers=headers, timeout=10)
        if resp.status_code not in (200, 201, 202):
            logger.warning(
                f"API retornou status {resp.status_code} ao enviar robot-state: {resp.text}"
            )
    except requests.RequestException as e:
        logger.warning(f"Falha ao enviar robot-state para {url}: {e}")


def main():
    rtde = None
    modo_simulado = True

    if RTDE_MODE in ("real", "robot", "ur3e"):
        rtde = conectar_rtde(UR_ROBOT_IP)
        if rtde is not None:
            modo_simulado = False
    else:
        logger.info("Iniciando leitor RTDE em modo simulado (RTDE_MODE=simulado).")

    try:
        while True:
            inicio_ciclo = time.time()
            ts_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

            if not modo_simulado and rtde is not None:
                try:
                    juntas_raw = rtde.getActualQ()       # 6 valores em radianos
                    pose_raw = rtde.getActualTCPPose()   # [x, y, z, rx, ry, rz]

                    posicao_cartesiana = {
                        "x": round(float(pose_raw[0]), 4),
                        "y": round(float(pose_raw[1]), 4),
                        "z": round(float(pose_raw[2]), 4),
                    }
                    posicao_juntas = [round(float(q), 4) for q in juntas_raw]
                except Exception as e:
                    logger.warning(
                        f"Perda de comunicação RTDE com o robô ({e}). "
                        "Alternando para modo simulado (simulado=true)."
                    )
                    try:
                        rtde.disconnect()
                    except Exception:
                        pass
                    rtde = None
                    modo_simulado = True
                    posicao_cartesiana, posicao_juntas = gerar_estado_simulado(inicio_ciclo)
            else:
                posicao_cartesiana, posicao_juntas = gerar_estado_simulado(inicio_ciclo)

            payload = {
                "posicaoCartesiana": posicao_cartesiana,
                "posicaoJuntas": posicao_juntas,
                "garraFechada": None,
                "simulado": modo_simulado,
                "timestamp": ts_utc,
            }

            enviar_estado(payload)

            decorrido = time.time() - inicio_ciclo
            espera = max(0.0, RTDE_INTERVAL_S - decorrido)
            time.sleep(espera)

    except KeyboardInterrupt:
        logger.info("Encerrando leitor RTDE...")
    finally:
        if rtde is not None:
            try:
                rtde.disconnect()
            except Exception:
                pass


if __name__ == "__main__":
    main()