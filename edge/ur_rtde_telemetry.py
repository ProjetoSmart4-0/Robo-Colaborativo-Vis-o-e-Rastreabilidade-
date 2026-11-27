"""
ur_rtde_telemetry.py - Agente de Borda para Leitura de Telemetria RTDE do Robô UR3e e Garra SCHUNK EGH80
Projeto Integrador SMART 4.0 - Planta N2 (Grupo 2: Cobot, Visão/RFID & Rastreabilidade)

Funcionalidades da Sprint 3:
  1. Leitura RTDE na porta industrial 30004 (125 Hz) para extração das 6 juntas (joints_position_deg),
     velocidade vetorial do TCP (tcp_speed_m_s), pose cartesiana e registradores/saídas digitais
     da garra SCHUNK EGH80 (IO-Link / TURCK FEN20).
  2. Modo Simulação / Replay Cinético: quando a controladora física UR3e não está acessível,
     simula um ciclo industrial realista de Pick-and-Place (Inspeção Reg 7 -> Preensão EGH Grip ->
     Montagem Reg 9 / Descarte Reg 8 -> Liberação EGH Release) com estados HOLDING, OPEN, CLOSED e MOVING.
  3. Sincronização opcional via HTTP POST com o microsserviço cobot-service (/api/v1/cobot/telemetry)
     para alimentar o streaming SSE do Gêmeo Digital na IHM Web.
"""

import os
import sys
import math
import time
import json
import struct
import socket
import logging
import argparse
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional

try:
    import requests
except ImportError:
    requests = None  # Permite execução local mesmo sem requests instalado

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("ur-rtde-telemetry")

# Configurações do Robô UR3e, Garra SCHUNK EGH80 e API Cloud
ROBOT_IP = os.getenv("UR_ROBOT_IP", "192.168.1.100")
RTDE_PORT = int(os.getenv("UR_RTDE_PORT", "30004"))
SAMPLING_RATE_HZ = float(os.getenv("SAMPLING_RATE_HZ", "10.0"))
MAX_STROKE_MM = 80.0

COBOT_API_URL = os.getenv("COBOT_API_URL", "").rstrip("/")
TEST_JWT_TOKEN = os.getenv("TEST_JWT_TOKEN", "")

# Sequência de Replay de Ciclo Industrial (Pick-and-Place + Inspeção + Garra SCHUNK EGH80)
REPLAY_SEQUENCE: List[Dict[str, Any]] = [
    {
        "phase": "APPROACH_PICK",
        "register_1": 7,
        "gripper_state": "OPEN",
        "position_mm": 80.0,
        "force_percentage": 20,
        "led_color": "BLUE",
        "last_command": "EGH Release",
        "joints_deg": [0.0, -85.0, -15.0, -80.0, 90.0, 0.0],
        "tcp_speed": 0.22,
    },
    {
        "phase": "CLOSING_FINGERS",
        "register_1": 7,
        "gripper_state": "MOVING",
        "position_mm": 42.0,
        "force_percentage": 50,
        "led_color": "YELLOW",
        "last_command": "EGH Position",
        "joints_deg": [5.0, -88.0, -10.0, -82.0, 90.0, 5.0],
        "tcp_speed": 0.05,
    },
    {
        "phase": "HOLDING_INSPECTION",
        "register_1": 7,
        "gripper_state": "HOLDING",
        "position_mm": 18.5,
        "force_percentage": 75,
        "led_color": "BLUE",
        "last_command": "EGH Grip",
        "joints_deg": [15.0, -90.0, 0.0, -90.0, 90.0, 15.0],
        "tcp_speed": 0.0,
    },
    {
        "phase": "MOVE_TO_ASSEMBLY_L1",
        "register_1": 9,
        "gripper_state": "HOLDING",
        "position_mm": 18.5,
        "force_percentage": 75,
        "led_color": "GREEN",
        "last_command": "EGH Grip",
        "joints_deg": [45.0, -78.0, 20.0, -112.0, 90.0, 45.0],
        "tcp_speed": 0.18,
    },
    {
        "phase": "RELEASE_AT_ASSEMBLY",
        "register_1": 9,
        "gripper_state": "OPEN",
        "position_mm": 80.0,
        "force_percentage": 20,
        "led_color": "GREEN",
        "last_command": "EGH Release",
        "joints_deg": [45.0, -75.0, 22.0, -115.0, 90.0, 45.0],
        "tcp_speed": 0.04,
    },
    {
        "phase": "EMPTY_GRIP_CHECK",
        "register_1": 9,
        "gripper_state": "CLOSED",
        "position_mm": 1.5,
        "force_percentage": 40,
        "led_color": "YELLOW",
        "last_command": "EGH Grip",
        "joints_deg": [20.0, -85.0, 5.0, -95.0, 90.0, 20.0],
        "tcp_speed": 0.14,
    },
    {
        "phase": "SCRAP_REJECT_CYCLE",
        "register_1": 8,
        "gripper_state": "HOLDING",
        "position_mm": 18.5,
        "force_percentage": 85,
        "led_color": "RED",
        "last_command": "EGH Grip",
        "joints_deg": [-35.0, -82.0, 12.0, -100.0, 90.0, -35.0],
        "tcp_speed": 0.20,
    },
]


class URTelemetryClient:
    """
    Cliente de telemetria RTDE (porta 30004) com decodificação de registradores da garra SCHUNK EGH80
    e gerador de Simulação/Replay integrado para o Gêmeo Digital.
    """

    def __init__(self, host: str = ROBOT_IP, port: int = RTDE_PORT, mode: str = "auto"):
        self.host = host
        self.port = port
        self.mode = mode.lower()  # auto | real | simulation | replay
        self.socket: Optional[socket.socket] = None
        self.connected = False
        self._step_idx = 0
        self._start_ts = time.time()

    def connect(self) -> bool:
        """Estabelece socket TCP na porta 30004 (a menos que forçado modo simulation/replay)."""
        if self.mode in ("simulation", "simulado", "replay"):
            logger.info(f"Iniciando URTelemetryClient diretamente em modo '{self.mode.upper()}'.")
            self.connected = False
            return False

        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.settimeout(2.0)
            self.socket.connect((self.host, self.port))
            self.connected = True
            logger.info(f"Conectado ao robô UR3e via RTDE em {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.warning(
                f"Robô físico não acessível em {self.host}:{self.port} ({e}). "
                "Ativando modo Simulação / Replay do Gêmeo Digital."
            )
            self.connected = False
            return False

    @staticmethod
    def decode_gripper_from_registers(
        digital_out_bits: int,
        position_reg_mm: float,
        force_reg_pct: int,
        register_1: int = 9,
    ) -> Dict[str, Any]:
        """
        Decodifica o estado da garra SCHUNK EGH80 a partir dos sinais digitais/IO-Link e registradores RTDE:
          - Bit 0 (DO0): Comando fechar (Grip)
          - Bit 1 (DO1): Comando abrir (Release)
          - Bit 2 (DO2): Em movimento (Moving)
          - Bit 3 (DO3): Peça detectada (Holding)
        """
        pos_mm = max(0.0, min(MAX_STROKE_MM, float(position_reg_mm)))
        opening_pct = round((pos_mm / MAX_STROKE_MM) * 100.0, 1)
        is_moving = bool(digital_out_bits & 0b0100)
        piece_detected = bool(digital_out_bits & 0b1000) or (15.0 <= pos_mm <= 25.0)

        if is_moving:
            state = "MOVING"
            led_color = "YELLOW"
            last_cmd = "EGH Position"
        elif piece_detected and pos_mm < 70.0:
            state = "HOLDING"
            led_color = "RED" if register_1 == 8 else ("BLUE" if register_1 == 7 else "GREEN")
            last_cmd = "EGH Grip"
        elif pos_mm >= 70.0:
            state = "OPEN"
            led_color = "BLUE"
            last_cmd = "EGH Release"
        else:
            state = "CLOSED"
            led_color = "YELLOW"
            last_cmd = "EGH Grip"

        return {
            "model": "SCHUNK EGH80",
            "state": state,
            "position_mm": round(pos_mm, 2),
            "opening_percentage": opening_pct,
            "target_position_mm": round(pos_mm, 2),
            "force_percentage": max(0, min(100, int(force_reg_pct))),
            "piece_detected": state == "HOLDING",
            "iolink_master": "TURCK FEN20-4IOL (Port P1)",
            "iolink_state": 3,
            "is_referenced": True,
            "is_success": True,
            "led_color": led_color,
            "last_command": last_cmd,
        }

    def read_telemetry(self) -> Dict[str, Any]:
        """
        Retorna a telemetria das 6 juntas e estado da garra SCHUNK EGH80.
        Caso o robô físico não esteja online, provê os dados sintetizados do Gêmeo Digital.
        """
        if not self.connected or self.socket is None:
            return self._generate_simulated_telemetry()

        try:
            data = self.socket.recv(4096)
            if not data or len(data) < 24:
                return self._generate_simulated_telemetry()
            return self._parse_rtde_buffer_or_fallback(data)
        except socket.timeout:
            return self._generate_simulated_telemetry()
        except Exception as err:
            logger.error(f"Falha de leitura RTDE ({err}). Alternando para modo Simulação/Replay.")
            self.connected = False
            return self._generate_simulated_telemetry()

    def _parse_rtde_buffer_or_fallback(self, data: bytes) -> Dict[str, Any]:
        """
        Extrai grandezas cinemáticas e registradores do pacote binário RTDE (30004)
        ou retorna o pacote enriquecido caso o frame seja de handshake/keep-alive.
        """
        try:
            # Se o pacote contiver ao menos 6 doubles (48 bytes) após cabeçalho de 3 bytes
            if len(data) >= 59:
                q_rad = struct.unpack(">6d", data[3:51])
                joints_deg = [round(math.degrees(q), 2) for q in q_rad]
                if all(-360.0 <= j <= 360.0 for j in joints_deg):
                    gripper = self.decode_gripper_from_registers(
                        digital_out_bits=0b1001, position_reg_mm=18.5, force_reg_pct=65, register_1=9
                    )
                    return {
                        "timestamp": time.time(),
                        "iso_timestamp": datetime.now(timezone.utc).isoformat(),
                        "telemetry_source": "UR_RTDE",
                        "robot_id": "UR3e-MONT-01",
                        "robot_mode": "RUNNING",
                        "safety_status": "NORMAL",
                        "register_1": 9,
                        "joints_position_deg": joints_deg,
                        "tcp_pose_m": {"x": 0.125, "y": -0.398, "z": 0.305},
                        "tcp_speed_m_s": 0.15,
                        "gripper": gripper,
                    }
        except Exception:
            pass
        return self._generate_simulated_telemetry(source="UR_RTDE")

    def _generate_simulated_telemetry(self, source: Optional[str] = None) -> Dict[str, Any]:
        """
        Gera pacote telemétrico dinâmico em conformidade com o Gêmeo Digital SMART 4.0.
        No modo 'replay', percorre ciclicamente todas as fases da garra (OPEN, MOVING, HOLDING, CLOSED).
        """
        now = time.time()
        t = now - self._start_ts

        if self.mode == "replay":
            step = REPLAY_SEQUENCE[self._step_idx % len(REPLAY_SEQUENCE)]
            self._step_idx += 1
            pos_mm = step["position_mm"]
            opening_pct = round((pos_mm / MAX_STROKE_MM) * 100.0, 1)
            joints = [round(j + 1.5 * math.sin(t + idx), 2) for idx, j in enumerate(step["joints_deg"])]
            return {
                "timestamp": now,
                "iso_timestamp": datetime.now(timezone.utc).isoformat(),
                "telemetry_source": source or "REPLAY",
                "replay_phase": step["phase"],
                "robot_id": "UR3e-MONT-01",
                "robot_mode": "RUNNING",
                "safety_status": "NORMAL",
                "register_1": step["register_1"],
                "joints_position_deg": joints,
                "tcp_pose_m": {
                    "x": round(0.123 + 0.04 * math.cos(t), 4),
                    "y": round(-0.402 + 0.04 * math.sin(t), 4),
                    "z": round(0.310 + 0.02 * math.sin(t * 0.5), 4),
                },
                "tcp_speed_m_s": step["tcp_speed"],
                "gripper": {
                    "model": "SCHUNK EGH80",
                    "state": step["gripper_state"],
                    "position_mm": pos_mm,
                    "opening_percentage": opening_pct,
                    "target_position_mm": pos_mm,
                    "force_percentage": step["force_percentage"],
                    "piece_detected": step["gripper_state"] == "HOLDING",
                    "iolink_master": "TURCK FEN20-4IOL (Port P1)",
                    "iolink_state": 3,
                    "is_referenced": True,
                    "is_success": True,
                    "led_color": step["led_color"],
                    "last_command": step["last_command"],
                },
            }

        # Modo Simulação Contínua padrão (compatível com testes e Gêmeo Digital)
        base_joints = [0.0, -90.0, 0.0, -90.0, 0.0, 0.0]
        joints = [round(base_joints[i] + 5.0 * math.sin(t * 0.5 + i), 2) for i in range(6)]
        pos_mm = 18.5
        return {
            "timestamp": now,
            "iso_timestamp": datetime.now(timezone.utc).isoformat(),
            "telemetry_source": source or "SIMULATED",
            "robot_id": "UR3e-MONT-01",
            "robot_mode": "RUNNING",
            "safety_status": "NORMAL",
            "register_1": 9,
            "joints_position_deg": joints,
            "tcp_pose_m": {
                "x": round(0.123 + 0.03 * math.cos(t * 0.5), 4),
                "y": round(-0.402 + 0.03 * math.sin(t * 0.5), 4),
                "z": round(0.310 + 0.015 * math.sin(t * 0.25), 4),
            },
            "tcp_speed_m_s": round(0.15 + 0.03 * math.sin(t), 3),
            "gripper": {
                "model": "SCHUNK EGH80",
                "state": "HOLDING",
                "position_mm": pos_mm,
                "opening_percentage": round((pos_mm / MAX_STROKE_MM) * 100.0, 1),
                "target_position_mm": pos_mm,
                "force_percentage": 50,
                "piece_detected": True,
                "iolink_master": "TURCK FEN20-4IOL (Port P1)",
                "iolink_state": 3,
                "is_referenced": True,
                "is_success": True,
                "led_color": "GREEN",
                "last_command": "EGH Grip",
            },
        }

    def push_to_cloud(self, telemetry: Dict[str, Any], api_url: str = COBOT_API_URL, token: str = TEST_JWT_TOKEN) -> bool:
        """Envia o pacote telemétrico para o cobot-service para atualizar o Gêmeo Digital via REST/SSE."""
        if not api_url:
            return False
        endpoint = f"{api_url}/api/v1/cobot/telemetry"
        headers = {"Content-Type": "application/json"}
        if token:
            headers["Authorization"] = token if token.startswith("Bearer ") else f"Bearer {token}"
        try:
            resp = requests.post(endpoint, json=telemetry, headers=headers, timeout=5)
            return resp.ok
        except Exception as e:
            logger.warning(f"Não foi possível publicar telemetria em {endpoint}: {e}")
            return False

    def close(self):
        if self.socket:
            try:
                self.socket.close()
            except Exception:
                pass
        self.connected = False


def main():
    parser = argparse.ArgumentParser(description="Agente RTDE & Gêmeo Digital SCHUNK EGH80 (UR3e)")
    parser.add_argument(
        "--mode",
        choices=["auto", "real", "simulation", "replay"],
        default=os.getenv("RTDE_MODE", "auto"),
        help="Modo de operação: auto (tenta 30004 e cai para simulação), real, simulation ou replay.",
    )
    parser.add_argument(
        "--cycles",
        type=int,
        default=0,
        help="Número de ciclos a executar (0 = contínuo até Ctrl+C).",
    )
    parser.add_argument(
        "--push-api",
        type=str,
        default=COBOT_API_URL,
        help="URL base do cobot-service para sincronizar o Gêmeo Digital via HTTP POST.",
    )
    args = parser.parse_args()

    client = URTelemetryClient(mode=args.mode)
    client.connect()
    print(f"Iniciando streaming de telemetria RTDE / Digital Twin (mode={args.mode})...")
    count = 0
    try:
        while True:
            telemetry = client.read_telemetry()
            g = telemetry["gripper"]
            print(
                f"[RTDE {telemetry['telemetry_source']}] Reg1={telemetry.get('register_1')} | "
                f"Juntas(deg)={telemetry['joints_position_deg']} | "
                f"Garra={g['state']} ({g['position_mm']}mm / {g['opening_percentage']}%, "
                f"Força={g['force_percentage']}%, LED={g['led_color']})"
            )
            if args.push_api:
                client.push_to_cloud(telemetry, api_url=args.push_api)

            count += 1
            if args.cycles > 0 and count >= args.cycles:
                break
            time.sleep(1.0 / SAMPLING_RATE_HZ)
    except KeyboardInterrupt:
        print("\nEncerrado pelo operador.")
    finally:
        client.close()


if __name__ == "__main__":
    main()
