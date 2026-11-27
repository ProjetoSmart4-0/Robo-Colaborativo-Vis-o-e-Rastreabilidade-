"""
cobot_service_app.py - Microsserviço do Robô Colaborativo (UR3e) & Gêmeo Digital SCHUNK EGH80
Projeto Integrador SMART 4.0 - Planta N2 (Grupo 2: Cobot, Visão/RFID & Rastreabilidade)

Funcionalidades da Sprint 3:
  1. Gêmeo Digital (Digital Twin) completo da Garra SCHUNK EGH80 e Robô UR3e:
     - Estado operacional: HOLDING, OPEN, CLOSED, MOVING
     - Força de preensão (force_percentage: 0..100%), abertura (position_mm: 0..80mm / opening_percentage: 0..100%)
     - Indicador LED (led_color: GREEN, RED, BLUE, YELLOW) e diagnósticos IO-Link (FEN20)
     - Cinemática das 6 juntas (joints_position_deg), pose TCP e velocidade TCP (tcp_speed_m_s)
     - Modo Simulação/Replay dinâmico quando a bancada física está offline
  2. Comandos Diretos do Gêmeo Digital da Garra:
     - POST /api/v1/cobot/gripper/command (e /api/cobot/gripper/command): EGH Grip, EGH Release, EGH Position
     - Atalhos REST: POST /api/v1/cobot/gripper/grip, /release, /position
  3. Streaming Contínuo SSE (Server-Sent Events):
     - GET /api/v1/cobot/telemetry/stream (e /api/cobot/telemetry/stream): telemetria em tempo real do Gêmeo Digital
     - GET /api/cobot/events/stream (e /api/v1/cobot/events/stream): stream de eventos de inspeção e garra
  4. Rastreabilidade & Integração Cloud:
     - Persistência idempotente em pieces_traceability (Neon Postgres / SQLite)
     - Despacho AMQP (cobot_pick_place_queue) e publicação MQTT (amq.topic)
     - Compatibilidade total com proxy Vercel (/api/cobot/* e /api/v1/cobot/*) e CORS

cobot_service_app.py - Microsserviço do Robô Colaborativo (UR3e) - Grupo 2, SMART 4.0
Versão unificada: base v2 (RFID + banco + SSE) + CORS/RABBITMQ_URL/PORT (Render)
+ publicação de eventos em MQTT (via exchange amq.topic do RabbitMQ/CloudAMQP).

Fluxos de saída de uma inspeção (POST /api/v1/cobot/inspection):
  1. Banco (storage.py)                  -> rastreabilidade idempotente
  2. SSE  (GET /api/cobot/events/stream) -> IHM web em tempo real
  3. MQTT (amq.topic, smart40.n2.cobot.inspection -> tópico smart40/n2/cobot/inspection)
  4. Fila AMQP cobot_pick_place_queue    -> worker aciona o UR3e (rotina 9 ou 8)
Se o broker estiver fora, 1 e 2 continuam funcionando (resposta traz queued=false).
"""

import os
import sys
import math
import uuid
import json
import time
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, Literal, List, Tuple

import pika
import jwt
from fastapi import FastAPI, HTTPException, Depends, Security, Query, status
from fastapi import FastAPI, HTTPException, Depends, Security, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field

sys.path.append(str(Path(__file__).resolve().parent))
import storage  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cobot-service")

# ----------------------------------------------------------------------------
# Configuração
# ----------------------------------------------------------------------------
APP_VERSION = "3.0.0"

JWT_SECRET = os.getenv("JWT_SECRET")
if not JWT_SECRET:
    JWT_SECRET = "dev-only-secret-nao-usar-em-producao"
    logger.warning("JWT_SECRET não definido: usando segredo de desenvolvimento. Defina a variável no Render.")
JWT_ALGORITHM = "HS256"

RABBITMQ_URL = os.getenv("RABBITMQ_URL")  # amqps://user:pass@host/vhost (CloudAMQP) - tem prioridade
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_QUEUE = os.getenv("RABBITMQ_QUEUE", "cobot_pick_place_queue")

# O plugin MQTT do RabbitMQ liga o exchange amq.topic aos tópicos MQTT (ponto <-> barra).
MQTT_EXCHANGE = "amq.topic"
MQTT_ROUTING_KEY_INSPECTION = os.getenv("MQTT_ROUTING_KEY_INSPECTION", "smart40.n2.cobot.inspection")
MQTT_ROUTING_KEY_TELEMETRY = os.getenv("MQTT_ROUTING_KEY_TELEMETRY", "smart40.n2.cobot.status")
MQTT_ROUTING_KEY_GRIPPER = os.getenv("MQTT_ROUTING_KEY_GRIPPER", "smart40.n2.cobot.gripper")

CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]
SSE_REQUIRE_AUTH = os.getenv("SSE_REQUIRE_AUTH", "0") == "1"

# Intervalo padrão do stream SSE de telemetria e timeout de queda para simulação (fallback)
TELEMETRY_STREAM_INTERVAL_S = float(os.getenv("TELEMETRY_STREAM_INTERVAL_S", "0.5"))
PHYSICAL_TELEMETRY_TTL_S = float(os.getenv("PHYSICAL_TELEMETRY_TTL_S", "15.0"))

if os.getenv("RENDER") and storage.BACKEND == "sqlite":
    logger.warning("Rodando no Render com SQLite: os dados são perdidos a cada deploy. Defina DATABASE_URL (Neon).")

app = FastAPI(
    title="SMART 4.0 - Cobot & SCHUNK EGH80 Digital Twin API",
    description=(
        "Microsserviço RESTful e SSE do Gêmeo Digital da Garra SCHUNK EGH80 e Robô UR3e "
        "com rastreabilidade RFID/Visão, persistência Neon Postgres e mensageria RabbitMQ/MQTT."
    ),
    version=APP_VERSION,
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=CORS_ORIGINS,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ----------------------------------------------------------------------------
# Autenticação JWT + RBAC
# ----------------------------------------------------------------------------
bearer = HTTPBearer(auto_error=False)

# O plugin MQTT do RabbitMQ liga o exchange amq.topic aos tópicos MQTT (ponto <-> barra).
MQTT_EXCHANGE = "amq.topic"
MQTT_ROUTING_KEY_INSPECTION = os.getenv("MQTT_ROUTING_KEY_INSPECTION", "smart40.n2.cobot.inspection")

def _decode_token(token: str, allowed_roles: set) -> dict:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.PyJWTError:
        raise HTTPException(
            status.HTTP_401_UNAUTHORIZED,
            "Token JWT inválido ou expirado",
            headers={"WWW-Authenticate": "Bearer"},
        )
    role = payload.get("role")
    if role not in allowed_roles:
        raise HTTPException(
            status.HTTP_403_FORBIDDEN,
            f"Acesso negado: perfil '{role}' não tem permissão para esta operação",
        )
    return payload


def require_roles(*roles: str):
    """ADMIN sempre é aceito. Ex.: require_roles('OPERATOR','ENGINEER') para leitura."""
    allowed = set(roles) | {"ADMIN"}

    def dependency(credentials: Optional[HTTPAuthorizationCredentials] = Security(bearer)):
        if credentials is None:
            raise HTTPException(
                status.HTTP_401_UNAUTHORIZED,
                "Token de autenticação ausente",
                headers={"WWW-Authenticate": "Bearer"},
            )
        return _decode_token(credentials.credentials, allowed)

    return dependency


READ_ROLES = ("OPERATOR", "ENGINEER")
WRITE_ROLES = ("ENGINEER",)

# ----------------------------------------------------------------------------
# Schemas Pydantic (Gêmeo Digital, Comandos da Garra, Rotinas e Inspeção)
# ----------------------------------------------------------------------------
GripperStateType = Literal["HOLDING", "OPEN", "CLOSED", "MOVING"]
GripperLedColorType = Literal["GREEN", "RED", "BLUE", "YELLOW"]
GripperCommandType = Literal[
    "EGH Grip",
    "EGH Release",
    "EGH Position",
    "GRIP",
    "RELEASE",
    "POSITION",
]


class GripperTelemetryModel(BaseModel):
    model: str = Field("SCHUNK EGH80", description="Modelo da garra elétrica inteligente IO-Link")
    state: GripperStateType = Field("HOLDING", description="Estado operacional: HOLDING, OPEN, CLOSED ou MOVING")
    force_percentage: int = Field(50, ge=0, le=100, description="Porcentagem de força de preensão (0% a 100%)")
    position_mm: float = Field(18.5, ge=0.0, le=80.0, description="Posição atual dos dedos/curso em mm (0 a 80 mm)")
    opening_percentage: float = Field(23.1, ge=0.0, le=100.0, description="Porcentagem de abertura da garra (0% a 100%)")
    target_position_mm: float = Field(18.5, ge=0.0, le=80.0, description="Posição alvo comandada em mm")
    led_color: GripperLedColorType = Field("GREEN", description="Cor do indicador visual LED (GREEN, RED, BLUE, YELLOW)")
    piece_detected: bool = Field(True, description="Indica se há peça detectada entre os dedos da garra")
    iolink_master: str = Field("TURCK FEN20-4IOL (Port P1)", description="Mestre IO-Link conectado à garra")
    iolink_state: int = Field(3, description="Código de estado IO-Link URCap (3 = Operacional)")
    is_referenced: bool = Field(True, description="Garra referenciada (IOL_is_referenced)")
    is_success: bool = Field(True, description="Último comando concluído com sucesso (IOL_is_success)")
    last_command: str = Field("EGH Grip", description="Último comando executado pela garra")
    updated_at: str = Field(..., description="Timestamp ISO-8601 da última atualização do Gêmeo Digital")


class GripperCommandRequest(BaseModel):
    command: GripperCommandType = Field(
        ...,
        description="Comando direto da garra: 'EGH Grip' (fechar), 'EGH Release' (abrir) ou 'EGH Position' (posicionar)",
        examples=["EGH Grip"],
    )
    position_mm: Optional[float] = Field(
        None,
        ge=0.0,
        le=80.0,
        description="Abertura desejada em mm (0.0 a 80.0 mm). Usado em 'EGH Position'.",
        examples=[35.0],
    )
    opening_percentage: Optional[float] = Field(
        None,
        ge=0.0,
        le=100.0,
        description="Abertura proporcional desejada em % (0% a 100%). Alternativa a position_mm.",
        examples=[43.75],
    )
    force_percentage: int = Field(
        50,
        ge=0,
        le=100,
        description="Porcentagem de força de preensão (0% a 100%).",
        examples=[75],
    )
    led_color: Optional[GripperLedColorType] = Field(
        None,
        description="Sobrescreve opcionalmente a cor do LED da garra (GREEN, RED, BLUE, YELLOW).",
    )
    piece_rfid_tag: Optional[str] = Field(
        None,
        description="TAG RFID ou QR Code da peça manipulada para gravação de rastreabilidade.",
        examples=["RFID-SMART40-2026-001-A"],
    )
    order_id: Optional[str] = Field(
        "OP-2026-001",
        description="Ordem de Produção associada ao comando da garra.",
        examples=["OP-2026-001"],
    )
    simulate_piece_holding: Optional[bool] = Field(
        True,
        description="Em 'EGH Grip', define se o Gêmeo Digital simula detecção de peça (HOLDING) ou fechamento vazio (CLOSED).",
    )
    correlation_id: Optional[str] = Field(
        None,
        description="Identificador único do comando para rastreabilidade idempotente em pieces_traceability.",
    )


class GripperPositionShortcutRequest(BaseModel):
    position_mm: Optional[float] = Field(None, ge=0.0, le=80.0, examples=[40.0])
    opening_percentage: Optional[float] = Field(None, ge=0.0, le=100.0, examples=[50.0])
    force_percentage: int = Field(50, ge=0, le=100)
    led_color: Optional[GripperLedColorType] = None
    piece_rfid_tag: Optional[str] = None
    order_id: Optional[str] = "OP-2026-001"


class TelemetryIngestRequest(BaseModel):
    """Payload enviado pelo agente de borda RTDE (ur_rtde_telemetry.py / rtde_reader.py) para atualizar o Gêmeo Digital."""
    robot_id: str = "UR3e-MONT-01"
    robot_mode: str = "RUNNING"
    safety_status: str = "NORMAL"
    telemetry_source: Literal["UR_RTDE", "SIMULATED", "REPLAY"] = "UR_RTDE"
    register_1: int = Field(9, description="Valor atual do Register 1 (7=Inspeção, 8=Refugo, 9/10/11=Montagem)")
    joints_position_deg: List[float] = Field(default_factory=lambda: [0.0, -90.0, 0.0, -90.0, 0.0, 0.0])
    tcp_pose_m: Optional[Dict[str, float]] = None
    tcp_speed_m_s: float = 0.15
    gripper: Optional[Dict[str, Any]] = None


class RoutineRequest(BaseModel):
    routine_id: int = Field(
        ...,
        description="ID da rotina no Register 1 do PolyScope (7=Inspeção, 8=Descarte, 9=Montagem L1, 10=L3, 11=L2)",
    )
    routine_name: str = Field(..., examples=["Pega_Lamina_Bloco1"])
    order_id: str = Field(..., examples=["OP-2026-001"])
    piece_rfid_tag: Optional[str] = Field(None, examples=["RFID-SMART40-2026-001-A"])


class InspectionResultRequest(BaseModel):
    piece_rfid_tag: str = Field(..., examples=["RFID-SMART40-2026-001-A"])
    order_id: str = Field(..., examples=["OP-2026-001"])
    inspection_status: Literal["INSPECTED_OK", "INSPECTED_NOK"]
    antenna_id: Optional[str] = Field("ANT-MONTAGEM-01")
    quality_score: Optional[float] = Field(
        None,
        ge=0.0,
        le=100.0,
        description="Score de qualidade/nitidez (0 a 100%). Se omitido, assume 100 para OK e 0 para NOK.",
    )
    correlation_id: Optional[str] = Field(
        None,
        description=(
            "Id único do evento de chegada do pallet. Reenvios com o mesmo id não repetem o movimento do robô. "
            "Se omitido, usa '<tag>-<order_id>'."
        ),
    )
    details: Optional[Dict[str, Any]] = None


# ----------------------------------------------------------------------------
# Motor de Estado do Gêmeo Digital (Digital Twin State Engine)
# ----------------------------------------------------------------------------
MAX_STROKE_MM = 80.0  # Curso máximo nominal da SCHUNK EGH80 (80 mm)

ROUTINE_DESCRIPTIONS = {
    7: "Posição de Foto / Inspeção de Qualidade",
    8: "Rotina 8 de Descarte / Refugo (INSPECTED_NOK)",
    9: "Rotina 9 de Montagem (INSPECTED_OK - Lâmina 1)",
    10: "Rotina 10 de Montagem (Lâmina 3)",
    11: "Rotina 11 de Montagem (Lâmina 2)",
}


class DigitalTwinManager:
    """
    Mantém o espelho em memória do Gêmeo Digital do Robô UR3e e da Garra SCHUNK EGH80.
    Quando a bancada física está offline (ou sem pacotes RTDE recentes), sintetiza cinemática
    suave e transições realistas da garra (MOVING -> HOLDING / OPEN / CLOSED) para a IHM Web.
    """

    def __init__(self):
        self.robot_id: str = "UR3e-MONT-01"
        self.robot_mode: str = "RUNNING"
        self.safety_status: str = "NORMAL"
        self.telemetry_source: str = "SIMULATED"
        self.simulation_enabled: bool = True
        self.register_1: int = 9
        self.active_order_id: str = "OP-2026-001"
        self.active_piece_tag: Optional[str] = "RFID-SMART40-2026-001-A"

        self.joints_position_deg: List[float] = [0.0, -90.0, 0.0, -90.0, 0.0, 0.0]
        self.tcp_pose_m: Dict[str, float] = {"x": 0.123, "y": -0.402, "z": 0.310}
        self.tcp_speed_m_s: float = 0.15

        self.gripper: Dict[str, Any] = {
            "model": "SCHUNK EGH80",
            "state": "HOLDING",
            "force_percentage": 50,
            "position_mm": 18.5,
            "opening_percentage": round((18.5 / MAX_STROKE_MM) * 100.0, 1),
            "target_position_mm": 18.5,
            "led_color": "GREEN",
            "piece_detected": True,
            "iolink_master": "TURCK FEN20-4IOL (Port P1)",
            "iolink_state": 3,
            "is_referenced": True,
            "is_success": True,
            "last_command": "EGH Grip",
            "updated_at": datetime.now(timezone.utc).isoformat(),
        }

        self._last_physical_update: float = 0.0
        self._last_command_ts: float = 0.0
        self._transition_duration_s: float = 0.35
        self._target_final_state: GripperStateType = "HOLDING"
        self._target_final_led: GripperLedColorType = "GREEN"
        self._start_position_mm: float = 18.5
        self._cycle_counter: int = 0

    @staticmethod
    def normalize_command_name(cmd: str) -> Literal["EGH Grip", "EGH Release", "EGH Position"]:
        c = cmd.strip().upper()
        if c in ("EGH GRIP", "GRIP", "CLOSE"):
            return "EGH Grip"
        if c in ("EGH RELEASE", "RELEASE", "OPEN"):
            return "EGH Release"
        return "EGH Position"

    def apply_gripper_command(self, req: GripperCommandRequest) -> Dict[str, Any]:
        """Atualiza o Gêmeo Digital da garra com base em um comando direto (Grip, Release, Position)."""
        canonical_cmd = self.normalize_command_name(req.command)
        now = time.time()
        now_iso = datetime.now(timezone.utc).isoformat()

        self._start_position_mm = float(self.gripper["position_mm"])
        self._last_command_ts = now

        if req.order_id:
            self.active_order_id = req.order_id
        if req.piece_rfid_tag:
            self.active_piece_tag = req.piece_rfid_tag

        if canonical_cmd == "EGH Grip":
            holding = True if req.simulate_piece_holding is None else bool(req.simulate_piece_holding)
            target_mm = req.position_mm if req.position_mm is not None else (18.5 if holding else 2.0)
            final_state: GripperStateType = "HOLDING" if holding else "CLOSED"
            default_led: GripperLedColorType = "GREEN" if holding else "YELLOW"
            piece_detected = holding
            force_pct = req.force_percentage if req.force_percentage is not None else 75

        elif canonical_cmd == "EGH Release":
            target_mm = req.position_mm if req.position_mm is not None else MAX_STROKE_MM
            final_state = "OPEN"
            default_led = "BLUE"
            piece_detected = False
            force_pct = req.force_percentage if req.force_percentage is not None else 20

        else:  # EGH Position
            if req.position_mm is not None:
                target_mm = float(req.position_mm)
            elif req.opening_percentage is not None:
                target_mm = round((float(req.opening_percentage) / 100.0) * MAX_STROKE_MM, 2)
            else:
                target_mm = 40.0

            target_mm = max(0.0, min(MAX_STROKE_MM, target_mm))
            if target_mm >= 76.0:
                final_state = "OPEN"
                default_led = "BLUE"
                piece_detected = False
            elif target_mm <= 4.0:
                final_state = "CLOSED"
                default_led = "YELLOW"
                piece_detected = False
            else:
                final_state = "HOLDING" if bool(req.simulate_piece_holding) and target_mm <= 30.0 else "OPEN"
                default_led = "GREEN" if final_state == "HOLDING" else "YELLOW"
                piece_detected = final_state == "HOLDING"
            force_pct = req.force_percentage if req.force_percentage is not None else 50

        led_color: GripperLedColorType = req.led_color or default_led
        opening_pct = round((target_mm / MAX_STROKE_MM) * 100.0, 1)

        self._target_final_state = final_state
        self._target_final_led = led_color

        # Atualiza estado imediatamente para o estado alvo (e registra metadados de transição)
        self.gripper.update(
            {
                "state": final_state,
                "force_percentage": int(force_pct),
                "position_mm": round(target_mm, 2),
                "opening_percentage": opening_pct,
                "target_position_mm": round(target_mm, 2),
                "led_color": led_color,
                "piece_detected": piece_detected,
                "is_referenced": True,
                "is_success": True,
                "last_command": canonical_cmd,
                "updated_at": now_iso,
            }
        )
        return dict(self.gripper)

    def apply_routine_effect(self, routine_id: int, piece_tag: Optional[str] = None, order_id: Optional[str] = None):
        """Sincroniza o estado do Gêmeo Digital quando uma rotina PolyScope (Register 1) é disparada."""
        self.register_1 = routine_id
        if piece_tag:
            self.active_piece_tag = piece_tag
        if order_id:
            self.active_order_id = order_id

        now_iso = datetime.now(timezone.utc).isoformat()
        if routine_id == 7:
            # Posição de inspeção: garra segurando peça na antena/câmera com LED Azul (em inspeção)
            self.gripper.update(
                {
                    "state": "HOLDING",
                    "position_mm": 18.5,
                    "opening_percentage": round((18.5 / MAX_STROKE_MM) * 100.0, 1),
                    "target_position_mm": 18.5,
                    "force_percentage": 60,
                    "led_color": "BLUE",
                    "piece_detected": True,
                    "last_command": "EGH Grip",
                    "updated_at": now_iso,
                }
            )
        elif routine_id == 8:
            # Rotina de descarte/refugo: LED Vermelho indicando peça NOK
            self.gripper.update(
                {
                    "state": "HOLDING",
                    "position_mm": 18.5,
                    "opening_percentage": round((18.5 / MAX_STROKE_MM) * 100.0, 1),
                    "target_position_mm": 18.5,
                    "force_percentage": 80,
                    "led_color": "RED",
                    "piece_detected": True,
                    "last_command": "EGH Grip",
                    "updated_at": now_iso,
                }
            )
        elif routine_id in (9, 10, 11):
            # Rotina de montagem: LED Verde indicando peça OK
            self.gripper.update(
                {
                    "state": "HOLDING",
                    "position_mm": 18.5,
                    "opening_percentage": round((18.5 / MAX_STROKE_MM) * 100.0, 1),
                    "target_position_mm": 18.5,
                    "force_percentage": 65,
                    "led_color": "GREEN",
                    "piece_detected": True,
                    "last_command": "EGH Grip",
                    "updated_at": now_iso,
                }
            )

    def ingest_edge_telemetry(self, payload: TelemetryIngestRequest) -> Dict[str, Any]:
        """Recebe telemetria vinda do driver de borda (RTDE 30004 ou Replay)."""
        self._last_physical_update = time.time()
        self.robot_id = payload.robot_id
        self.robot_mode = payload.robot_mode
        self.safety_status = payload.safety_status
        self.telemetry_source = payload.telemetry_source
        self.register_1 = payload.register_1
        if payload.joints_position_deg and len(payload.joints_position_deg) == 6:
            self.joints_position_deg = [round(float(x), 3) for x in payload.joints_position_deg]
        if payload.tcp_pose_m:
            self.tcp_pose_m = payload.tcp_pose_m
        self.tcp_speed_m_s = round(float(payload.tcp_speed_m_s), 4)

        if payload.gripper:
            pos_mm = float(payload.gripper.get("position_mm", self.gripper["position_mm"]))
            pos_mm = max(0.0, min(MAX_STROKE_MM, pos_mm))
            opening_pct = payload.gripper.get(
                "opening_percentage", round((pos_mm / MAX_STROKE_MM) * 100.0, 1)
            )
            self.gripper.update(
                {
                    "model": payload.gripper.get("model", "SCHUNK EGH80"),
                    "state": payload.gripper.get("state", self.gripper["state"]),
                    "force_percentage": int(payload.gripper.get("force_percentage", self.gripper["force_percentage"])),
                    "position_mm": round(pos_mm, 2),
                    "opening_percentage": round(float(opening_pct), 1),
                    "target_position_mm": round(float(payload.gripper.get("target_position_mm", pos_mm)), 2),
                    "led_color": payload.gripper.get("led_color", self.gripper["led_color"]),
                    "piece_detected": bool(
                        payload.gripper.get("piece_detected", payload.gripper.get("state") == "HOLDING")
                    ),
                    "iolink_state": int(payload.gripper.get("iolink_state", 3)),
                    "is_referenced": bool(payload.gripper.get("is_referenced", True)),
                    "is_success": bool(payload.gripper.get("is_success", True)),
                    "last_command": payload.gripper.get("last_command", self.gripper["last_command"]),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            )
        return self.snapshot()

    def step_simulation(self):
        """
        Quando a bancada física está desconectada, gera variações cinemáticas suaves
        nas 6 juntas e velocidade TCP para manter o Gêmeo Digital vivo na IHM Web.
        """
        now = time.time()
        if (now - self._last_physical_update) <= PHYSICAL_TELEMETRY_TTL_S:
            return  # Recebendo dados recentes da borda

        self.telemetry_source = "SIMULATED"
        self._cycle_counter += 1
        t = now

        # Cinemática senoidal suave em torno da pose nominal de operação do UR3e
        base_joints = [0.0, -90.0, 0.0, -90.0, 0.0, 0.0]
        amplitudes = [12.0, 8.0, 15.0, 10.0, 6.0, 20.0]
        self.joints_position_deg = [
            round(base_joints[i] + amplitudes[i] * math.sin(t * 0.6 + i * 0.7), 2)
            for i in range(6)
        ]
        self.tcp_pose_m = {
            "x": round(0.123 + 0.04 * math.cos(t * 0.6), 4),
            "y": round(-0.402 + 0.04 * math.sin(t * 0.6), 4),
            "z": round(0.310 + 0.02 * math.sin(t * 0.3), 4),
        }
        self.tcp_speed_m_s = round(0.12 + 0.06 * abs(math.cos(t * 0.8)), 3)
        self.gripper["updated_at"] = datetime.now(timezone.utc).isoformat()

    def snapshot(self, include_history: bool = True, animate_if_simulated: bool = False) -> Dict[str, Any]:
        """Retorna o estado completo do Gêmeo Digital para REST e SSE."""
        if animate_if_simulated:
            self.step_simulation()

        data = {
            "servico": "cobot-service",
            "status": "online",
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "telemetry_source": self.telemetry_source,
            "robot_id": self.robot_id,
            "robot_mode": self.robot_mode,
            "safety_status": self.safety_status,
            "register_1": self.register_1,
            "routine_description": ROUTINE_DESCRIPTIONS.get(self.register_1, "Idle / Aguardando Ordem"),
            "active_order_id": self.active_order_id,
            "active_piece_tag": self.active_piece_tag,
            "joints_position_deg": list(self.joints_position_deg),
            "tcp_pose_m": dict(self.tcp_pose_m),
            "tcp_speed_m_s": self.tcp_speed_m_s,
            "gripper": dict(self.gripper),
            "rfid_station": {"reader_status": "READY", "antenna_port": "C/Q1"},
        }
        if include_history:
            try:
                data["ultimaInspecao"] = storage.listar_inspecoes(limit=1)
            except Exception:
                data["ultimaInspecao"] = []
        return data


digital_twin = DigitalTwinManager()


# ----------------------------------------------------------------------------
# Mensageria (RabbitMQ): uma única conexão por chamada, com timeout curto
# ----------------------------------------------------------------------------
def _connection_params() -> pika.ConnectionParameters:
    if RABBITMQ_URL:
        params = pika.URLParameters(RABBITMQ_URL)
    else:
        params = pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
    params.socket_timeout = 5
    params.connection_attempts = 2
    params.retry_delay = 1
    return params


def amqp_publish(task_msg: Optional[dict] = None, mqtt_events: Optional[List[Tuple[str, dict]]] = None) -> bool:
    """
    Publica, na MESMA conexão:
      - task_msg    -> fila durável cobot_pick_place_queue (consumida pelo worker)
      - mqtt_events -> lista de (routing_key, dict) no exchange amq.topic (vira tópico MQTT)
    Retorna True se tudo foi publicado; False (sem exceção) se o broker estiver indisponível.
    """
    connection = None
    try:
        connection = pika.BlockingConnection(_connection_params())
        channel = connection.channel()
        if task_msg is not None:
            channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
            channel.basic_publish(
                exchange="",
                routing_key=RABBITMQ_QUEUE,
                body=json.dumps(task_msg),
                properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
            )
        for routing_key, body in (mqtt_events or []):
            channel.basic_publish(
                exchange=MQTT_EXCHANGE,
                routing_key=routing_key,
                body=json.dumps(body),
                properties=pika.BasicProperties(delivery_mode=1, content_type="application/json"),
            )
        return True
    except Exception as e:
        destino = "RABBITMQ_URL" if RABBITMQ_URL else f"{RABBITMQ_HOST}:{RABBITMQ_PORT}"
        logger.warning(f"RabbitMQ indisponível ({destino}): {e}")
        return False
    finally:
        if connection is not None and connection.is_open:
            try:
                connection.close()
            except Exception:
                pass


# ----------------------------------------------------------------------------
# SSE (Server-Sent Events) para Inspeções e Telemetria do Gêmeo Digital
# ----------------------------------------------------------------------------
class SSETransmitter:
    """Distribui eventos para os clientes SSE. Seguro para chamadas vindas de threads (endpoints sync)."""

    def __init__(self):
        self.clients: set = set()
        self.loop: Optional[asyncio.AbstractEventLoop] = None

    async def subscribe(self) -> asyncio.Queue:
        self.loop = asyncio.get_running_loop()
        queue: asyncio.Queue = asyncio.Queue(maxsize=100)
        self.clients.add(queue)
        return queue

    def unsubscribe(self, queue: asyncio.Queue):
        self.clients.discard(queue)

    @staticmethod
    def _put(queue: asyncio.Queue, payload: str):
        try:
            queue.put_nowait(payload)
        except asyncio.QueueFull:
            pass  # cliente lento: descarta

    def broadcast_event(self, event_data: dict, event_name: str = "inspection"):
        payload = f"event: {event_name}\ndata: {json.dumps(event_data)}\n\n"
        for queue in list(self.clients):
            if self.loop is not None and self.loop.is_running():
                self.loop.call_soon_threadsafe(self._put, queue, payload)
            else:
                self._put(queue, payload)


sse_transmitter = SSETransmitter()
telemetry_sse_transmitter = SSETransmitter()


# ----------------------------------------------------------------------------
# Endpoints Principais & Saúde
# ----------------------------------------------------------------------------
@app.get("/")
def root():
    return {
        "service": "cobot-service",
        "digital_twin": "UR3e + SCHUNK EGH80",
        "docs": "/docs",
        "health": "/health",
        "telemetry_stream": "/api/v1/cobot/telemetry/stream",
        "events_stream": "/api/cobot/events/stream",
        "version": APP_VERSION,
    }


@app.get("/health")
def health_check():
    db_info = storage.verificar_conexao()
    return {
        "status": "online",
        "service": "cobot-service",
        "identification_mode": "RFID",
        "digital_twin_gripper": digital_twin.gripper["model"],
        "telemetry_source": digital_twin.telemetry_source,
        "version": APP_VERSION,
        "database": storage.BACKEND,
        "database_connected": db_info.get("connected", False),
        "database_records": db_info.get("records_count"),
        "broker_configured": bool(RABBITMQ_URL) or RABBITMQ_HOST != "localhost",
        "nodered_mqtt_topics": {
            "gripper_command_out": MQTT_ROUTING_KEY_GRIPPER.replace(".", "/"),
            "inspection_out": MQTT_ROUTING_KEY_INSPECTION.replace(".", "/"),
            "telemetry_in": "POST /api/v1/cobot/telemetry",
        },
    }


# ----------------------------------------------------------------------------
# Endpoints de Status e Telemetria do Gêmeo Digital
# ----------------------------------------------------------------------------
@app.get("/api/v1/cobot/status")
@app.get("/api/cobot/status")
def get_cobot_status(
    simulate: bool = Query(
        True,
        description="Se true (padrão), anima suavemente as juntas/TCP quando em modo SIMULATED (ideal para polling da Vercel).",
    ),
    user=Depends(require_roles(*READ_ROLES)),
):
    """Retorna o estado completo do Gêmeo Digital do Robô UR3e e da Garra SCHUNK EGH80."""
    return digital_twin.snapshot(include_history=True, animate_if_simulated=simulate)


@app.get("/api/v1/cobot/gripper/status")
@app.get("/api/cobot/gripper/status")
def get_gripper_status(user=Depends(require_roles(*READ_ROLES))):
    """Retorna especificamente o modelo telemétrico do Gêmeo Digital da garra SCHUNK EGH80."""
    return {
        "robot_id": digital_twin.robot_id,
        "telemetry_source": digital_twin.telemetry_source,
        "register_1": digital_twin.register_1,
        "gripper": dict(digital_twin.gripper),
    }


@app.post("/api/v1/cobot/telemetry")
@app.post("/api/cobot/telemetry")
@app.post("/api/cobot/robot-state")
def ingest_telemetry(payload: TelemetryIngestRequest, user=Depends(require_roles(*WRITE_ROLES))):
    """
    Endpoint para o agente de borda (ur_rtde_telemetry.py) atualizar o Gêmeo Digital na nuvem
    e transmitir imediatamente via SSE para a IHM Web (Vercel).
    """
    snap = digital_twin.ingest_edge_telemetry(payload)
    try:
        telemetry_sse_transmitter.broadcast_event(snap, event_name="telemetry")
        sse_transmitter.broadcast_event(snap, event_name="telemetry")
    except Exception as e:
        logger.warning(f"Erro ao transmitir telemetria ingerida via SSE: {e}")
    return {"status": "UPDATED", "telemetry_source": snap["telemetry_source"], "gripper": snap["gripper"]}


# ----------------------------------------------------------------------------
# Endpoints SSE (Server-Sent Events) - Streaming em Tempo Real para a IHM Web
# ----------------------------------------------------------------------------
@app.get("/api/v1/cobot/telemetry/stream")
@app.get("/api/cobot/telemetry/stream")
async def telemetry_stream(
    access_token: Optional[str] = None,
    interval_s: float = Query(
        TELEMETRY_STREAM_INTERVAL_S,
        ge=0.1,
        le=10.0,
        description="Intervalo em segundos entre pacotes de telemetria SSE no modo contínuo.",
    ),
    max_events: Optional[int] = Query(
        None,
        ge=1,
        le=10000,
        description="Limite opcional de eventos emitidos antes de encerrar o stream (útil para testes automatizados e curl).",
    ),
):
    """
    Rota SSE (Server-Sent Events) para streaming contínuo em tempo real do Gêmeo Digital
    da Garra SCHUNK EGH80 e das 6 juntas do UR3e para o frontend do Grupo 4 (Vercel).
    Quando a bancada física está offline, sintetiza eventos dinâmicos automaticamente.
    """
    if access_token:
        _decode_token(access_token, set(READ_ROLES) | set(WRITE_ROLES) | {"ADMIN"})
    elif SSE_REQUIRE_AUTH:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "access_token ausente")

    queue = await telemetry_sse_transmitter.subscribe()

    async def event_generator():
        sent = 0
        try:
            yield "retry: 2000\n: connected-telemetry-stream\n\n"
            # Emite imediatamente o snapshot inicial ao conectar
            initial_snap = digital_twin.snapshot(include_history=False, animate_if_simulated=True)
            yield f"event: telemetry\ndata: {json.dumps(initial_snap)}\n\n"
            sent += 1
            if max_events is not None and sent >= max_events:
                return

            while True:
                try:
                    evt_payload = await asyncio.wait_for(queue.get(), timeout=interval_s)
                    yield evt_payload
                except asyncio.TimeoutError:
                    snap = digital_twin.snapshot(include_history=False, animate_if_simulated=True)
                    yield f"event: telemetry\ndata: {json.dumps(snap)}\n\n"

                sent += 1
                if max_events is not None and sent >= max_events:
                    break
        finally:
            telemetry_sse_transmitter.unsubscribe(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


@app.get("/api/cobot/events/stream")
@app.get("/api/v1/cobot/events/stream")
async def events_stream(access_token: Optional[str] = None):
    """SSE de eventos de inspeção e comandos da garra. Compatível com EventSource do navegador."""
    if access_token:
        _decode_token(access_token, set(READ_ROLES) | set(WRITE_ROLES) | {"ADMIN"})
    elif SSE_REQUIRE_AUTH:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "access_token ausente")

    queue = await sse_transmitter.subscribe()

    async def event_generator():
        try:
            yield "retry: 3000\n: connected\n\n"
            while True:
                try:
                    yield await asyncio.wait_for(queue.get(), timeout=15.0)
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        finally:
            sse_transmitter.unsubscribe(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"},
    )


# ----------------------------------------------------------------------------
# Endpoints de Comandos Diretos do Gêmeo Digital da Garra SCHUNK EGH80
# ----------------------------------------------------------------------------
def _execute_gripper_command(request: GripperCommandRequest, user: dict) -> dict:
    """
    Processa um comando direto da garra SCHUNK EGH80:
      1. Atualiza o estado do Gêmeo Digital (state, position_mm, force_percentage, led_color)
      2. Persiste a ação na tabela pieces_traceability (Neon Postgres / SQLite)
      3. Publica o comando na fila RabbitMQ (cobot_pick_place_queue) e tópico MQTT
      4. Transmite a atualização via SSE para a IHM Web em tempo real
    """
    canonical_cmd = digital_twin.normalize_command_name(request.command)
    updated_gripper = digital_twin.apply_gripper_command(request)

    correlation_id = request.correlation_id or f"GRIPPER-{uuid.uuid4().hex[:12].upper()}"
    piece_tag = request.piece_rfid_tag or digital_twin.active_piece_tag or "SCHUNK-EGH80-DIRECT"
    order_id = request.order_id or digital_twin.active_order_id or "OP-2026-001"
    ts_iso = datetime.now(timezone.utc).isoformat()

    # 1. Persistência na tabela pieces_traceability
    trace_event = {
        "correlation_id": correlation_id,
        "qrCode": piece_tag,
        "resultado": f"GRIPPER_{updated_gripper['state']}",
        "pontuacaoQualidade": float(updated_gripper["force_percentage"]),
        "fonteLeitura": "SCHUNK_EGH80_TWIN",
        "order_id": order_id,
        "gripper_action": f"{canonical_cmd} ({updated_gripper['position_mm']}mm, {updated_gripper['force_percentage']}%)",
        "ts": ts_iso,
        "acao": canonical_cmd,
    }
    try:
        storage.salvar_inspecao(trace_event)
    except Exception as e:
        logger.warning(f"Não foi possível persistir ação da garra no banco: {e}")

    # 2. Enfileiramento RabbitMQ para o worker de borda + evento MQTT
    amqp_task = {
        "action": "GRIPPER_COMMAND",
        "gripper_command": canonical_cmd,
        "position_mm": updated_gripper["position_mm"],
        "opening_percentage": updated_gripper["opening_percentage"],
        "force_percentage": updated_gripper["force_percentage"],
        "led_color": updated_gripper["led_color"],
        "piece_rfid_tag": piece_tag,
        "order_id": order_id,
        "correlation_id": correlation_id,
        "triggered_by": user.get("sub", "engineer_user"),
        "timestamp": ts_iso,
    }
    queued = amqp_publish(
        task_msg=amqp_task,
        mqtt_events=[(MQTT_ROUTING_KEY_GRIPPER, amqp_task)],
    )

    # 3. Notificação imediata via SSE (tanto no stream de telemetria quanto no de eventos)
    snapshot = digital_twin.snapshot(include_history=False, animate_if_simulated=False)
    try:
        telemetry_sse_transmitter.broadcast_event(snapshot, event_name="telemetry")
        sse_transmitter.broadcast_event(
            {
                "event": "GRIPPER_COMMAND",
                "correlation_id": correlation_id,
                "command": canonical_cmd,
                "gripper": updated_gripper,
                "piece_rfid_tag": piece_tag,
                "order_id": order_id,
                "ts": ts_iso,
            },
            event_name="gripper",
        )
    except Exception as e:
        logger.warning(f"Falha ao transmitir comando da garra via SSE: {e}")

    return {
        "message": f"Comando '{canonical_cmd}' aplicado ao Gêmeo Digital da garra SCHUNK EGH80",
        "command": canonical_cmd,
        "correlation_id": correlation_id,
        "piece_rfid_tag": piece_tag,
        "order_id": order_id,
        "queued": queued,
        "telemetry_source": digital_twin.telemetry_source,
        "gripper": updated_gripper,
    }


@app.post("/api/v1/cobot/gripper/command")
@app.post("/api/cobot/gripper/command")
def command_gripper(request: GripperCommandRequest, user=Depends(require_roles(*WRITE_ROLES))):
    """
    Endpoint principal de despacho de comandos do Gêmeo Digital da Garra SCHUNK EGH80:
    suporta 'EGH Grip' (fechar), 'EGH Release' (abrir) e 'EGH Position' (ajuste proporcional).
    """
    return _execute_gripper_command(request, user)


@app.post("/api/v1/cobot/gripper/grip")
@app.post("/api/cobot/gripper/grip")
def command_gripper_grip(
    force_percentage: int = Query(75, ge=0, le=100),
    position_mm: float = Query(18.5, ge=0.0, le=80.0),
    simulate_piece_holding: bool = Query(True),
    piece_rfid_tag: Optional[str] = Query(None),
    order_id: str = Query("OP-2026-001"),
    user=Depends(require_roles(*WRITE_ROLES)),
):
    """Atalho REST para fechar a garra ('EGH Grip') com força parametrizada."""
    req = GripperCommandRequest(
        command="EGH Grip",
        position_mm=position_mm,
        force_percentage=force_percentage,
        simulate_piece_holding=simulate_piece_holding,
        piece_rfid_tag=piece_rfid_tag,
        order_id=order_id,
    )
    return _execute_gripper_command(req, user)


@app.post("/api/v1/cobot/gripper/release")
@app.post("/api/cobot/gripper/release")
def command_gripper_release(
    piece_rfid_tag: Optional[str] = Query(None),
    order_id: str = Query("OP-2026-001"),
    user=Depends(require_roles(*WRITE_ROLES)),
):
    """Atalho REST para abrir totalmente a garra ('EGH Release' -> 80 mm)."""
    req = GripperCommandRequest(
        command="EGH Release",
        position_mm=MAX_STROKE_MM,
        force_percentage=20,
        simulate_piece_holding=False,
        piece_rfid_tag=piece_rfid_tag,
        order_id=order_id,
    )
    return _execute_gripper_command(req, user)


@app.post("/api/v1/cobot/gripper/position")
@app.post("/api/cobot/gripper/position")
def command_gripper_position(
    body: GripperPositionShortcutRequest,
    user=Depends(require_roles(*WRITE_ROLES)),
):
    """Atalho REST para ajuste proporcional de abertura ('EGH Position') em mm ou %."""
    req = GripperCommandRequest(
        command="EGH Position",
        position_mm=body.position_mm,
        opening_percentage=body.opening_percentage,
        force_percentage=body.force_percentage,
        led_color=body.led_color,
        piece_rfid_tag=body.piece_rfid_tag,
        order_id=body.order_id,
    )
    return _execute_gripper_command(req, user)


# ----------------------------------------------------------------------------
# Endpoints de Histórico, Rotinas Robóticas e Inspeção de Qualidade
# ----------------------------------------------------------------------------
@app.get("/api/v1/cobot/inspections")
@app.get("/api/cobot/inspections")
def get_inspections_history(limit: int = 10, status_filter: Optional[str] = None):
    return storage.listar_inspecoes(limit=limit, status_filter=status_filter)


@app.post("/api/v1/cobot/routines", status_code=202)
@app.post("/api/cobot/routines", status_code=202)
def trigger_cobot_routine(request: RoutineRequest, user=Depends(require_roles(*WRITE_ROLES))):
    """
    Enfileira uma rotina robótica (Register 1 = 7, 8, 9, 10 ou 11) associada à garra.
    Atualiza o estado do Gêmeo Digital e responde 503 se o broker RabbitMQ estiver offline.
    """
    digital_twin.apply_routine_effect(
        routine_id=request.routine_id,
        piece_tag=request.piece_rfid_tag,
        order_id=request.order_id,
    )

    payload = {
        "action": "EXECUTE_ROUTINE",
        "routine_id": request.routine_id,
        "register_1": request.routine_id,
        "routine_name": request.routine_name,
        "order_id": request.order_id,
        "piece_rfid_tag": request.piece_rfid_tag,
        "gripper_state": digital_twin.gripper["state"],
        "triggered_by": user.get("sub", "engineer_user"),
    }
    if not amqp_publish(task_msg=payload):
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Barramento RabbitMQ indisponível")

    # Transmite atualização do Gêmeo Digital via SSE
    try:
        telemetry_sse_transmitter.broadcast_event(
            digital_twin.snapshot(include_history=False), event_name="telemetry"
        )
    except Exception:
        pass

    return {
        "message": "Rotina do robô enviada para a fila RabbitMQ com sucesso",
        "queue": RABBITMQ_QUEUE,
        "routine_id": request.routine_id,
        "register_1": request.routine_id,
        "piece_rfid_tag": request.piece_rfid_tag,
        "gripper": dict(digital_twin.gripper),
        "status": "QUEUED",
    }


@app.post("/api/v1/cobot/inspection")
@app.post("/api/cobot/inspection")
def record_inspection_result(request: InspectionResultRequest, user=Depends(require_roles(*WRITE_ROLES))):
    """Grava a inspeção (RFID/Visão) com ação da garra, notifica SSE/MQTT e enfileira o movimento do robô."""
    ok = request.inspection_status == "INSPECTED_OK"
    action_taken = "ASSEMBLY" if ok else "SCRAP_ROUTINE_8"
    routine_id = 9 if ok else 8
    gripper_action = "EGH Grip -> Assembly (LED GREEN)" if ok else "EGH Grip -> Scrap Routine 8 (LED RED)"
    correlation_id = request.correlation_id or f"{request.piece_rfid_tag}-{request.order_id}"
    score = request.quality_score if request.quality_score is not None else (100.0 if ok else 0.0)

    logger.info(
        f"Inspeção {request.piece_rfid_tag} ({request.order_id}): {request.inspection_status} [{correlation_id}]"
    )

    evento = {
        "correlation_id": correlation_id,
        "qrCode": request.piece_rfid_tag,
        "resultado": request.inspection_status,
        "pontuacaoQualidade": score,
        "fonteLeitura": request.antenna_id or "ANT-MONTAGEM-01",
        "order_id": request.order_id,
        "gripper_action": gripper_action,
    }

    # 1. Persistência (idempotente). Sem rastreabilidade, o robô NÃO se move.
    try:
        inserted = storage.salvar_inspecao(evento)
    except Exception as e:
        logger.error(f"Falha ao gravar no banco: {e}")
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Banco de dados indisponível")

    if not inserted:
        logger.info(f"Evento duplicado ({correlation_id}): ignorado, robô não reacionado.")
        return {
            "message": "Evento já processado (idempotência)",
            "piece_rfid_tag": request.piece_rfid_tag,
            "action_taken": "NONE_DUPLICATE",
            "duplicate": True,
        }

    # Atualiza o Gêmeo Digital da garra (LED GREEN para OK / Rotina 9, LED RED para NOK / Rotina 8)
    digital_twin.apply_routine_effect(
        routine_id=routine_id,
        piece_tag=request.piece_rfid_tag,
        order_id=request.order_id,
    )

    evento["ts"] = datetime.now(timezone.utc).isoformat()
    evento["acao"] = action_taken
    evento["gripper"] = dict(digital_twin.gripper)

    # 2. SSE (IHM - canal de inspeção e canal de telemetria do Gêmeo Digital)
    try:
        sse_transmitter.broadcast_event(evento, event_name="inspection")
        telemetry_sse_transmitter.broadcast_event(
            digital_twin.snapshot(include_history=False), event_name="telemetry"
        )
    except Exception as e:
        logger.warning(f"Falha ao transmitir via SSE: {e}")

    # 3 + 4. MQTT (Node-RED/MES) e fila do robô, numa conexão só
    if ok:
        robot_task = {
            "action": "MOVE_TO_ASSEMBLY",
            "routine_id": 9,
            "register_1": 9,
            "piece_rfid_tag": request.piece_rfid_tag,
            "order_id": request.order_id,
            "antenna_id": request.antenna_id,
            "correlation_id": correlation_id,
            "gripper_command": "EGH Grip",
        }
    else:
        robot_task = {
            "action": "MOVE_TO_SCRAP",
            "routine_id": 8,
            "register_1": 8,
            "piece_rfid_tag": request.piece_rfid_tag,
            "order_id": request.order_id,
            "antenna_id": request.antenna_id,
            "correlation_id": correlation_id,
            "gripper_command": "EGH Grip",
        }
    queued = amqp_publish(task_msg=robot_task, mqtt_events=[(MQTT_ROUTING_KEY_INSPECTION, evento)])
    if not queued:
        logger.warning("Inspeção gravada e enviada ao SSE, mas NÃO enfileirada/publicada (broker offline).")

    return {
        "message": f"Validação RFID ({request.inspection_status}) processada com sucesso",
        "piece_rfid_tag": request.piece_rfid_tag,
        "action_taken": action_taken,
        "routine_id": routine_id,
        "gripper_action": gripper_action,
        "gripper": dict(digital_twin.gripper),
        "correlation_id": correlation_id,
        "queued": queued,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
