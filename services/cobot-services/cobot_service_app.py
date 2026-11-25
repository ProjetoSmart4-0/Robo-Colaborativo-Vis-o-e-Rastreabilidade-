"""
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
import json
import time
import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Any, Optional, Literal, List, Tuple

import pika
import jwt
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
APP_VERSION = "2.1.0"

JWT_SECRET = os.getenv("JWT_SECRET")
if not JWT_SECRET:
    JWT_SECRET = "dev-only-secret-nao-usar-em-producao"
    logger.warning("JWT_SECRET não definido: usando segredo de desenvolvimento. Defina a variável no Render.")
JWT_ALGORITHM = "HS256"

RABBITMQ_URL = os.getenv("RABBITMQ_URL")  # amqps://user:pass@host/vhost (CloudAMQP) - tem prioridade
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_QUEUE = "cobot_pick_place_queue"

# O plugin MQTT do RabbitMQ liga o exchange amq.topic aos tópicos MQTT (ponto <-> barra).
MQTT_EXCHANGE = "amq.topic"
MQTT_ROUTING_KEY_INSPECTION = os.getenv("MQTT_ROUTING_KEY_INSPECTION", "smart40.n2.cobot.inspection")

CORS_ORIGINS = [o.strip() for o in os.getenv("CORS_ORIGINS", "*").split(",") if o.strip()]
SSE_REQUIRE_AUTH = os.getenv("SSE_REQUIRE_AUTH", "0") == "1"

if os.getenv("RENDER") and storage.BACKEND == "sqlite":
    logger.warning("Rodando no Render com SQLite: os dados são perdidos a cada deploy. Defina DATABASE_URL (Neon).")

app = FastAPI(
    title="SMART 4.0 - Cobot Service API (RFID)",
    description="API do robô UR3e com rastreabilidade RFID, SSE e mensageria RabbitMQ/MQTT",
    version=APP_VERSION,
)
app.add_middleware(CORSMiddleware, allow_origins=CORS_ORIGINS, allow_methods=["*"], allow_headers=["*"])

# ----------------------------------------------------------------------------
# Autenticação JWT + RBAC
# ----------------------------------------------------------------------------
bearer = HTTPBearer(auto_error=False)


def _decode_token(token: str, allowed_roles: set) -> dict:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGORITHM])
    except jwt.PyJWTError:
        raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token JWT inválido ou expirado",
                            headers={"WWW-Authenticate": "Bearer"})
    role = payload.get("role")
    if role not in allowed_roles:
        raise HTTPException(status.HTTP_403_FORBIDDEN,
                            f"Acesso negado: perfil '{role}' não tem permissão para esta operação")
    return payload


def require_roles(*roles: str):
    """ADMIN sempre é aceito. Ex.: require_roles('OPERATOR','ENGINEER') para leitura."""
    allowed = set(roles) | {"ADMIN"}

    def dependency(credentials: Optional[HTTPAuthorizationCredentials] = Security(bearer)):
        if credentials is None:
            raise HTTPException(status.HTTP_401_UNAUTHORIZED, "Token de autenticação ausente",
                                headers={"WWW-Authenticate": "Bearer"})
        return _decode_token(credentials.credentials, allowed)

    return dependency


READ_ROLES = ("OPERATOR", "ENGINEER")
WRITE_ROLES = ("ENGINEER",)

# ----------------------------------------------------------------------------
# Schemas
# ----------------------------------------------------------------------------
class RoutineRequest(BaseModel):
    routine_id: int = Field(..., description="ID da rotina no PolyScope (7=Inspeção, 8=Descarte, 9=Montagem)")
    routine_name: str = Field(..., examples=["Pega_Lamina_Bloco1"])
    order_id: str = Field(..., examples=["OP-2026-001"])
    piece_rfid_tag: Optional[str] = Field(None, examples=["RFID-SMART40-2026-001-A"])


class InspectionResultRequest(BaseModel):
    piece_rfid_tag: str = Field(..., examples=["RFID-SMART40-2026-001-A"])
    order_id: str = Field(..., examples=["OP-2026-001"])
    inspection_status: Literal["INSPECTED_OK", "INSPECTED_NOK"]
    antenna_id: Optional[str] = Field("ANT-MONTAGEM-01")
    correlation_id: Optional[str] = Field(
        None,
        description="Id único do evento de chegada do pallet. Reenvios com o mesmo id não repetem o movimento do robô. "
                    "Se omitido, usa '<tag>-<order_id>'.",
    )
    details: Optional[Dict[str, Any]] = None


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
                exchange="", routing_key=RABBITMQ_QUEUE, body=json.dumps(task_msg),
                properties=pika.BasicProperties(delivery_mode=2, content_type="application/json"),
            )
        for routing_key, body in (mqtt_events or []):
            channel.basic_publish(
                exchange=MQTT_EXCHANGE, routing_key=routing_key, body=json.dumps(body),
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
# SSE
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

# ----------------------------------------------------------------------------
# Endpoints
# ----------------------------------------------------------------------------
@app.get("/")
def root():
    return {"service": "cobot-service", "docs": "/docs", "health": "/health", "version": APP_VERSION}


@app.get("/health")
def health_check():
    return {
        "status": "online",
        "service": "cobot-service",
        "identification_mode": "RFID",
        "version": APP_VERSION,
        "database": storage.BACKEND,
        "broker_configured": bool(RABBITMQ_URL) or RABBITMQ_HOST != "localhost",
    }


@app.get("/api/v1/cobot/status")
@app.get("/api/cobot/status")
def get_cobot_status(user=Depends(require_roles(*READ_ROLES))):
    """Telemetria do UR3e. ATENÇÃO: ainda simulada (o agente RTDE não alimenta a API)."""
    return {
        "servico": "cobot-service",
        "status": "online",
        "telemetry_source": "SIMULATED",
        "robot_id": "UR3e-MONT-01",
        "robot_mode": "RUNNING",
        "safety_status": "NORMAL",
        "joints_position_deg": [0.0, -90.0, 0.0, -90.0, 0.0, 0.0],
        "tcp_speed_m_s": 0.15,
        "gripper": {"model": "SCHUNK EGH80", "state": "HOLDING", "force_percentage": 50, "led_color": "GREEN"},
        "rfid_station": {"reader_status": "READY", "antenna_port": "C/Q1"},
        "ultimaInspecao": storage.listar_inspecoes(limit=1),
    }


@app.get("/api/cobot/events/stream")
async def events_stream(access_token: Optional[str] = None):
    """SSE de inspeções. EventSource do navegador não envia headers, por isso o token vai em ?access_token=.
    Público por padrão; defina SSE_REQUIRE_AUTH=1 para exigir token válido."""
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


@app.get("/api/v1/cobot/inspections")
def get_inspections_history(limit: int = 10):
    return storage.listar_inspecoes(limit=limit)


@app.post("/api/v1/cobot/routines", status_code=202)
def trigger_cobot_routine(request: RoutineRequest, user=Depends(require_roles(*WRITE_ROLES))):
    """Enfileira uma rotina manual. Se o broker estiver fora, responde 503 (não finge que enfileirou)."""
    payload = {
        "action": "EXECUTE_ROUTINE",
        "routine_id": request.routine_id,
        "routine_name": request.routine_name,
        "order_id": request.order_id,
        "piece_rfid_tag": request.piece_rfid_tag,
        "triggered_by": user.get("sub", "engineer_user"),
    }
    if not amqp_publish(task_msg=payload):
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Barramento RabbitMQ indisponível")
    return {
        "message": "Rotina do robô enviada para a fila RabbitMQ com sucesso",
        "queue": RABBITMQ_QUEUE,
        "routine_id": request.routine_id,
        "piece_rfid_tag": request.piece_rfid_tag,
        "status": "QUEUED",
    }


@app.post("/api/v1/cobot/inspection")
def record_inspection_result(request: InspectionResultRequest, user=Depends(require_roles(*WRITE_ROLES))):
    """Grava a inspeção, notifica SSE/MQTT e enfileira o movimento do robô."""
    ok = request.inspection_status == "INSPECTED_OK"
    action_taken = "ASSEMBLY" if ok else "SCRAP_ROUTINE_8"
    correlation_id = request.correlation_id or f"{request.piece_rfid_tag}-{request.order_id}"
    logger.info(f"Inspeção RFID {request.piece_rfid_tag} ({request.order_id}): {request.inspection_status} [{correlation_id}]")

    evento = {
        "correlation_id": correlation_id,
        "qrCode": request.piece_rfid_tag,
        "resultado": request.inspection_status,
        "pontuacaoQualidade": 100.0 if ok else 0.0,
        "fonteLeitura": request.antenna_id or "ANT-MONTAGEM-01",
        "order_id": request.order_id,
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

    evento["ts"] = datetime.now(timezone.utc).isoformat()
    evento["acao"] = action_taken

    # 2. SSE (IHM)
    try:
        sse_transmitter.broadcast_event(evento)
    except Exception as e:
        logger.warning(f"Falha ao transmitir via SSE: {e}")

    # 3 + 4. MQTT (Node-RED/MES) e fila do robô, numa conexão só
    if ok:
        robot_task = {
            "action": "MOVE_TO_ASSEMBLY",
            "piece_rfid_tag": request.piece_rfid_tag,
            "order_id": request.order_id,
            "antenna_id": request.antenna_id,
            "correlation_id": correlation_id,
        }
    else:
        robot_task = {
            "action": "MOVE_TO_SCRAP",
            "routine_id": 8,
            "piece_rfid_tag": request.piece_rfid_tag,
            "order_id": request.order_id,
            "antenna_id": request.antenna_id,
            "correlation_id": correlation_id,
        }
    queued = amqp_publish(task_msg=robot_task, mqtt_events=[(MQTT_ROUTING_KEY_INSPECTION, evento)])
    if not queued:
        logger.warning("Inspeção gravada e enviada ao SSE, mas NÃO enfileirada/publicada (broker offline).")

    return {
        "message": f"Validação RFID ({request.inspection_status}) processada com sucesso",
        "piece_rfid_tag": request.piece_rfid_tag,
        "action_taken": action_taken,
        "correlation_id": correlation_id,
        "queued": queued,
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", 8080)))
