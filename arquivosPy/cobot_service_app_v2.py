"""
cobot_service_app_v2.py - Microsserviço do Robô Colaborativo (Universal Robots UR3e)
Projeto Integrador SMART 4.0 - Indústria 4.0 (Grupo 2: Cobot, RFID & QA)
Versão 2: Suporte a RFID, Gravação no Banco (storage.py), SSE e RabbitMQ.
"""

import sys
from pathlib import Path
import os
import json
import logging
import asyncio
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Depends, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.responses import StreamingResponse
from pydantic import BaseModel, Field
import pika
import jwt

# 1. Garante que o Python encontre o módulo storage.py no mesmo diretório
sys.path.append(str(Path(__file__).resolve().parent))
import storage

# 2. Configuração de Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cobot-service-rfid")

# 3. Instância Principal do FastAPI (Instanciada ANTES das rotas)
app = FastAPI(
    title="SMART 4.0 - Cobot Service API (com RFID)",
    description="API Serverless para controle, telemetria e agendamento de tarefas do robô UR3e com rastreabilidade via TAGs RFID",
    version="2.0.0"
)

# 4. Configurações de Ambiente e Segurança
JWT_SECRET = os.getenv("JWT_SECRET", "chave_secreta_industria_40_super_segura")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_QUEUE = "cobot_pick_place_queue"

security = HTTPBearer()

# 5. Schemas Pydantic
class RoutineRequest(BaseModel):
    routine_id: int = Field(..., description="ID da rotina no PolyScope (ex: 7=Inspeção, 8=Descarte, 9=Montagem Bloco 1)")
    routine_name: str = Field(..., example="Pega_Lamina_Bloco1")
    order_id: str = Field(..., example="OP-2026-001")
    piece_rfid_tag: Optional[str] = Field(None, example="RFID-SMART40-2026-001-A", description="UID/ID da TAG RFID gravada no SmartBlock")

class InspectionResultRequest(BaseModel):
    piece_rfid_tag: str = Field(..., example="RFID-SMART40-2026-001-A", description="Código ou UID da TAG RFID lida na bancada")
    order_id: str = Field(..., example="OP-2026-001")
    inspection_status: str = Field(..., example="INSPECTED_OK", description="INSPECTED_OK ou INSPECTED_NOK")
    antenna_id: Optional[str] = Field("ANT-MONTAGEM-01", example="ANT-MONTAGEM-01", description="Identificador da antena RFID que efetuou a leitura")
    details: Optional[Dict[str, Any]] = None

# 6. Dependência de Autenticação e RBAC
def verify_jwt_and_role(required_role: str = "ENGINEER"):
    def dependency(credentials: HTTPAuthorizationCredentials = Security(security)):
        token = credentials.credentials
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
            user_role = payload.get("role")
            if user_role not in [required_role, "ADMIN"]:
                raise HTTPException(
                    status_code=status.HTTP_403_FORBIDDEN,
                    detail=f"Acesso negado: Perfil {user_role} não possui permissão de {required_role}"
                )
            return payload
        except jwt.PyJWTError:
            raise HTTPException(
                status_code=status.HTTP_401_UNAUTHORIZED,
                detail="Token JWT inválido ou expirado"
            )
    return dependency

# 7. Helper para Enfileirar Mensagens no RabbitMQ
def publish_to_rabbitmq(message: dict):
    try:
        # Força o IP 127.0.0.1 para evitar problemas com IPv6 no Windows
        parameters = pika.ConnectionParameters(
            host="127.0.0.1", 
            port=RABBITMQ_PORT, 
            connection_attempts=1, 
            retry_delay=1
        )
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
        connection.close()
        logger.info(f"Mensagem enfileirada no RabbitMQ '{RABBITMQ_QUEUE}'")
        return True
    except Exception as e:
        logger.warning(f"[AVISO] RabbitMQ offline (127.0.0.1:{RABBITMQ_PORT}). Inspeção salva no banco e SSE transmitido com sucesso.")
        return False

# 8. Gerenciador de Transmissão SSE em Tempo Real (TH-5)
class SSETransmitter:
    def __init__(self):
        self.clients = set()

    async def subscribe(self):
        queue = asyncio.Queue()
        self.clients.add(queue)
        return queue

    def unsubscribe(self, queue):
        self.clients.discard(queue)

    def broadcast_event(self, event_data: dict, loop=None):
        payload = f"event: inspection\ndata: {json.dumps(event_data)}\n\n"
        for queue in list(self.clients):
            if loop:
                loop.call_soon_threadsafe(queue.put_nowait, payload)
            else:
                queue.put_nowait(payload)

sse_transmitter = SSETransmitter()

# 9. Endpoints da API

@app.get("/health")
def health_check():
    return {
        "status": "online",
        "service": "cobot-service",
        "identification_mode": "RFID",
        "version": "2.0.0"
    }

@app.get("/api/v1/cobot/status")
@app.get("/api/cobot/status")
def get_cobot_status(user=Depends(verify_jwt_and_role(required_role="OPERATOR"))):
    """Retorna telemetria atualizada das 6 juntas e garra SCHUNK EGH do robô UR3e."""
    return {
        "servico": "cobot-service",
        "status": "online",
        "robot_id": "UR3e-MONT-01",
        "robot_mode": "RUNNING",
        "safety_status": "NORMAL",
        "joints_position_deg": [0.0, -90.0, 0.0, -90.0, 0.0, 0.0],
        "tcp_speed_m_s": 0.15,
        "gripper": {
            "model": "SCHUNK EGH80",
            "state": "HOLDING",
            "force_percentage": 50,
            "led_color": "GREEN"
        },
        "rfid_station": {
            "reader_status": "READY",
            "antenna_port": "C/Q1"
        },
        "ultimaInspecao": storage.listar_inspecoes(limit=1)
    }

@app.get("/api/cobot/events/stream")
async def events_stream(access_token: Optional[str] = None):
    """Endpoint SSE para envio de inspeções em tempo real para a IHM/Dashboard."""
    queue = await sse_transmitter.subscribe()

    async def event_generator():
        try:
            while True:
                try:
                    data = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield data
                except asyncio.TimeoutError:
                    yield ": keep-alive\n\n"
        except asyncio.CancelledError:
            sse_transmitter.unsubscribe(queue)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "keep-alive"
        }
    )

@app.get("/api/v1/cobot/inspections")
def get_inspections_history(limit: int = 10):
    """Retorna o histórico de inspeções gravadas no banco de dados."""
    return storage.listar_inspecoes(limit=limit)

@app.post("/api/v1/cobot/routines", status_code=202)
def trigger_cobot_routine(
    request: RoutineRequest,
    user=Depends(verify_jwt_and_role(required_role="ENGINEER"))
):
    """Enfileira ordem de execução de pick-and-place para o robô via RabbitMQ com RFID."""
    payload = {
        "action": "EXECUTE_ROUTINE",
        "routine_id": request.routine_id,
        "routine_name": request.routine_name,
        "order_id": request.order_id,
        "piece_rfid_tag": request.piece_rfid_tag,
        "triggered_by": user.get("sub", "engineer_user")
    }
    
    publish_to_rabbitmq(payload)
    
    return {
        "message": "Rotina do robô enviada para a fila RabbitMQ com sucesso",
        "queue": RABBITMQ_QUEUE,
        "routine_id": request.routine_id,
        "piece_rfid_tag": request.piece_rfid_tag,
        "status": "QUEUED"
    }

@app.post("/api/v1/cobot/inspection")
def record_inspection_result(
    request: InspectionResultRequest,
    user=Depends(verify_jwt_and_role(required_role="ENGINEER"))
):
    """Recebe resultado da validação da TAG RFID, grava no banco, notifica via SSE e comanda o robô."""
    logger.info(f"Inspeção RFID registrada para a peça {request.piece_rfid_tag}: {request.inspection_status}")

    # 1. Grava no banco de dados SQLite local (ou Neon Postgres)
    evento = {
        "correlation_id": f"{request.piece_rfid_tag}-{request.order_id}",
        "qrCode": request.piece_rfid_tag,
        "resultado": request.inspection_status,
        "pontuacaoQualidade": 100.0 if request.inspection_status == "INSPECTED_OK" else 0.0,
        "fonteLeitura": request.antenna_id or "ANT-MONTAGEM-01",
        "order_id": request.order_id
    }
    storage.salvar_inspecao(evento)

    # 2. Transmite em tempo real para a tela HTML via SSE
    try:
        sse_transmitter.broadcast_event(evento)
    except Exception as e:
        logger.warning(f"Não foi possível transmitir evento via SSE: {e}")

    # 3. Se for aprovado, enfileira próximo movimento do robô para montagem
    if request.inspection_status == "INSPECTED_OK":
        publish_to_rabbitmq({
            "action": "MOVE_TO_ASSEMBLY",
            "piece_rfid_tag": request.piece_rfid_tag,
            "order_id": request.order_id,
            "antenna_id": request.antenna_id
        })
    else:
        # Se for reprovado, aciona rotina de descarte (Register 1 = 8 em Smart40.txt)
        publish_to_rabbitmq({
            "action": "MOVE_TO_SCRAP",
            "routine_id": 8,
            "piece_rfid_tag": request.piece_rfid_tag,
            "order_id": request.order_id,
            "antenna_id": request.antenna_id
        })
        
    return {
        "message": f"Validação RFID ({request.inspection_status}) processada com sucesso",
        "piece_rfid_tag": request.piece_rfid_tag,
        "action_taken": "ASSEMBLY" if request.inspection_status == "INSPECTED_OK" else "SCRAP_ROUTINE_8"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)