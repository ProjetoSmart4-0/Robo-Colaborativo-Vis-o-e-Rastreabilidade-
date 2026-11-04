"""
cobot_service_app-v2.py - Microsserviço do Robô Colaborativo (Universal Robots UR3e)
Projeto Integrador SMART 4.0 - Indústria 4.0 (Grupo 2: Cobot, RFID & QA)
Versão 2: Substituição da identificação por QR Code por TAG RFID / Antena Industrial.
"""

import os
import json
import logging
from typing import Dict, Any, Optional
from fastapi import FastAPI, HTTPException, Depends, Security, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from pydantic import BaseModel, Field
import pika
import jwt

# Configuração de Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cobot-service-rfid")

app = FastAPI(
    title="SMART 4.0 - Cobot Service API (com RFID)",
    description="API Serverless para controle, telemetria e agendamento de tarefas do robô UR3e com rastreabilidade via TAGs RFID",
    version="2.0.0"
)

# Configurações de Ambiente
JWT_SECRET = os.getenv("JWT_SECRET", "chave_secreta_industria_40_super_segura")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_QUEUE = "cobot_pick_place_queue"

security = HTTPBearer()

# Schemas Pydantic atualizados para RFID
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

# Dependência de Autenticação e RBAC
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

# Helper para Enfileirar Mensagens no RabbitMQ
def publish_to_rabbitmq(message: dict):
    try:
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(host=RABBITMQ_HOST, port=RABBITMQ_PORT)
        )
        channel = connection.channel()
        channel.queue_declare(queue=RABBITMQ_QUEUE, durable=True)
        channel.basic_publish(
            exchange='',
            routing_key=RABBITMQ_QUEUE,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)  # mensagem persistente
        )
        connection.close()
        logger.info(f"Mensagem enfileirada com sucesso na fila '{RABBITMQ_QUEUE}'")
        return True
    except Exception as e:
        logger.error(f"Erro ao conectar ao RabbitMQ ({RABBITMQ_HOST}:{RABBITMQ_PORT}): {str(e)}")
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail=f"Falha ao conectar ao barramento RabbitMQ: {str(e)}"
        )

# Endpoints
@app.get("/health")
def health_check():
    return {
        "status": "online",
        "service": "cobot-service",
        "identification_mode": "RFID",
        "version": "2.0.0"
    }

@app.get("/api/v1/cobot/status")
def get_cobot_status(user=Depends(verify_jwt_and_role(required_role="OPERATOR"))):
    """Retorna telemetria atualizada das 6 juntas e garra SCHUNK EGH do robô UR3e."""
    return {
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
        }
    }

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
    """Recebe resultado da validação da TAG RFID e comanda o próximo passo do robô."""
    logger.info(f"Inspeção RFID registrada para a peça {request.piece_rfid_tag}: {request.inspection_status}")
    
    # Se for aprovado, enfileira próximo movimento do robô para montagem
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
