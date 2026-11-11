"""
cobot_service_app.py - Microsserviço do Robô Colaborativo (Universal Robots UR3e)
Projeto Integrador SMART 4.0 - Indústria 4.0 (Grupo 2)
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
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("cobot-service")

app = FastAPI(
    title="SMART 4.0 - Cobot Service API",
    description="API Serverless para controle, telemetria e agendamento de tarefas do robô UR3e com Garra SCHUNK EGH80",
    version="1.0.0"
)

# Configurações de Ambiente
JWT_SECRET = os.getenv("JWT_SECRET", "chave_secreta_industria_40_super_segura")
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_QUEUE = "cobot_pick_place_queue"

security = HTTPBearer()

# Schemas Pydantic
class RoutineRequest(BaseModel):
    routine_id: int = Field(..., description="ID da rotina registrada no PolyScope (ex: 7=Foto, 8=Descarte, 9=Bloco1 Lamina1)")
    routine_name: str = Field(..., example="Pega_Lamina_Bloco1")
    order_id: str = Field(..., example="OP-2026-001")
    piece_qr_code: Optional[str] = Field(None, example="QR-SMART40-2026-001-A")

class InspectionResultRequest(BaseModel):
    piece_qr_code: str = Field(..., example="QR-SMART40-2026-001-A")
    order_id: str = Field(..., example="OP-2026-001")
    inspection_status: str = Field(..., example="INSPECTED_OK", description="INSPECTED_OK ou INSPECTED_NOK")
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
    return {"status": "online", "service": "cobot-service", "version": "1.0.0"}

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
        }
    }

@app.post("/api/v1/cobot/routines", status_code=202)
def trigger_cobot_routine(
    request: RoutineRequest,
    user=Depends(verify_jwt_and_role(required_role="ENGINEER"))
):
    """Enfileira ordem de execução de pick-and-place para o robô via RabbitMQ."""
    payload = {
        "action": "EXECUTE_ROUTINE",
        "routine_id": request.routine_id,
        "routine_name": request.routine_name,
        "order_id": request.order_id,
        "piece_qr_code": request.piece_qr_code,
        "triggered_by": user.get("sub", "engineer_user")
    }
    
    publish_to_rabbitmq(payload)
    
    return {
        "message": "Rotina do robô enviada para a fila RabbitMQ com sucesso",
        "queue": RABBITMQ_QUEUE,
        "routine_id": request.routine_id,
        "status": "QUEUED"
    }

@app.post("/api/v1/cobot/inspection")
def record_inspection_result(
    request: InspectionResultRequest,
    user=Depends(verify_jwt_and_role(required_role="ENGINEER"))
):
    """Recebe resultado da inspeção de qualidade por visão e registra o resultado."""
    logger.info(f"Inspeção registrada para a peça {request.piece_qr_code}: {request.inspection_status}")
    
    # Se for aprovado, enfileira próximo movimento do robô para montagem
    if request.inspection_status == "INSPECTED_OK":
        publish_to_rabbitmq({
            "action": "MOVE_TO_ASSEMBLY",
            "piece_qr_code": request.piece_qr_code,
            "order_id": request.order_id
        })
    else:
        # Se for reprovado, aciona rotina de descarte (Register 1 = 8 em Smart40.txt)
        publish_to_rabbitmq({
            "action": "MOVE_TO_SCRAP",
            "routine_id": 8,
            "piece_qr_code": request.piece_qr_code,
            "order_id": request.order_id
        })
        
    return {
        "message": f"Resultado da inspeção ({request.inspection_status}) gravado com sucesso",
        "piece_qr_code": request.piece_qr_code,
        "action_taken": "ASSEMBLY" if request.inspection_status == "INSPECTED_OK" else "SCRAP_ROUTINE_8"
    }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)