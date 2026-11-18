"""
cobot_worker_consumer.py - Worker Consumidor da Fila de Tarefas RabbitMQ (AMQP 5672)
Projeto Integrador SMART 4.0 - Planta N2 (Grupo 2: Cobot, RFID & QA)
Versão 2: Suporte completo à rastreabilidade por TAG RFID do SmartBlock.
"""

import os
import json
import time
import socket
import logging
import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cobot-worker-rfid")

# Parâmetros de Conexão
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_NAME = "cobot_pick_place_queue"
RABBITMQ_URL = os.getenv("RABBITMQ_URL")  # amqps://... do CloudAMQP; tem prioridade sobre host/porta

UR_ROBOT_IP = os.getenv("UR_ROBOT_IP", "192.168.1.100")
DASHBOARD_PORT = 29999  # Porta Dashboard Server do PolyScope UR

def trigger_robot_routine(routine_id: int, piece_rfid_tag: str):
    """Aciona a rotina no controlador do robô UR associando à TAG RFID da peça."""
    logger.info(f"Disparando Rotina {routine_id} no UR3e para o SmartBlock (RFID: '{piece_rfid_tag}')...")
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.settimeout(1.5)
        s.connect((UR_ROBOT_IP, DASHBOARD_PORT))
        s.recv(1024)
        s.sendall(b"play\n")
        resp = s.recv(1024)
        logger.info(f"Resposta do PolyScope Dashboard: {resp.decode().strip()}")
    except Exception as e:
        logger.warning(
            f"Robô físico não acessível em {UR_ROBOT_IP}:{DASHBOARD_PORT} ({e}). "
            f"Rotina {routine_id} (RFID: {piece_rfid_tag}) executada em modo simulado/mock."
        )
    finally:
        s.close()

def on_message_received(ch, method, properties, body):
    """Callback executado a cada tarefa recebida da fila."""
    try:
        task = json.loads(body.decode())
        action = task.get("action")
        # Suporta o novo campo piece_rfid_tag mantendo fallback retrocompatível
        piece_rfid = task.get("piece_rfid_tag") or task.get("piece_qr_code") or "UNKNOWN_TAG"
        order_id = task.get("order_id", "UNKNOWN")
        antenna_id = task.get("antenna_id", "ANT-MONTAGEM-01")

        logger.info(f"[NOVA TAREFA] Ação: {action} | OP: {order_id} | RFID: {piece_rfid} | Antena: {antenna_id}")

        if action == "MOVE_TO_ASSEMBLY":
            logger.info(f"SmartBlock com TAG {piece_rfid} APROVADO: Executando rotina de montagem (Rotina 9).")
            trigger_robot_routine(routine_id=9, piece_rfid_tag=piece_rfid)

        elif action == "MOVE_TO_SCRAP":
            logger.warning(f"SmartBlock com TAG {piece_rfid} REPROVADO: Executando Rotina 8 (Descarte / Refugo).")
            trigger_robot_routine(routine_id=8, piece_rfid_tag=piece_rfid)

        elif action == "EXECUTE_ROUTINE":
            routine_id = task.get("routine_id", 0)
            logger.info(f"Executando rotina parametrizada {routine_id} ({task.get('routine_name')}) para RFID {piece_rfid}.")
            trigger_robot_routine(routine_id=routine_id, piece_rfid_tag=piece_rfid)

        else:
            logger.warning(f"Ação não reconhecida: {action}")

        # Confirma processamento (ACK)
        ch.basic_ack(delivery_tag=method.delivery_tag)
        logger.info("Tarefa finalizada com confirmação (ACK).")

    except Exception as err:
        logger.error(f"Erro no processamento da mensagem: {err}")
        ch.basic_nack(delivery_tag=method.delivery_tag, requeue=False)

def get_connection_parameters():
    if RABBITMQ_URL:
        params = pika.URLParameters(RABBITMQ_URL)
        params.heartbeat = 60
        return params
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    return pika.ConnectionParameters(
        host=RABBITMQ_HOST,
        port=RABBITMQ_PORT,
        credentials=credentials,
        heartbeat=60
    )

def start_worker():
    parameters = get_connection_parameters()

    destino = "CloudAMQP (RABBITMQ_URL)" if RABBITMQ_URL else f"{RABBITMQ_HOST}:{RABBITMQ_PORT}"
    logger.info(f"Conectando worker ao RabbitMQ em {destino}...")
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message_received)

    logger.info(f"Worker ativo na fila '{QUEUE_NAME}'. Aguardando leituras e ordens RFID...")
    channel.start_consuming()

if __name__ == "__main__":
    # Reconecta sozinho se a conexão cair (comum em broker em nuvem)
    while True:
        try:
            start_worker()
        except KeyboardInterrupt:
            logger.info("Worker encerrado.")
            break
        except pika.exceptions.AMQPConnectionError as err:
            logger.error(f"Conexão com o RabbitMQ perdida: {err}. Nova tentativa em 5 s...")
            time.sleep(5)
