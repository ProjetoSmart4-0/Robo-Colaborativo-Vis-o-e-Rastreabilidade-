"""
cobot_worker_consumer.py - Worker Consumidor da Fila de Tarefas RabbitMQ (AMQP 5672)
Projeto Integrador SMART 4.0 - Planta N2 (Grupo 2: Cobot, RFID & Gêmeo Digital SCHUNK EGH80)

Versão 3 (Sprint 3):
  - Fila padrão alinhada com cobot-service: 'cobot_pick_place_queue'
  - Despacho de rotinas via Register 1 (7=Inspeção, 8=Refugo, 9=Montagem L1, 10=L3, 11=L2)
    e Dashboard Server (29999) / Secondary Interface (30002)
  - Suporte aos comandos diretos do Gêmeo Digital da garra SCHUNK EGH80:
    'EGH Grip' (fechar), 'EGH Release' (abrir) e 'EGH Position' (abertura proporcional em mm/%)
  - Modo Simulação / Dry-Run seguro quando a bancada física UR3e está desconectada
"""

import os
import json
import time
import socket
import logging
from typing import Optional
import pika

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("cobot-worker-rfid")

# Parâmetros de Conexão RabbitMQ / CloudAMQP
RABBITMQ_HOST = os.getenv("RABBITMQ_HOST", "localhost")
RABBITMQ_PORT = int(os.getenv("RABBITMQ_PORT", 5672))
RABBITMQ_USER = os.getenv("RABBITMQ_USER", "guest")
RABBITMQ_PASS = os.getenv("RABBITMQ_PASS", "guest")
QUEUE_NAME = os.getenv("QUEUE_TASKS", "cobot_pick_place_queue")
RABBITMQ_URL = os.getenv("RABBITMQ_URL")  # amqps://... do CloudAMQP; tem prioridade sobre host/porta

# Parâmetros do Robô UR3e e Garra SCHUNK EGH80
UR_ROBOT_IP = os.getenv("UR_ROBOT_IP", "192.168.1.100")
DASHBOARD_PORT = int(os.getenv("UR_DASHBOARD_PORT", "29999"))   # Porta Dashboard Server do PolyScope UR
UR_SECONDARY_PORT = int(os.getenv("UR_SECONDARY_PORT", "30002"))  # Porta URScript / Registradores

DRY_RUN = os.getenv("DRY_RUN", "true").lower() != "false"
ROBOT_COMMANDS_ENABLED = os.getenv("ROBOT_COMMANDS_ENABLED", "false").lower() == "true"


def write_ur_register_and_play(routine_id: int, piece_rfid_tag: str) -> bool:
    """
    Escreve o ID da rotina no Register 1 (write_output_integer_register(1, routine_id))
    via porta secundária 30002 e garante que o programa principal esteja em execução via Dashboard 29999.
    """
    if DRY_RUN or not ROBOT_COMMANDS_ENABLED:
        logger.info(
            f"[SIMULADO / DIGITAL TWIN] Register 1 <- {routine_id} | Executando rotina {routine_id} para TAG '{piece_rfid_tag}'"
        )
        return True

    logger.info(f"Escrevendo Register 1 = {routine_id} no UR3e ({UR_ROBOT_IP}) para o SmartBlock (RFID: '{piece_rfid_tag}')...")
    try:
        # 1. Envia URScript de escrita de registrador na porta 30002
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_sec:
            s_sec.settimeout(1.5)
            s_sec.connect((UR_ROBOT_IP, UR_SECONDARY_PORT))
            urscript_cmd = f"write_output_integer_register(1, {int(routine_id)})\n".encode("utf-8")
            s_sec.sendall(urscript_cmd)

        # 2. Garante 'play' no Dashboard Server (29999)
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_dash:
            s_dash.settimeout(1.5)
            s_dash.connect((UR_ROBOT_IP, DASHBOARD_PORT))
            s_dash.recv(1024)
            s_dash.sendall(b"play\n")
            resp = s_dash.recv(1024)
            logger.info(f"Resposta do PolyScope Dashboard: {resp.decode(errors='ignore').strip()}")
        return True
    except Exception as e:
        logger.warning(
            f"Robô físico não acessível em {UR_ROBOT_IP} ({e}). "
            f"Rotina {routine_id} (Register 1 = {routine_id}, RFID: {piece_rfid_tag}) executada em modo simulado/Digital Twin."
        )
        return False


DRY_RUN = os.getenv("DRY_RUN", "true").lower() != "false"
ROBOT_COMMANDS_ENABLED = os.getenv("ROBOT_COMMANDS_ENABLED", "false").lower() == "true"

def trigger_robot_routine(routine_id: int, piece_rfid_tag: str):
    """Aciona a rotina no controlador do robô UR associando à TAG RFID da peça."""
    write_ur_register_and_play(routine_id=routine_id, piece_rfid_tag=piece_rfid_tag)


def trigger_gripper_command(
    gripper_command: str,
    position_mm: float = 18.5,
    force_percentage: int = 50,
    led_color: str = "GREEN",
    piece_rfid_tag: str = "SCHUNK-EGH80",
):
    """
    Executa comando direto na Garra Elétrica SCHUNK EGH80 (via URCap IO-Link / TURCK FEN20).
    Comandos suportados: 'EGH Grip', 'EGH Release', 'EGH Position'.
    """
    if DRY_RUN or not ROBOT_COMMANDS_ENABLED:
        logger.info(
            f"[SIMULADO / DIGITAL TWIN] Garra SCHUNK EGH80 -> Comando: '{gripper_command}' | "
            f"Posição: {position_mm} mm | Força: {force_percentage}% | LED: {led_color} | Peça: {piece_rfid_tag}"
        )
        return True

    logger.info(
        f"Enviando comando IO-Link '{gripper_command}' para Garra SCHUNK EGH80 em {UR_ROBOT_IP}:{UR_SECONDARY_PORT} "
        f"(pos={position_mm}mm, force={force_percentage}%, led={led_color})..."
    )
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s_sec:
            s_sec.settimeout(1.5)
            s_sec.connect((UR_ROBOT_IP, UR_SECONDARY_PORT))
            # Emite comentário/chamada URScript compatível com o URCap da SCHUNK EGH80
            script = (
                f"# SCHUNK EGH80 Command: {gripper_command}\n"
                f"write_output_float_register(0, {float(position_mm):.2f})\n"
                f"write_output_integer_register(2, {int(force_percentage)})\n"
            ).encode("utf-8")
            s_sec.sendall(script)
        return True
    except Exception as e:
        logger.warning(
            f"Bancada física indisponível em {UR_ROBOT_IP}:{UR_SECONDARY_PORT} ({e}). "
            f"Comando '{gripper_command}' da garra SCHUNK EGH80 processado em modo Gêmeo Digital."
        )
        return False


def on_message_received(ch, method, properties, body):
    """Callback executado a cada tarefa recebida da fila AMQP."""
    try:
        task = json.loads(body.decode())
        action = task.get("action")
        piece_rfid = task.get("piece_rfid_tag") or task.get("piece_qr_code") or "UNKNOWN_TAG"
        order_id = task.get("order_id", "UNKNOWN")
        antenna_id = task.get("antenna_id", "ANT-MONTAGEM-01")

        logger.info(f"[NOVA TAREFA] Ação: {action} | OP: {order_id} | RFID: {piece_rfid} | Antena: {antenna_id}")

        if action == "MOVE_TO_ASSEMBLY":
            routine_id = int(task.get("routine_id", 9))
            logger.info(f"SmartBlock com TAG {piece_rfid} APROVADO: Executando rotina de montagem (Register 1 = {routine_id}).")
            trigger_robot_routine(routine_id=routine_id, piece_rfid_tag=piece_rfid)

        elif action == "MOVE_TO_SCRAP":
            routine_id = int(task.get("routine_id", 8))
            logger.warning(f"SmartBlock com TAG {piece_rfid} REPROVADO: Executando Rotina {routine_id} (Descarte / Refugo).")
            trigger_robot_routine(routine_id=routine_id, piece_rfid_tag=piece_rfid)

        elif action == "EXECUTE_ROUTINE":
            routine_id = int(task.get("routine_id", 0))
            logger.info(
                f"Executando rotina parametrizada (Register 1 = {routine_id}, '{task.get('routine_name')}') para RFID {piece_rfid}."
            )
            trigger_robot_routine(routine_id=routine_id, piece_rfid_tag=piece_rfid)

        elif action == "GRIPPER_COMMAND":
            cmd = task.get("gripper_command", "EGH Grip")
            pos_mm = float(task.get("position_mm", 18.5))
            force_pct = int(task.get("force_percentage", 50))
            led = task.get("led_color", "GREEN")
            trigger_gripper_command(
                gripper_command=cmd,
                position_mm=pos_mm,
                force_percentage=force_pct,
                led_color=led,
                piece_rfid_tag=piece_rfid,
            )

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
        heartbeat=60,
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

    logger.info(f"Worker ativo na fila '{QUEUE_NAME}'. Aguardando comandos do Gêmeo Digital e ordens RFID...")
    channel.start_consuming()


if __name__ == "__main__":
    while True:
        try:
            start_worker()
        except KeyboardInterrupt:
            logger.info("Worker encerrado.")
            break
        except pika.exceptions.AMQPConnectionError as err:
            logger.error(f"Conexão com o RabbitMQ perdida: {err}. Nova tentativa em 5 s...")
            time.sleep(5)