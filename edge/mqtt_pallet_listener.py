"""
mqtt_pallet_listener.py - Agente de borda: escuta a chegada do pallet (MQTT) e dispara a inspeção RFID.

Fluxo:  CLP --MQTT--> Node-RED --MQTT (CloudAMQP)--> [este agente] --> rfid_inspection --HTTPS--> cobot-service

Tópico (padrão): smart40/n2/montagem/pallet
Payload esperado (JSON, gerado pelo fluxo Node-RED):
  {"event":"PALLET_ARRIVED","station":"MONTAGEM","order_id":"OP-2026-001",
   "correlation_id":"<id único>","ts":"2026-11-24T12:00:00.000Z","tag_uid":"<opcional, p/ testes>"}

Segurança operacional (o robô se move por causa disso):
  - sessão MQTT "limpa": mensagens atrasadas NÃO são entregues depois de uma queda de conexão;
  - eventos mais velhos que MAX_EVENT_AGE_S (padrão 30 s) são descartados;
  - correlation_id repetido (reentrega QoS 1) é ignorado;
  - a inspeção roda numa thread própria, sem bloquear o keep-alive MQTT.

Variáveis de ambiente: MQTT_HOST, MQTT_PORT (8883), MQTT_USERNAME ("vhost:user"), MQTT_PASSWORD,
MQTT_TLS (auto: ligado na 8883), MQTT_TOPIC_PALLET, MAX_EVENT_AGE_S, MQTT_CLIENT_ID,
além das de rfid_inspection.py (COBOT_SERVICE_URL, TEST_JWT_TOKEN, ORDER_ID, RFID_ALLOW_SIMULATION).
"""

import os
import json
import queue
import socket
import logging
import threading
from collections import deque
from datetime import datetime, timezone

import paho.mqtt.client as mqtt

from rfid_inspection import process_rfid_pipeline, DEFAULT_ORDER_ID

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("mqtt-pallet-listener")

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 8883))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_TLS = os.getenv("MQTT_TLS", "auto")  # auto | 1 | 0
TOPIC_PALLET = os.getenv("MQTT_TOPIC_PALLET", "smart40/n2/montagem/pallet")
MAX_EVENT_AGE_S = float(os.getenv("MAX_EVENT_AGE_S", 30))
CLIENT_ID = os.getenv("MQTT_CLIENT_ID", f"edge-rfid-{socket.gethostname()}")

_jobs: "queue.Queue[dict]" = queue.Queue(maxsize=10)
_seen_ids: deque = deque(maxlen=100)


def _event_age_seconds(ts: str):
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return (datetime.now(timezone.utc) - dt).total_seconds()
    except Exception:
        return None


def handle_message(raw: bytes) -> bool:
    """Valida o evento e o coloca na fila de trabalho. Retorna True se aceito."""
    try:
        evt = json.loads(raw.decode("utf-8"))
    except Exception:
        logger.warning(f"Payload não é JSON válido: {raw[:120]!r}")
        return False

    if not isinstance(evt, dict) or evt.get("event") != "PALLET_ARRIVED":
        logger.info(f"Evento ignorado (esperado event=PALLET_ARRIVED): {str(evt)[:120]}")
        return False

    cid = evt.get("correlation_id")
    if not cid:
        logger.warning("Evento sem correlation_id: ignorado (sem ele não há proteção contra repetição).")
        return False
    if cid in _seen_ids:
        logger.info(f"Evento {cid} já visto (reentrega QoS 1): ignorado.")
        return False

    ts = evt.get("ts")
    if ts:
        age = _event_age_seconds(ts)
        if age is None:
            logger.warning(f"Timestamp ilegível ({ts}): evento ignorado.")
            return False
        if age > MAX_EVENT_AGE_S:
            logger.warning(f"Evento {cid} velho demais ({age:.0f}s > {MAX_EVENT_AGE_S:.0f}s): descartado por segurança.")
            return False
    else:
        logger.warning(f"Evento {cid} sem 'ts': aceito, mas sem checagem de validade.")

    try:
        _jobs.put_nowait(evt)
    except queue.Full:
        logger.error("Fila de inspeções cheia: evento descartado.")
        return False
    _seen_ids.append(cid)
    return True


def worker_loop():
    while True:
        evt = _jobs.get()
        try:
            logger.info(f"Pallet {evt['correlation_id']} na estação {evt.get('station', '?')}: iniciando inspeção RFID...")
            process_rfid_pipeline(
                simulated_tag=evt.get("tag_uid"),
                order_id=evt.get("order_id") or DEFAULT_ORDER_ID,
                correlation_id=evt["correlation_id"],
            )
        except Exception:
            logger.exception("Falha no pipeline de inspeção RFID")
        finally:
            _jobs.task_done()


# --- MQTT (paho-mqtt 2.x) ----------------------------------------------------
def on_connect(client, userdata, flags, reason_code, properties):
    if reason_code.is_failure:
        logger.error(f"Conexão MQTT recusada: {reason_code}")
        return
    logger.info(f"Conectado ao broker {MQTT_HOST}:{MQTT_PORT}. Assinando '{TOPIC_PALLET}' (QoS 1)")
    client.subscribe(TOPIC_PALLET, qos=1)  # reassina a cada reconexão


def on_disconnect(client, userdata, flags, reason_code, properties):
    logger.warning(f"Desconectado do broker ({reason_code}). Reconectando automaticamente...")


def on_message(client, userdata, msg):
    handle_message(msg.payload)


def build_client() -> mqtt.Client:
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=CLIENT_ID,
        protocol=mqtt.MQTTv311,   # RabbitMQ/CloudAMQP: MQTT 3.1.1
        clean_session=True,       # sem mensagens atrasadas após reconexão
    )
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    if MQTT_TLS == "1" or (MQTT_TLS == "auto" and MQTT_PORT == 8883):
        client.tls_set()  # valida o certificado com as CAs do sistema
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message
    client.reconnect_delay_set(min_delay=1, max_delay=30)
    return client


def main():
    threading.Thread(target=worker_loop, daemon=True, name="rfid-worker").start()
    client = build_client()
    logger.info(f"Iniciando agente (client_id={CLIENT_ID})...")
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    try:
        client.loop_forever(retry_first_connection=True)
    except KeyboardInterrupt:
        logger.info("Encerrado pelo operador.")
        client.disconnect()


if __name__ == "__main__":
    main()
