"""
telemetry_mqtt_publisher.py - Publica a telemetria do UR3e em MQTT (smart40/n2/cobot/status).

ATENÇÃO: enquanto ur_rtde_telemetry.URTelemetryClient devolver dados simulados, o que vai
para o broker também é simulado (o campo "source" do payload indica isso). Troque o cliente
por ur_rtde.RTDEReceiveInterface (lib `ur_rtde`) quando for ligar no robô real.

Taxa padrão 1 Hz (TELEMETRY_HZ) - mantenha baixa para respeitar o limite de mensagens do plano do CloudAMQP.
Variáveis: MQTT_HOST, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD, MQTT_TLS, MQTT_TOPIC_STATUS, TELEMETRY_HZ, UR_ROBOT_IP.
"""
import os
import json
import time
import socket
import logging

import paho.mqtt.client as mqtt

from ur_rtde_telemetry import URTelemetryClient

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("telemetry-mqtt")

MQTT_HOST = os.getenv("MQTT_HOST", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", 8883))
MQTT_USERNAME = os.getenv("MQTT_USERNAME")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD")
MQTT_TLS = os.getenv("MQTT_TLS", "auto")
TOPIC_STATUS = os.getenv("MQTT_TOPIC_STATUS", "smart40/n2/cobot/status")
HZ = float(os.getenv("TELEMETRY_HZ", 1))


def main():
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2,
                         client_id=f"edge-telemetry-{socket.gethostname()}",
                         protocol=mqtt.MQTTv311, clean_session=True)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    if MQTT_TLS == "1" or (MQTT_TLS == "auto" and MQTT_PORT == 8883):
        client.tls_set()
    client.reconnect_delay_set(1, 30)
    client.connect_async(MQTT_HOST, MQTT_PORT, keepalive=60)
    client.loop_start()

    robot = URTelemetryClient()
    robot.connect()
    logger.info(f"Publicando telemetria em '{TOPIC_STATUS}' a {HZ} Hz (Ctrl+C para sair)")
    try:
        while True:
            data = robot.read_telemetry()
            data["source"] = "UR_RTDE" if robot.connected else "SIMULATED"
            client.publish(TOPIC_STATUS, json.dumps(data), qos=0)
            time.sleep(1.0 / HZ)
    except KeyboardInterrupt:
        pass
    finally:
        robot.close()
        client.loop_stop()
        client.disconnect()


if __name__ == "__main__":
    main()
