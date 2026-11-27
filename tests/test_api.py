import os
import sys
import json
import tempfile
from pathlib import Path

# Ambiente de teste ANTES de importar a API (banco temporário, sem broker real)
JWT_SECRET = "chave_secreta_industria_40_super_segura"
os.environ["JWT_SECRET"] = JWT_SECRET
os.environ["SQLITE_PATH"] = str(Path(tempfile.mkdtemp()) / "test.db")
os.environ.pop("DATABASE_URL", None)

import jwt  # noqa: E402
import pytest  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "services" / "cobot-services"))
sys.path.insert(0, str(BASE_DIR / "edge"))

import cobot_service_app as api  # noqa: E402
from ur_rtde_telemetry import URTelemetryClient  # noqa: E402

client = TestClient(api.app)


def token(role: str) -> dict:
    t = jwt.encode({"sub": "teste", "role": role}, JWT_SECRET, algorithm="HS256")
    return {"Authorization": f"Bearer {t}"}


@pytest.fixture
def broker(monkeypatch):
    """Substitui a publicação AMQP por um gravador (sem RabbitMQ). Retorna a lista de chamadas."""
    calls = []

    def fake_publish(task_msg=None, mqtt_events=None):
        calls.append({"task": task_msg, "mqtt": mqtt_events})
        return True

    monkeypatch.setattr(api, "amqp_publish", fake_publish)
    return calls


def body(tag="RFID-SMART40-2026-001-A", order="OP-T", status="INSPECTED_OK", **extra):
    return {"piece_rfid_tag": tag, "order_id": order, "inspection_status": status, **extra}


# --- Segurança -------------------------------------------------------------
def test_inspection_sem_token_401():
    assert client.post("/api/v1/cobot/inspection", json=body()).status_code == 401


def test_inspection_token_invalido_401():
    r = client.post("/api/v1/cobot/inspection", json=body(), headers={"Authorization": "Bearer lixo"})
    assert r.status_code == 401


def test_inspection_operator_403():
    assert client.post("/api/v1/cobot/inspection", json=body(), headers=token("OPERATOR")).status_code == 403


def test_status_aceita_operator_engineer_admin():
    for role in ("OPERATOR", "ENGINEER", "ADMIN"):
        r = client.get("/api/v1/cobot/status", headers=token(role))
        assert r.status_code == 200
        data = r.json()
        assert data["gripper"]["model"] == "SCHUNK EGH80"
        assert "position_mm" in data["gripper"]
        assert "opening_percentage" in data["gripper"]
        assert "force_percentage" in data["gripper"]
        assert "led_color" in data["gripper"]
        assert len(data["joints_position_deg"]) == 6
    assert client.get("/api/v1/cobot/status").status_code == 401


def test_payload_invalido_422():
    r = client.post("/api/v1/cobot/inspection", json={"campo_errado": "x"}, headers=token("ENGINEER"))
    assert r.status_code == 422


def test_status_de_inspecao_invalido_422():
    r = client.post("/api/v1/cobot/inspection", json=body(status="TALVEZ"), headers=token("ENGINEER"))
    assert r.status_code == 422


# --- Fluxo de inspeção ------------------------------------------------------
def test_inspection_ok_enfileira_montagem_e_publica_mqtt(broker):
    r = client.post("/api/v1/cobot/inspection", json=body(order="OP-A1"), headers=token("ENGINEER"))
    assert r.status_code == 200
    j = r.json()
    assert j["action_taken"] == "ASSEMBLY" and j["queued"] is True
    assert j["gripper"]["led_color"] == "GREEN"
    assert len(broker) == 1
    assert broker[0]["task"]["action"] == "MOVE_TO_ASSEMBLY"
    routing_key, evento = broker[0]["mqtt"][0]
    assert routing_key == "smart40.n2.cobot.inspection"
    assert evento["resultado"] == "INSPECTED_OK" and evento["order_id"] == "OP-A1"


def test_inspection_nok_enfileira_descarte_rotina_8(broker):
    r = client.post(
        "/api/v1/cobot/inspection",
        json=body(tag="RFID-SMART40-2025-999-Z", order="OP-A2", status="INSPECTED_NOK"),
        headers=token("ENGINEER"),
    )
    assert r.status_code == 200 and r.json()["action_taken"] == "SCRAP_ROUTINE_8"
    assert r.json()["gripper"]["led_color"] == "RED"
    assert broker[0]["task"]["action"] == "MOVE_TO_SCRAP" and broker[0]["task"]["routine_id"] == 8


def test_idempotencia_nao_reaciona_o_robo(broker):
    h = token("ENGINEER")
    b = body(order="OP-DUP", correlation_id="evt-123")
    assert client.post("/api/v1/cobot/inspection", json=b, headers=h).json().get("duplicate") is None
    r2 = client.post("/api/v1/cobot/inspection", json=b, headers=h)
    assert r2.status_code == 200 and r2.json()["duplicate"] is True
    assert len(broker) == 1  # só a primeira chamada enfileirou


def test_broker_offline_inspecao_continua_gravada(monkeypatch):
    monkeypatch.setattr(api, "amqp_publish", lambda **kw: False)
    r = client.post("/api/v1/cobot/inspection", json=body(order="OP-OFF"), headers=token("ENGINEER"))
    assert r.status_code == 200 and r.json()["queued"] is False
    hist = client.get("/api/v1/cobot/inspections?limit=50").json()
    assert any(h["correlation_id"].endswith("OP-OFF") for h in hist)


def test_routines_broker_offline_503(monkeypatch):
    monkeypatch.setattr(api, "amqp_publish", lambda **kw: False)
    r = client.post(
        "/api/v1/cobot/routines",
        json={"routine_id": 9, "routine_name": "Montagem", "order_id": "OP-R"},
        headers=token("ENGINEER"),
    )
    assert r.status_code == 503


def test_routines_202(broker):
    r = client.post(
        "/api/v1/cobot/routines",
        json={"routine_id": 9, "routine_name": "Montagem", "order_id": "OP-R"},
        headers=token("ENGINEER"),
    )
    assert r.status_code == 202 and r.json()["status"] == "QUEUED"
    assert r.json()["register_1"] == 9


def test_health_publico():
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["database"] == "sqlite"
    assert r.json()["digital_twin_gripper"] == "SCHUNK EGH80"


# --- Gêmeo Digital da Garra SCHUNK EGH80 (Sprint 3) -------------------------
def test_gripper_commands_grip_release_position(broker):
    h = token("ENGINEER")

    # 1. EGH Release (abrir para 80 mm)
    r_rel = client.post(
        "/api/v1/cobot/gripper/command",
        json={"command": "EGH Release", "order_id": "OP-2026-001", "piece_rfid_tag": "RFID-SMART40-2026-001-A"},
        headers=h,
    )
    assert r_rel.status_code == 200
    g_rel = r_rel.json()["gripper"]
    assert g_rel["state"] == "OPEN"
    assert g_rel["position_mm"] == 80.0
    assert g_rel["opening_percentage"] == 100.0
    assert g_rel["led_color"] == "BLUE"

    # 2. EGH Position (posicionar em 50% = 40.0 mm)
    r_pos = client.post(
        "/api/v1/cobot/gripper/command",
        json={"command": "EGH Position", "opening_percentage": 50.0, "force_percentage": 60},
        headers=h,
    )
    assert r_pos.status_code == 200
    g_pos = r_pos.json()["gripper"]
    assert g_pos["position_mm"] == 40.0
    assert g_pos["opening_percentage"] == 50.0
    assert g_pos["force_percentage"] == 60

    # 3. EGH Grip (fechar segurando peça = HOLDING, LED GREEN)
    r_grip = client.post(
        "/api/v1/cobot/gripper/command",
        json={
            "command": "EGH Grip",
            "force_percentage": 85,
            "simulate_piece_holding": True,
            "piece_rfid_tag": "RFID-SMART40-2026-001-A",
        },
        headers=h,
    )
    assert r_grip.status_code == 200
    g_grip = r_grip.json()["gripper"]
    assert g_grip["state"] == "HOLDING"
    assert g_grip["force_percentage"] == 85
    assert g_grip["led_color"] == "GREEN"
    assert g_grip["piece_detected"] is True

    # 4. EGH Grip sem peça (fechamento completo = CLOSED, LED YELLOW)
    r_closed = client.post(
        "/api/v1/cobot/gripper/command",
        json={"command": "EGH Grip", "simulate_piece_holding": False},
        headers=h,
    )
    assert r_closed.status_code == 200
    assert r_closed.json()["gripper"]["state"] == "CLOSED"

    # Verifica que todas as ações da garra foram persistidas em pieces_traceability
    hist = client.get("/api/v1/cobot/inspections?limit=20").json()
    assert any(item.get("source") == "SCHUNK_EGH80_TWIN" and item.get("gripper_action") for item in hist)


def test_gripper_operator_forbidden():
    r = client.post(
        "/api/v1/cobot/gripper/command",
        json={"command": "EGH Grip"},
        headers=token("OPERATOR"),
    )
    assert r.status_code == 403


def test_telemetry_ingest_and_sse_stream():
    # Ingestão de pacote RTDE vindo da borda
    r_ing = client.post(
        "/api/v1/cobot/telemetry",
        json={
            "robot_id": "UR3e-MONT-01",
            "telemetry_source": "UR_RTDE",
            "register_1": 9,
            "joints_position_deg": [10.0, -85.0, 15.0, -95.0, 90.0, 5.0],
            "tcp_speed_m_s": 0.21,
            "gripper": {
                "state": "HOLDING",
                "position_mm": 18.5,
                "force_percentage": 80,
                "led_color": "GREEN",
            },
        },
        headers=token("ENGINEER"),
    )
    assert r_ing.status_code == 200
    assert r_ing.json()["telemetry_source"] == "UR_RTDE"

    # Stream SSE de telemetria com max_events=1 para validar payload instantaneamente
    with client.stream("GET", "/api/v1/cobot/telemetry/stream?max_events=1") as resp:
        assert resp.status_code == 200
        lines = [line for line in resp.iter_lines() if line]
        assert any("event: telemetry" in line for line in lines)
        data_lines = [line[6:] for line in lines if line.startswith("data: ")]
        assert len(data_lines) == 1
        payload = json.loads(data_lines[0])
        assert payload["gripper"]["model"] == "SCHUNK EGH80"
        assert payload["joints_position_deg"] == [10.0, -85.0, 15.0, -95.0, 90.0, 5.0]


def test_ur_rtde_telemetry_replay_states():
    """Garante que o driver de borda em modo replay percorre todos os estados do Gêmeo Digital."""
    rtde_client = URTelemetryClient(mode="replay")
    states_seen = set()
    for _ in range(10):
        pkt = rtde_client.read_telemetry()
        assert pkt["telemetry_source"] == "REPLAY"
        assert len(pkt["joints_position_deg"]) == 6
        states_seen.add(pkt["gripper"]["state"])
    assert {"OPEN", "MOVING", "HOLDING", "CLOSED"}.issubset(states_seen)
