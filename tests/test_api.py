import os
import sys
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

import cobot_service_app as api  # noqa: E402

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
        assert client.get("/api/v1/cobot/status", headers=token(role)).status_code == 200
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
    assert len(broker) == 1
    assert broker[0]["task"]["action"] == "MOVE_TO_ASSEMBLY"
    routing_key, evento = broker[0]["mqtt"][0]
    assert routing_key == "smart40.n2.cobot.inspection"
    assert evento["resultado"] == "INSPECTED_OK" and evento["order_id"] == "OP-A1"


def test_inspection_nok_enfileira_descarte_rotina_8(broker):
    r = client.post("/api/v1/cobot/inspection",
                    json=body(tag="RFID-SMART40-2025-999-Z", order="OP-A2", status="INSPECTED_NOK"),
                    headers=token("ENGINEER"))
    assert r.status_code == 200 and r.json()["action_taken"] == "SCRAP_ROUTINE_8"
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
    r = client.post("/api/v1/cobot/routines",
                    json={"routine_id": 9, "routine_name": "Montagem", "order_id": "OP-R"},
                    headers=token("ENGINEER"))
    assert r.status_code == 503


def test_routines_202(broker):
    r = client.post("/api/v1/cobot/routines",
                    json={"routine_id": 9, "routine_name": "Montagem", "order_id": "OP-R"},
                    headers=token("ENGINEER"))
    assert r.status_code == 202 and r.json()["status"] == "QUEUED"


def test_health_publico():
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["database"] == "sqlite"
