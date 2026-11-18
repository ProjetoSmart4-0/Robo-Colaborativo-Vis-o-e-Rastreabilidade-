import sys
from pathlib import Path
import pytest
from fastapi.testclient import TestClient
import jwt

# Define os caminhos da raiz e das subpastas do projeto
BASE_DIR = Path(__file__).resolve().parent.parent

# Adiciona as pastas onde a API pode estar localizada no sys.path
sys.path.insert(0, str(BASE_DIR))
sys.path.insert(0, str(BASE_DIR / "services" / "cobot-services"))
sys.path.insert(0, str(BASE_DIR / "arquivosPy"))

# Agora importa a API
from cobot_service_app_v2 import app

client = TestClient(app)

JWT_SECRET = "chave_secreta_industria_40_super_segura"

def gerar_token(role: str):
    """Função auxiliar para gerar tokens de teste."""
    payload = {"sub": "thiago_test", "role": role}
    return jwt.encode(payload, JWT_SECRET, algorithm="HS256")

# Teste 1: Rota sem token deve retornar 401 Unauthorized
def test_inspection_sem_token():
    response = client.post("/api/v1/cobot/inspection", json={
        "piece_rfid_tag": "RFID-TEST-01",
        "order_id": "OP-TEST",
        "inspection_status": "INSPECTED_OK"
    })
    assert response.status_code == 401

# Teste 2: Perfil sem permissão (OPERATOR em rota de ENGINEER) deve retornar 403 Forbidden
def test_inspection_permissao_negada():
    token_operador = gerar_token("OPERATOR")
    headers = {"Authorization": f"Bearer {token_operador}"}
    response = client.post("/api/v1/cobot/inspection", json={
        "piece_rfid_tag": "RFID-TEST-01",
        "order_id": "OP-TEST",
        "inspection_status": "INSPECTED_OK"
    }, headers=headers)
    assert response.status_code == 403

# Teste 3: Perfil ENGINEER autorizado deve retornar 200 OK
def test_inspection_sucesso_engineer():
    token_engenheiro = gerar_token("ENGINEER")
    headers = {"Authorization": f"Bearer {token_engenheiro}"}
    response = client.post("/api/v1/cobot/inspection", json={
        "piece_rfid_tag": "RFID-TEST-02",
        "order_id": "OP-TEST-02",
        "inspection_status": "INSPECTED_OK",
        "antenna_id": "ANT-MONTAGEM-01"
    }, headers=headers)
    assert response.status_code == 200
    assert response.json()["piece_rfid_tag"] == "RFID-TEST-02"

# Teste 4: Payload inválida deve retornar 422 Unprocessable Entity
def test_inspection_payload_invalida():
    token_engenheiro = gerar_token("ENGINEER")
    headers = {"Authorization": f"Bearer {token_engenheiro}"}
    response = client.post("/api/v1/cobot/inspection", json={
        "campo_errado": "valor"
    }, headers=headers)
    assert response.status_code == 422