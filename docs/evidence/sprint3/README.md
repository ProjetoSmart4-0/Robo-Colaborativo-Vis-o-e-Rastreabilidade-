# 📂 Evidências de Integração e Testes - Sprint 3 (Grupo 2 - Cobot)

**Responsável pelos testes da API e resiliência:** Thiago Berzin  
**Data da consolidação:** 23/11/2026  
**Status do Módulo:** APROVADO (PASS)

---

### 📊 Tabela de Evidências do Módulo `cobot-service`

| Arquivo | Tema / Teste | Resultado | Data | Versão | Responsável |
| :--- | :--- | :---: | :---: | :---: | :--- |
| `2026-11-23_pytest_PASS_v1.0.txt` | Autenticação JWT, Matriz RBAC (401/403) e Validação de Payload (422) | **PASS** | 23/11/2026 | v1.0 | Thiago |
| `2026-11-23_inspection_200_PASS_v1.0.txt` | Gravação de inspeção RFID com sucesso (200 OK) no SQLite/Neon | **PASS** | 23/11/2026 | v1.0 | Thiago |
| `2026-11-23_sse_streaming_PASS_v1.0.txt` | Transmissão de eventos em tempo real para a IHM (`ui_teste.html`) em < 2s | **PASS** | 23/11/2026 | v1.0 | Thiago |
| `2026-11-23_rabbitmq_resilience_PASS_v1.0.txt` | Teste de resiliência e fallback caso o broker RabbitMQ esteja offline | **PASS** | 23/11/2026 | v1.0 | Thiago |

---

### 🛡️ Matriz de Validação do Módulo de Cobot

* [x] **Autenticação JWT & RBAC**: Perfis sem token retornam `401 Unauthorized`; papel `OPERATOR` em rota restrita retorna `403 Forbidden` [6].
* [x] **Persistência de Inspeções**: O endpoint `POST /api/v1/cobot/inspection` grava o evento com a tag RFID e cria a entrada correspondente [7, 8].
* [x] **Streaming em Tempo Real**: Conexão SSE via `GET /api/cobot/events/stream` renderiza os eventos instantaneamente na página web [9].
* [x] **Resiliência e Desacoplamento**: Operação de inspeção gravada e transmitida com sucesso mesmo com fallbacks na mensageria [10, 11].