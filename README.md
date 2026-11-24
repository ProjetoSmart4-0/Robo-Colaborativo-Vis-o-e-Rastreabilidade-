## 🤖 Microsserviço `cobot-service` (Sprint 3)
* **Endpoints**: Suporte a rotinas de montagem/descarte, consulta de status e histórico de inspeções RFID [7].
* **Real-time SSE**: Transmissão nativa em tempo real para a IHM Web via `GET /api/cobot/events/stream` [9, 12].
* **Persistência e Idempotência**: Gravação garantida via `storage.py` (SQLite / Neon Postgres) [8].
* **Testes Automatizados**: Suíte PyTest em `tests/test_api.py` validando autenticação, RBAC e payloads [3].