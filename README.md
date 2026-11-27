# 🤖 Planta N2 (SMART 4.0) — Automação Robótica, Gêmeo Digital SCHUNK EGH80, Visão/RFID e Rastreabilidade (Grupo 2)

Repositório oficial do **Grupo 2** para a Estação **SMART 4.0 (Planta N2)**. Integra o chão de fábrica (robô colaborativo **Universal Robots UR3e**, garra elétrica inteligente **SCHUNK EGH80** via IO-Link/FEN20 e leitura RFID/Visão) a uma arquitetura serverless em nuvem (**Render**, **CloudAMQP / RabbitMQ**, **Neon Postgres** e **Vercel**).

---

## 🚀 Destaques da Sprint 3: Gêmeo Digital (Digital Twin) da Garra SCHUNK EGH80 & UR3e

1. **Espelhamento Telemétrico em Tempo Real (`GET /api/v1/cobot/status` e `GET /api/v1/cobot/telemetry/stream`)**:
   - **Estado operacional da garra (`state`)**: `HOLDING` (segurando peça), `OPEN` (aberta), `CLOSED` (fechada sem peça) ou `MOVING` (dedos em transição).
   - **Força de preensão (`force_percentage`)**: `0%` a `100%`.
   - **Posição atual dos dedos (`position_mm` e `opening_percentage`)**: curso físico de `0.0 mm` a `80.0 mm` (`0%` a `100%`).
   - **Indicador visual de LED (`led_color`)**: `GREEN` (peça segura / aprovada), `RED` (reprovada / descarte Rotina 8), `BLUE` (aberta / em inspeção Rotina 7), `YELLOW` (em movimento / fechada vazia).
   - **Cinemática do UR3e**: posição angular das 6 juntas (`joints_position_deg`), pose cartesiana (`tcp_pose_m`), velocidade vetorial do TCP (`tcp_speed_m_s`) e registrador ativo (`register_1`: `7`, `8`, `9`, `10`, `11`).
2. **Despacho de Comandos Diretos do Gêmeo Digital**:
   - `POST /api/v1/cobot/gripper/command` (e `/api/cobot/gripper/command`): suporta `EGH Grip` (fechar), `EGH Release` (abrir) e `EGH Position` (ajuste proporcional em `mm` ou `%`).
   - Atalhos REST dedicados: `POST /api/v1/cobot/gripper/grip`, `POST /api/v1/cobot/gripper/release` e `POST /api/v1/cobot/gripper/position`.
   - Execução de rotinas associadas ao `Register 1` do PolyScope (`POST /api/v1/cobot/routines`).
3. **Modo Simulação & Replay Integrado**:
   - Quando a bancada física (`192.168.1.100:30004`) está offline, tanto a API (`cobot_service_app.py`) quanto o agente de borda (`ur_rtde_telemetry.py --mode replay`) sintetizam eventos telemétricos contínuos para que o frontend do Grupo 4 (Vercel) renderize o Gêmeo Digital sem interrupções.
4. **Persistência de Rastreabilidade (`pieces_traceability`)**:
   - Todas as inspeções (`INSPECTED_OK` / `INSPECTED_NOK`) e comandos acionados na garra são persistidos de forma idempotente no **Neon Postgres** (ou SQLite local em dev) com `correlation_id`, `order_id` e `gripper_action`.

---

## 🗺️ Rotas da API (`cobot-service`) e Compatibilidade com Proxy Vercel

Todas as rotas estão expostas tanto no prefixo versionado `/api/v1/cobot/*` quanto no prefixo `/api/cobot/*` (compatível com o `vercel.json` do Grupo 4) e contam com CORS habilitado:

| Método | Rota (`/api/v1/cobot/...` ou `/api/cobot/...`) | Perfil JWT (RBAC) | Descrição |
| :--- | :--- | :---: | :--- |
| `GET` | `/health` | Público | Saúde do serviço, banco ativo (`postgres`/`sqlite`) e status do Gêmeo Digital |
| `GET` | `/api/v1/cobot/status` | `OPERATOR`, `ENGINEER`, `ADMIN` | Snapshot completo do Gêmeo Digital do robô UR3e e garra SCHUNK EGH80 |
| `GET` | `/api/v1/cobot/gripper/status` | `OPERATOR`, `ENGINEER`, `ADMIN` | Estado detalhado exclusivo da garra SCHUNK EGH80 |
| `GET` | `/api/v1/cobot/telemetry/stream` | Público / `?access_token=` | **Streaming SSE** contínuo em tempo real da telemetria do Gêmeo Digital |
| `GET` | `/api/cobot/events/stream` | Público / `?access_token=` | **Streaming SSE** de eventos de inspeção RFID e comandos da garra |
| `POST` | `/api/v1/cobot/gripper/command` | `ENGINEER`, `ADMIN` | Despacha `EGH Grip`, `EGH Release` ou `EGH Position` para a garra |
| `POST` | `/api/v1/cobot/gripper/grip` | `ENGINEER`, `ADMIN` | Atalho para fechar a garra (`EGH Grip`) com força parametrizada |
| `POST` | `/api/v1/cobot/gripper/release` | `ENGINEER`, `ADMIN` | Atalho para abrir totalmente a garra (`EGH Release` -> 80 mm) |
| `POST` | `/api/v1/cobot/gripper/position` | `ENGINEER`, `ADMIN` | Atalho para ajuste proporcional da abertura (`position_mm` ou `opening_percentage`) |
| `POST` | `/api/v1/cobot/routines` | `ENGINEER`, `ADMIN` | Enfileira rotina robótica (`Register 1 = 7, 8, 9, 10, 11`) no RabbitMQ |
| `POST` | `/api/v1/cobot/inspection` | `ENGINEER`, `ADMIN` | Registra laudo `INSPECTED_OK`/`INSPECTED_NOK` no banco, SSE, MQTT e fila AMQP |
| `POST` | `/api/v1/cobot/telemetry` | `ENGINEER`, `ADMIN` | Ingestão de pacotes telemétricos vindos do agente de borda RTDE |
| `GET` | `/api/v1/cobot/inspections` | Público | Lista histórico de rastreabilidade gravado em `pieces_traceability` |

---

## 🧪 Exemplos de Requisição `curl` para Testar o Gêmeo Digital

### 1. Gerar um Token JWT de Teste (`ENGINEER`)
```bash
export JWT_SECRET="dev-only-secret-nao-usar-em-producao"
export TOKEN=$(python -c "import jwt, os; print(jwt.encode({'sub':'eng01','role':'ENGINEER'}, os.getenv('JWT_SECRET','dev-only-secret-nao-usar-em-producao'), algorithm='HS256'))")
export API_URL="http://localhost:8080"
```

### 2. Consultar o Estado Completo do Gêmeo Digital (`GET /api/v1/cobot/status`)
```bash
curl -s -H "Authorization: Bearer $TOKEN" "$API_URL/api/v1/cobot/status?simulate=true"
```

### 3. Comandar a Garra SCHUNK EGH80 — `EGH Grip` (Fechar segurando peça)
```bash
curl -X POST "$API_URL/api/v1/cobot/gripper/command" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "command": "EGH Grip",
    "force_percentage": 80,
    "simulate_piece_holding": true,
    "piece_rfid_tag": "RFID-SMART40-2026-001-A",
    "order_id": "OP-2026-001"
  }'
```

### 4. Comandar a Garra SCHUNK EGH80 — `EGH Release` (Abrir totalmente para 80 mm)
```bash
curl -X POST "$API_URL/api/v1/cobot/gripper/command" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "command": "EGH Release",
    "piece_rfid_tag": "RFID-SMART40-2026-001-A",
    "order_id": "OP-2026-001"
  }'
```

### 5. Comandar a Garra SCHUNK EGH80 — `EGH Position` (Ajuste proporcional de abertura)
```bash
curl -X POST "$API_URL/api/v1/cobot/gripper/command" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "command": "EGH Position",
    "position_mm": 36.0,
    "force_percentage": 60,
    "led_color": "YELLOW"
  }'
```

### 6. Escutar o Streaming SSE de Telemetria do Gêmeo Digital (`GET /api/v1/cobot/telemetry/stream`)
```bash
curl -N "$API_URL/api/v1/cobot/telemetry/stream?interval_s=0.5&max_events=5"
```

### 7. Disparar Rotina Robótica (`Register 1 = 9` — Montagem Lâmina 1)
```bash
curl -X POST "$API_URL/api/v1/cobot/routines" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "routine_id": 9,
    "routine_name": "Montagem_Lamina_1",
    "order_id": "OP-2026-001",
    "piece_rfid_tag": "RFID-SMART40-2026-001-A"
  }'
```

### 8. Registrar Inspeção RFID/Visão e Acionar Garra + Robô (`POST /api/v1/cobot/inspection`)
```bash
curl -X POST "$API_URL/api/v1/cobot/inspection" \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "piece_rfid_tag": "RFID-SMART40-2026-001-A",
    "order_id": "OP-2026-001",
    "inspection_status": "INSPECTED_OK",
    "antenna_id": "ANT-MONTAGEM-01",
    "quality_score": 98.5
  }'
```

---

## ☁️ Instruções de Execução Local e Deploy no Render

### Execução Local
1. **Instalar dependências e subir RabbitMQ local**:
   ```bash
   pip install -r requirements.txt pytest httpx
   docker compose -f infra/local/cobot/docker-compose.yml up -d
   ```
2. **Iniciar a API `cobot-service`**:
   ```bash
   python services/cobot-services/cobot_service_app.py
   ```
3. **Iniciar o Worker RabbitMQ e o Agente RTDE (Modo Replay)**:
   ```bash
   python edge/cobot_worker_consumer.py
   python edge/ur_rtde_telemetry.py --mode replay --push-api http://localhost:8080
   ```
4. **Executar a suíte de testes automatizados**:
   ```bash
   pytest -v
   ```

### Deploy no Render (Docker + CloudAMQP + Neon Postgres)
1. Conecte este repositório a um **Web Service** no **Render** com **Runtime: Docker** (o `Dockerfile` da raiz já inicia `uvicorn cobot_service_app:app --app-dir services/cobot-services --host 0.0.0.0 --port ${PORT:-8080}`).
2. Configure as variáveis de ambiente no painel do Render:
   - `JWT_SECRET`: segredo forte compartilhado com o gateway de autenticação e os agentes de borda.
   - `DATABASE_URL`: string de conexão PostgreSQL do **Neon** (`postgresql://user:pass@ep-...neon.tech/neondb?sslmode=require`).
   - `RABBITMQ_URL`: URL `amqps://...` da instância **CloudAMQP**.
   - `CORS_ORIGINS`: `*` (ou o domínio da aplicação do Grupo 4 na Vercel).
