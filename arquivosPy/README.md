# Guia Técnico e Operacional dos Módulos Python — Grupo 2: Cobot, RFID e Telemetria

Este documento apresenta a especificação técnica e o funcionamento operacional de cada módulo em linguagem Python desenvolvido para o **Grupo 2 (Cobot, Sensoriamento RFID, Mensageria RabbitMQ e Telemetria)** da **Planta N2** no projeto integrado **SMART 4.0**.

A arquitetura adota o modelo híbrido Borda-Nuvem (*Edge-Cloud*), separando de forma estrita o ambiente operacional de chão de fábrica (OT) dos serviços corporativos em nuvem (IT). Essa estrutura coordena o braço robótico colaborativo Universal Robots UR3e, a garra elétrica inteligente SCHUNK EGH, a leitura de identificadores por radiofrequência (RFID) das peças (*SmartBlocks*) e o tráfego telemétrico de alta frequência destinado ao Gêmeo Digital.

---

## 1. Visão Geral da Arquitetura e Comunicação

O fluxo de dados entre os componentes foi estruturado para assegurar baixa latência, rastreabilidade ponta a ponta e segurança cibernética:

| Módulo Python | Protocolo / Porta | Camada de Rede | Papel e Responsabilidade Principal |
| :--- | :--- | :--- | :--- |
| `cobot_service_app-v2.py` | HTTPS / REST (`8080`) | Nuvem / Servidor IT | Microsserviço de controle com autenticação JWT e RBAC; produtor de tarefas para o RabbitMQ |
| `cobot_worker_consumer-v2.py` | AMQP (`5672`) / TCP (`29999`) | Borda / Gateway OT | Consumidor de tarefas da fila; interface de despacho de comandos para o PolyScope do robô UR3e |
| `rfid_inspection.py` | TCP/Serial (`4001`) / HTTPS | Borda / Inspeção | Agente de leitura de TAGs RFID, validação de conformidade e envio de laudo de qualidade |
| `rfid_reader_test.py` | TCP Loopback (`127.0.0.1:4001`) | Borda / Diagnóstico | Script de homologação para validação de bancada na Sprint 1 com emulador sintético integrado |
| `ur_rtde_telemetry.py` | RTDE TCP (`30004`) | Borda OT Local | Coletor de telemetria contínua das 6 juntas e da garra SCHUNK EGH a 125 Hz |

---

## 2. Funcionamento de Cada Módulo Python

### `cobot_service_app-v2.py` — API REST e Microsserviço de Controle
* **Objetivo**: Disponibilizar os pontos de extremidade (endpoints) HTTP da célula robótica para a aplicação web e outros serviços, desacoplando requisições externas do chão de fábrica.
* **Funcionamento e Mecanismos**:
  * Construído com o framework **FastAPI**, utilizando modelos **Pydantic** para validação estrita de esquemas de dados.
  * **Controle de Acesso Baseado em Papéis (RBAC)**: Valida tokens JWT via cabeçalho HTTP Bearer. Usuários com papel `OPERATOR` possuem permissão de leitura em `GET /api/v1/cobot/status`. Ações que alteram estado, como agendamento de tarefas (`POST /api/v1/cobot/routines`) e registro de inspeções (`POST /api/v1/cobot/inspection`), exigem perfil `ENGINEER` ou `ADMIN`.
  * **Produtor RabbitMQ**: Ao receber uma ordem de produção ou resultado de inspeção, serializa o payload em JSON e publica a mensagem de forma persistente (`delivery_mode=2`) na fila durável `cobot_pick_place_queue`.
* **Entradas e Saídas**: Recebe requisições HTTP REST; despacha mensagens para a fila AMQP e retorna respostas com status da operação em formato JSON.

---

### `cobot_worker_consumer-v2.py` — Consumidor de Tarefas e Despachante PolyScope
* **Objetivo**: Processo de segundo plano (*worker*) executado localmente na borda para converter mensagens assíncronas em comandos físicos executados pelo robô.
* **Funcionamento e Mecanismos**:
  * Mantém conexão ativa com o RabbitMQ na porta `5672` utilizando a biblioteca `pika`.
  * Opera com confirmação manual de processamento (`manual ACK/NACK`) e pré-carregamento unitário (`prefetch_count=1`), garantindo que falhas de execução mantenham a mensagem na fila para reprocessamento.
  * **Interface com o Robô**: Estabelece comunicação via socket TCP com o *Dashboard Server* do PolyScope do robô UR3e na porta industrial `29999`. Ao identificar a ação `MOVE_TO_ASSEMBLY`, aciona a Rotina de Montagem (Rotina 9); ao receber `MOVE_TO_SCRAP`, aciona a Rotina de Descarte/Refugo (Rotina 8).
* **Entradas e Saídas**: Consome eventos da fila `cobot_pick_place_queue`; envia comandos de controle e início de programa ao PolyScope do robô.

---

### `rfid_inspection.py` — Agente de Leitura e Validação de Rastreabilidade RFID
* **Objetivo**: Responsável pela leitura automática da TAG RFID acoplada à base dos produtos (*SmartBlocks*) que chegam à bancada de montagem.
* **Funcionamento e Mecanismos**:
  * Conecta-se à antena RFID instalada na estação (canal C/Q1 da bancada) via interface de rede ou serial para extrair o UID e os dados gravados na TAG.
  * **Validação de Regras de Negócio**:
    1. Verifica a intensidade do sinal eletromagnético (RSSI >= -85 dBm).
    2. Valida o formato do identificador contra a expressão regular padrão da fábrica (`^RFID-SMART40-\d{4}-\d{3}-[A-Z0-9]+$`).
    3. Realiza a validação cruzada entre o identificador da TAG e a Ordem de Produção (OP) em andamento.
  * Gera a classificação de qualidade: `INSPECTED_OK` (peça aprovada) ou `INSPECTED_NOK` (peça rejeitada ou de lote divergente).
  * Notifica a API `cobot-service` via HTTPS POST, registrando a ocorrência no histórico de rastreabilidade da tabela `pieces_traceability`.
* **Entradas e Saídas**: Captura dados eletromagnéticos da antena; emite requisição HTTPS autenticada para o microsserviço de controle.

---

### `rfid_reader_test.py` — Script de Homologação e Diagnóstico de Bancada (Sprint 1)
* **Objetivo**: Ferramenta de verificação rápida e auditoria de ambiente local para entrega das metas iniciais da Sprint 1.
* **Funcionamento e Mecanismos**:
  * Executa a tentativa de conexão com a antena RFID na porta de comunicação local `4001`.
  * Caso o hardware físico não esteja conectado, ativa automaticamente um emulador de TAGs com perfis válidos e inválidos, permitindo simular o fluxo de esteira e testes unitários sem dependência física.
  * **Isolamento de Segurança**: Realiza testes da camada lógica sem emitir nenhum comando de acionamento aos motores ou juntas do robô colaborativo UR3e, respeitando o regulamento de segurança da Sprint 1.
* **Entradas e Saídas**: Exibe no terminal o diagnóstico detalhado da comunicação, força de sinal RSSI e conformidade das TAGs testadas.

---

### `ur_rtde_telemetry.py` — Cliente de Telemetria Industrial RTDE (125 Hz)
* **Objetivo**: Efetuar a ingestão de telemetria física do robô colaborativo e da garra inteligente em tempo real para sincronização com o Gêmeo Digital.
* **Funcionamento e Mecanismos**:
  * Estabelece conexão via socket TCP *non-blocking* diretamente com a controladora do robô UR3e na porta industrial `30004`, utilizando o protocolo **RTDE (Real-Time Data Exchange)** em ciclo fechado de 125 Hz.
  * Extrai a telemetria angular das 6 juntas mecânicas (`base`, `shoulder`, `elbow`, `wrist_1`, `wrist_2`, `wrist_3`) e a velocidade vetorial do Tool Center Point (TCP).
  * Monitora os sinais de estado da garra elétrica SCHUNK EGH integrados via módulo mestre IO-Link (porta P1 do módulo TURCK FEN20-4IOL), lendo registradores de referência e preensão (`IOL_get_state`, `IOL_is_referenced`, `IOL_is_success`) e comandando a faixa de LED de sinalização luminosa (Verde para preensão concluída com sucesso e Vermelho para falha).
  * Inclui gerador de telemetria cinemática simulada para viabilizar desenvolvimento com a controladora desligada.
* **Entradas e Saídas**: Recebe pacotes binários RTDE do controlador; formata os dados em estruturas JSON prontas para persistência temporal e renderização na interface web.

---

### Módulos de Referência Baseados em Visão Computacional (Baseline Inicial)
Para fins de rastreabilidade histórica do projeto, o repositório mantém documentadas as versões que utilizavam câmeras para decodificação visual:
* `vision_inspection.py`: Agente de visão computacional desenvolvido com a biblioteca OpenCV para decodificação de QR Codes sintéticos.
* `vision_qr_test.py`: Script de teste para validação de foco, iluminação e delimitação de polígonos de QR Code em janela de visualização em tempo real.
* `cobot_service_app.py` e `cobot_worker_consumer.py` (v1): Versões iniciais dos contratos REST e AMQP mapeadas para identificadores por QR Code.

---

## 3. Dependências e Ordem de Inicialização

### Dependências Principais
Para executar os módulos, instale os pacotes necessários no ambiente virtual Python:
```bash
pip install fastapi uvicorn pika pydantic pyjwt requests
```

### Sequência Operacional Recomendada
1. **Subir o Barramento RabbitMQ (Docker)**:
   ```bash
   cd infra/local/cobot
   docker compose up -d rabbitmq
   ```
2. **Iniciar o Microsserviço de Controle**:
   ```bash
   python cobot_service_app-v2.py
   ```
3. **Iniciar o Consumidor da Borda (Worker)**:
   ```bash
   python cobot_worker_consumer-v2.py
   ```
4. **Iniciar a Coleta de Telemetria RTDE**:
   ```bash
   python ur_rtde_telemetry.py
   ```
5. **Executar a Inspeção de Rastreabilidade**:
   ```bash
   python rfid_inspection.py
   ```
