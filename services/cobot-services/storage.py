import os
import sqlite3

DATABASE_URL = os.getenv("DATABASE_URL")

def get_connection():
    # Se DATABASE_URL existir (no Render), pode conectar ao Postgres
    # Para o seu teste local no VS Code, utiliza o SQLite local cobot_dev.db
    conn = sqlite3.connect("cobot_dev.db")
    conn.row_factory = sqlite3.Row
    return conn

def inicializar_banco():
    """Cria a tabela de rastreabilidade se não existir."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pieces_traceability (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                correlation_id TEXT UNIQUE NOT NULL,
                piece_qr TEXT NOT NULL,
                status TEXT NOT NULL,
                quality_score REAL,
                source TEXT,
                event_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        conn.commit()

def salvar_inspecao(evento: dict):
    """Salva a inspeção garantindo idempotência pelo correlation_id."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO pieces_traceability (correlation_id, piece_qr, status, quality_score, source)
            VALUES (?, ?, ?, ?, ?)
            ON CONFLICT(correlation_id) DO NOTHING;
        """, (
            evento.get("correlation_id"),
            evento.get("qrCode"),
            evento.get("resultado"),
            evento.get("pontuacaoQualidade"),
            evento.get("fonteLeitura", "RFID")
        ))
        conn.commit()

def listar_inspecoes(limit: int = 10):
    """Retorna as inspeções mais recentes."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute(
            "SELECT * FROM pieces_traceability ORDER BY event_ts DESC LIMIT ?;", 
            (limit,)
        )
        return [dict(row) for row in cursor.fetchall()]

# Inicializa a tabela assim que o arquivo é carregado
inicializar_banco()