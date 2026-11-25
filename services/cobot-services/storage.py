"""
storage.py - Persistência da rastreabilidade (tabela pieces_traceability).

- Sem DATABASE_URL  -> SQLite local (arquivo cobot_dev.db, ou o definido em SQLITE_PATH).
- Com DATABASE_URL  -> PostgreSQL (Neon). Requer `psycopg[binary]` instalado.

No Render o disco é efêmero: SEM DATABASE_URL os dados somem a cada deploy/restart.
O esquema da tabela é o mesmo do SQLite original, para não quebrar o MES (Grupo 3).
"""
import os
import sqlite3
import logging
from contextlib import contextmanager

logger = logging.getLogger("cobot-storage")

DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
SQLITE_PATH = os.getenv("SQLITE_PATH", "cobot_dev.db")
USE_POSTGRES = DATABASE_URL.startswith(("postgres://", "postgresql://"))

if DATABASE_URL and not USE_POSTGRES:
    logger.warning("DATABASE_URL definida mas não começa com postgres:// ou postgresql:// - usando SQLite.")

BACKEND = "postgres" if USE_POSTGRES else "sqlite"
PH = "%s" if USE_POSTGRES else "?"  # placeholder de parâmetro


@contextmanager
def _connect():
    if USE_POSTGRES:
        import psycopg
        from psycopg.rows import dict_row
        conn = psycopg.connect(DATABASE_URL, row_factory=dict_row, connect_timeout=10)
    else:
        conn = sqlite3.connect(SQLITE_PATH)
        conn.row_factory = sqlite3.Row
    try:
        yield conn
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def inicializar_banco():
    """Cria a tabela de rastreabilidade se não existir."""
    pk = "SERIAL PRIMARY KEY" if USE_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"
    with _connect() as conn:
        conn.execute(f"""
            CREATE TABLE IF NOT EXISTS pieces_traceability (
                id {pk},
                correlation_id TEXT UNIQUE NOT NULL,
                piece_qr TEXT NOT NULL,
                status TEXT NOT NULL,
                quality_score REAL,
                source TEXT,
                event_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)


def salvar_inspecao(evento: dict) -> bool:
    """
    Grava a inspeção de forma idempotente pelo correlation_id.
    Retorna True se inseriu, False se o correlation_id já existia (duplicata).
    """
    with _connect() as conn:
        cur = conn.execute(
            f"""
            INSERT INTO pieces_traceability (correlation_id, piece_qr, status, quality_score, source)
            VALUES ({PH}, {PH}, {PH}, {PH}, {PH})
            ON CONFLICT (correlation_id) DO NOTHING;
            """,
            (
                evento.get("correlation_id"),
                evento.get("qrCode"),
                evento.get("resultado"),
                evento.get("pontuacaoQualidade"),
                evento.get("fonteLeitura", "RFID"),
            ),
        )
        return cur.rowcount == 1


def listar_inspecoes(limit: int = 10):
    """Retorna as inspeções mais recentes (desempate por id, pois event_ts tem resolução de 1 s)."""
    with _connect() as conn:
        cur = conn.execute(
            f"SELECT * FROM pieces_traceability ORDER BY event_ts DESC, id DESC LIMIT {PH};",
            (limit,),
        )
        return [dict(r) for r in cur.fetchall()]


# Cria a tabela ao importar o módulo
inicializar_banco()
