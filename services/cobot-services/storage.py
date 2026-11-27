"""
storage.py - Persistência da rastreabilidade (tabela pieces_traceability).

- Sem DATABASE_URL  -> SQLite local (arquivo cobot_dev.db, ou o definido em SQLITE_PATH).
- Com DATABASE_URL  -> PostgreSQL (Neon). Requer `psycopg[binary]` instalado.

No Render o disco é efêmero: SEM DATABASE_URL os dados somem a cada deploy/restart.
Inclui fallback seguro caso a tabela pieces_traceability no Neon Postgres tenha sido
criada pelo Grupo 3 sem as colunas estendidas (order_id, gripper_action).
"""
import os
import sqlite3
import logging
from contextlib import contextmanager
from typing import Optional, Dict, Any

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
    """Cria a tabela de rastreabilidade se não existir e tenta adicionar colunas do Gêmeo Digital."""
    pk = "SERIAL PRIMARY KEY" if USE_POSTGRES else "INTEGER PRIMARY KEY AUTOINCREMENT"
    try:
        with _connect() as conn:
            conn.execute(f"""
                CREATE TABLE IF NOT EXISTS pieces_traceability (
                    id {pk},
                    correlation_id TEXT UNIQUE NOT NULL,
                    piece_qr TEXT NOT NULL,
                    status TEXT NOT NULL,
                    quality_score REAL,
                    source TEXT,
                    order_id TEXT,
                    gripper_action TEXT,
                    event_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                );
            """)
        for col_def in ("order_id TEXT", "gripper_action TEXT"):
            try:
                with _connect() as conn:
                    conn.execute(f"ALTER TABLE pieces_traceability ADD COLUMN {col_def};")
            except Exception:
                pass
    except Exception as e:
        logger.warning(f"Aviso ao inicializar/migrar schema do banco ({BACKEND}): {e}")


def verificar_conexao() -> Dict[str, Any]:
    """Testa a conexão ativa com o banco (Neon Postgres ou SQLite) e retorna diagnóstico."""
    try:
        with _connect() as conn:
            cur = conn.execute("SELECT COUNT(*) AS total FROM pieces_traceability;")
            row = cur.fetchone()
            total = row["total"] if isinstance(row, dict) or hasattr(row, "keys") else row[0]
            return {"connected": True, "backend": BACKEND, "records_count": int(total)}
    except Exception as e:
        return {"connected": False, "backend": BACKEND, "error": str(e)}


def salvar_inspecao(evento: dict) -> bool:
    """
    Grava a inspeção ou evento de rastreabilidade da garra de forma idempotente pelo correlation_id.
    Tenta gravar com as colunas estendidas (order_id, gripper_action) e, se a tabela do Grupo 3
    no Neon tiver apenas as 5 colunas originais, faz fallback automático sem perder o registro.
    """
    correlation_id = evento.get("correlation_id")
    piece_qr = evento.get("qrCode") or evento.get("piece_rfid_tag") or "GRIPPER-ACTION"
    status_val = evento.get("resultado") or evento.get("status") or "INSPECTED_OK"
    quality_score = evento.get("pontuacaoQualidade", 100.0)
    source_val = evento.get("fonteLeitura", "RFID")
    order_id = evento.get("order_id")
    gripper_action = evento.get("gripper_action") or evento.get("acao")

    try:
        with _connect() as conn:
            cur = conn.execute(
                f"""
                INSERT INTO pieces_traceability (
                    correlation_id, piece_qr, status, quality_score, source, order_id, gripper_action
                )
                VALUES ({PH}, {PH}, {PH}, {PH}, {PH}, {PH}, {PH})
                ON CONFLICT (correlation_id) DO NOTHING;
                """,
                (correlation_id, piece_qr, status_val, quality_score, source_val, order_id, gripper_action),
            )
            return cur.rowcount == 1
    except Exception as e:
        logger.warning(f"Fallback para schema original de 5 colunas em pieces_traceability ({e})")
        encoded_source = f"{source_val}|{order_id or ''}|{gripper_action or ''}".strip("|")
        with _connect() as conn:
            cur = conn.execute(
                f"""
                INSERT INTO pieces_traceability (correlation_id, piece_qr, status, quality_score, source)
                VALUES ({PH}, {PH}, {PH}, {PH}, {PH})
                ON CONFLICT (correlation_id) DO NOTHING;
                """,
                (correlation_id, piece_qr, status_val, quality_score, encoded_source),
            )
            return cur.rowcount == 1


def listar_inspecoes(limit: int = 10, status_filter: Optional[str] = None):
    """Retorna as inspeções mais recentes (desempate por id, pois event_ts tem resolução de 1 s)."""
    with _connect() as conn:
        if status_filter:
            cur = conn.execute(
                f"SELECT * FROM pieces_traceability WHERE status = {PH} ORDER BY event_ts DESC, id DESC LIMIT {PH};",
                (status_filter, limit),
            )
        else:
            cur = conn.execute(
                f"SELECT * FROM pieces_traceability ORDER BY event_ts DESC, id DESC LIMIT {PH};",
                (limit,),
            )
        return [dict(r) for r in cur.fetchall()]


# Cria/valida a tabela ao importar o módulo
inicializar_banco()
