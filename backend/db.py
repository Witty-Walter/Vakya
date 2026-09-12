"""
db.py — PostgreSQL database layer for Vakya
Uses psycopg2 with a threaded connection pool backed by Neon.
"""
import os
import json
import uuid
from contextlib import contextmanager
from dotenv import load_dotenv


load_dotenv(os.path.join(os.path.dirname(__file__), '..', '.env'))

import psycopg2
from psycopg2 import pool
from psycopg2.extras import RealDictCursor

_pool: pool.ThreadedConnectionPool | None = None


def get_pool() -> pool.ThreadedConnectionPool:
    global _pool
    if _pool is None:
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            raise RuntimeError("DATABASE_URL is not set. Add it to the root .env file.")
        _pool = pool.ThreadedConnectionPool(minconn=1, maxconn=10, dsn=database_url)
    return _pool


@contextmanager
def get_conn():
    p = get_pool()
    conn = p.getconn()
    
    
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT 1")
    except Exception:
        p.putconn(conn, close=True)
        conn = p.getconn()

    try:
        yield conn
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass  
        raise
    finally:
        try:
            close_it = (conn.closed != 0)
        except Exception:
            close_it = True
        p.putconn(conn, close=close_it)




def init_db():
    """Create tables if they don't exist."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id              TEXT PRIMARY KEY,
                    email           TEXT UNIQUE NOT NULL,
                    name            TEXT NOT NULL DEFAULT '',
                    phone           TEXT NOT NULL DEFAULT '',
                    photo           TEXT,
                    plan            TEXT NOT NULL DEFAULT 'free',
                    email_alerts    BOOLEAN NOT NULL DEFAULT TRUE,
                    weekly_digest   BOOLEAN NOT NULL DEFAULT FALSE,
                    risk_alerts     BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
                );
            """)
            cur.execute("""
                CREATE TABLE IF NOT EXISTS contracts (
                    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                    user_id         TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                    filename        TEXT NOT NULL,
                    analyzed_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                    risk_score      INTEGER,
                    risk_level      TEXT,
                    clauses_json    JSONB,
                    summary_json    JSONB,
                    status          TEXT NOT NULL DEFAULT 'review'
                );
            """)
            cur.execute("""
                CREATE INDEX IF NOT EXISTS idx_contracts_user_id
                ON contracts(user_id, analyzed_at DESC);
            """)
    print("[db] Tables initialised")




def upsert_user(user_id: str, email: str, name: str, photo: str | None = None, plan: str = "free") -> dict:
    """Insert or update a user row. Returns the full user row."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                INSERT INTO users (id, email, name, photo, plan)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (id) DO UPDATE
                    SET name       = EXCLUDED.name,
                        photo      = COALESCE(EXCLUDED.photo, users.photo),
                        updated_at = NOW()
                RETURNING *;
            """, (user_id, email, name, photo, plan))
            return dict(cur.fetchone())


def get_user(user_id: str) -> dict | None:
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM users WHERE id = %s;", (user_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def update_user_profile(user_id: str, name: str | None, phone: str | None,
                        email_alerts: bool | None, weekly_digest: bool | None,
                        risk_alerts: bool | None) -> dict | None:
    """Partial update — only updates fields that are not None."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                UPDATE users
                SET name          = COALESCE(%s, name),
                    phone         = COALESCE(%s, phone),
                    email_alerts  = COALESCE(%s, email_alerts),
                    weekly_digest = COALESCE(%s, weekly_digest),
                    risk_alerts   = COALESCE(%s, risk_alerts),
                    updated_at    = NOW()
                WHERE id = %s
                RETURNING *;
            """, (name, phone, email_alerts, weekly_digest, risk_alerts, user_id))
            row = cur.fetchone()
            return dict(row) if row else None




def save_contract(user_id: str, filename: str, risk_score: int, risk_level: str,
                  clauses: list, summary: dict, status: str = "review") -> str:
    """Persist an analysis result. Returns the new contract UUID."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            contract_id = str(uuid.uuid4())
            cur.execute("""
                INSERT INTO contracts
                    (id, user_id, filename, risk_score, risk_level, clauses_json, summary_json, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                RETURNING id;
            """, (
                contract_id, user_id, filename, risk_score, risk_level,
                json.dumps(clauses), json.dumps(summary), status
            ))
            return contract_id


def get_contracts_for_user(user_id: str) -> list[dict]:
    """Return all contracts for a user, newest first."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT id, user_id, filename, analyzed_at, risk_score, risk_level, status
                FROM contracts
                WHERE user_id = %s
                ORDER BY analyzed_at DESC;
            """, (user_id,))
            return [dict(r) for r in cur.fetchall()]


def get_contract_by_id(contract_id: str) -> dict | None:
    """Return full contract row including clauses_json + summary_json."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("SELECT * FROM contracts WHERE id = %s;", (contract_id,))
            row = cur.fetchone()
            return dict(row) if row else None


def count_analyses_today(user_id: str) -> int:
    """Count how many analyses this user has run since midnight (server time)."""
    with get_conn() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT COUNT(*) FROM contracts
                WHERE user_id = %s AND analyzed_at >= date_trunc('day', now());
            """, (user_id,))
            return cur.fetchone()[0]


def get_user_stats(user_id: str) -> dict:
    """Return aggregated stats for the profile page."""
    with get_conn() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT
                    COUNT(*)                                             AS contracts_analyzed,
                    COALESCE(SUM(
                        jsonb_array_length(clauses_json)
                    ), 0)                                                AS total_clauses,
                    COALESCE(SUM(
                        (SELECT COUNT(*) FROM jsonb_array_elements(clauses_json) elem
                         WHERE elem->>'risk' IN ('critical','warning'))
                    ), 0)                                                AS clauses_flagged
                FROM contracts
                WHERE user_id = %s;
            """, (user_id,))
            row = cur.fetchone()
            return dict(row) if row else {"contracts_analyzed": 0, "total_clauses": 0, "clauses_flagged": 0}
