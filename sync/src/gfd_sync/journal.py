"""Журнал синхронизации в служебной базе.

Каждая операция оставляет след: что делали, сколько строк, сколько времени,
чем кончилось. Админка первого этапа — это выборки отсюда.
"""
from __future__ import annotations

from datetime import datetime
from pathlib import Path

from .chunks import Chunk
from .clients import pg_app
from .loader import LoadResult

APP_SCHEMA = Path(__file__).resolve().parents[3] / "pg_app" / "init" / "01_journal.sql"


def ensure_app_schema() -> None:
    """Создаёт схему служебной базы, если её ещё нет.

    Тот же файл применяет docker-энтрипоинт при первом запуске контейнера,
    но полагаться на это нельзя: init-скрипты PostgreSQL выполняются только
    на пустом каталоге данных. На сервере база уже развёрнута — там схема
    приезжает исключительно этим путём. Файл идемпотентен, повторный вызов
    ничего не портит.
    """
    with pg_app() as conn:
        conn.execute(APP_SCHEMA.read_text(encoding="utf-8"))


def record(result: LoadResult, operation: str) -> int:
    with pg_app() as conn:
        return conn.execute("""
            INSERT INTO sync_journal (operation, chunk_key, rows, seconds, status, error)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        """, (operation, result.chunk.key if result.chunk else None,
              result.rows, round(result.seconds, 3),
              "ok" if result.error is None else "error", result.error)).fetchone()[0]


def last_runs(limit: int = 50) -> list[dict]:
    with pg_app() as conn:
        rows = conn.execute("""
            SELECT id, started_at, operation, chunk_key, rows, seconds, status, error
            FROM sync_journal ORDER BY started_at DESC, id DESC LIMIT %s
        """, (limit,)).fetchall()
    keys = ("id", "started_at", "operation", "chunk_key", "rows",
            "seconds", "status", "error")
    return [dict(zip(keys, r)) for r in rows]


def last_success(chunk: Chunk) -> datetime | None:
    with pg_app() as conn:
        row = conn.execute("""
            SELECT max(started_at) FROM sync_journal
            WHERE chunk_key = %s AND status = 'ok'
        """, (chunk.key,)).fetchone()
    return row[0] if row else None


def has_failures(since_minutes: int = 60) -> bool:
    with pg_app() as conn:
        row = conn.execute("""
            SELECT count(*) FROM sync_journal
            WHERE status = 'error'
              AND started_at > now() - make_interval(mins => %s)
        """, (since_minutes,)).fetchone()
    return row[0] > 0
