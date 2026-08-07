"""Соединения. Источник открывается строго на чтение — это защита от опечатки,
которая иначе может испортить боевую базу."""
from __future__ import annotations

import clickhouse_connect
import psycopg
from clickhouse_connect.driver import Client

from .config import get_settings


def ch_client() -> Client:
    s = get_settings()
    return clickhouse_connect.get_client(
        host=s.ch_host, port=s.ch_port,
        username=s.ch_user, password=s.ch_password, database=s.ch_db,
    )


def pg_source() -> psycopg.Connection:
    """Соединение с источником. read_only включён на уровне сессии:
    любая попытка записи упадёт с ReadOnlySqlTransaction."""
    conn = psycopg.connect(get_settings().pg_source_dsn)
    conn.read_only = True
    return conn


def pg_app() -> psycopg.Connection:
    return psycopg.connect(get_settings().pg_app_dsn, autocommit=True)
