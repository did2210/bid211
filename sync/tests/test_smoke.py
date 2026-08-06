"""Проверка, что инфраструктура поднята и отвечает."""
import os
import clickhouse_connect
import psycopg


def test_clickhouse_отвечает():
    client = clickhouse_connect.get_client(
        host=os.environ["CH_HOST"], port=int(os.environ["CH_PORT"]),
        username=os.environ["CH_USER"], password=os.environ["CH_PASSWORD"],
    )
    assert client.command("SELECT 1") == 1


def test_clickhouse_лимиты_применены():
    client = clickhouse_connect.get_client(
        host=os.environ["CH_HOST"], port=int(os.environ["CH_PORT"]),
        username=os.environ["CH_USER"], password=os.environ["CH_PASSWORD"],
    )
    limit = client.command(
        "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'"
    )
    assert int(limit) == 40 * 1024**3, "лимит памяти сервера должен быть 40 ГБ"


def test_служебный_postgres_отвечает():
    with psycopg.connect(os.environ["PG_APP_DSN"]) as conn:
        assert conn.execute("SELECT 1").fetchone()[0] == 1
