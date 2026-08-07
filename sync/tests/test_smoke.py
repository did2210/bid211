"""Проверка, что инфраструктура поднята и отвечает."""
import os

import clickhouse_connect
import psycopg
import pytest

# Потолок памяти сервера из clickhouse/config.d/limits.xml.
ПОТОЛОК_ПАМЯТИ = 40 * 1024**3


def ch_клиент():
    return clickhouse_connect.get_client(
        host=os.environ["CH_HOST"], port=int(os.environ["CH_PORT"]),
        username=os.environ["CH_USER"], password=os.environ["CH_PASSWORD"],
    )


def test_clickhouse_отвечает():
    assert ch_клиент().command("SELECT 1") == 1


def test_clickhouse_потолок_памяти():
    """Потолок памяти сервера — 40 ГБ, но не больше, чем позволяет машина.

    ClickHouse сам опускает его до 90 % доступной оперативной памяти, если
    машина меньше боевой. На сервере (62 ГБ) сработает наши 40 ГБ,
    на машине разработчика — урезанное значение.
    """
    client = ch_клиент()
    предел = int(client.command(
        "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'"
    ))
    всего_памяти = int(client.command(
        "SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryTotal'"
    ))
    ожидаемый = min(ПОТОЛОК_ПАМЯТИ, int(всего_памяти * 0.9))
    assert предел == pytest.approx(ожидаемый, rel=0.05)


@pytest.mark.parametrize("настройка, ожидание", [
    ("background_pool_size", "8"),
    ("background_merges_mutations_concurrency_ratio", "2"),
])
def test_clickhouse_фоновый_пул_урезан(настройка, ожидание):
    """Фоновые слияния делят диск с PostgreSQL, поэтому пул урезан вдвое."""
    значение = ch_клиент().command(
        f"SELECT value FROM system.server_settings WHERE name = '{настройка}'"
    )
    # command() приводит числовые строки к int, поэтому сравниваем как текст.
    assert str(значение) == ожидание


@pytest.mark.parametrize("настройка, ожидание", [
    ("number_of_free_entries_in_pool_to_execute_mutation", "10"),
    ("number_of_free_entries_in_pool_to_lower_max_size_of_merge", "4"),
    ("number_of_free_entries_in_pool_to_execute_optimize_entire_partition", "12"),
])
def test_clickhouse_пороги_merge_tree_согласованы_с_пулом(настройка, ожидание):
    """Пороги считаются от размера пула: с дефолтами сервер не стартует вовсе."""
    значение = ch_клиент().command(
        f"SELECT value FROM system.merge_tree_settings WHERE name = '{настройка}'"
    )
    # command() приводит числовые строки к int, поэтому сравниваем как текст.
    assert str(значение) == ожидание


@pytest.mark.parametrize("настройка, ожидание", [
    ("max_memory_usage", str(10 * 1024**3)),
    ("max_threads", "8"),
    ("max_execution_time", "60"),
])
def test_clickhouse_ограничения_профиля_применены(настройка, ожидание):
    """Ограничения на отдельный запрос: профили задаются в users.d, не в config.d."""
    значение = ch_клиент().command(
        f"SELECT value FROM system.settings WHERE name = '{настройка}'"
    )
    # command() приводит числовые строки к int, поэтому сравниваем как текст.
    assert str(значение) == ожидание


def test_служебный_postgres_отвечает():
    with psycopg.connect(os.environ["PG_APP_DSN"]) as conn:
        assert conn.execute("SELECT 1").fetchone()[0] == 1
