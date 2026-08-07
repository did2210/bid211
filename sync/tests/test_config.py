"""Настройки читаются из окружения, соединение с источником — только на чтение."""
import psycopg
import pytest

from gfd_sync.clients import ch_client, pg_app, pg_source
from gfd_sync.config import get_settings
from tools.gen_fake_data import CHAINS


def test_настройки_читаются():
    s = get_settings()
    assert s.ch_port > 0
    assert s.pg_source_dsn.startswith("postgresql://")


def test_исключённые_сети_заданы():
    s = get_settings()
    assert set(s.excluded_chains) == {
        "ДОМ ЛЕНТА", "КАРУСЕЛЬ", "ЛЕНТА ЗООМАРКЕТ", "ПЯТЁРОЧКА РЦ"}


def test_исключённые_сети_есть_в_источнике():
    """Написание должно совпадать посимвольно с тем, что лежит в данных.

    Фильтр сравнивает названия как текст, поэтому «ПЯТЁРОЧКА РЦ» через «Е»
    вместо «Ё» не отсеет ничего и сделает это молча: заливка пройдёт,
    а в витрину приедут лишние сети.
    """
    сети_источника = {название for название, _ in CHAINS}
    assert set(get_settings().excluded_chains) <= сети_источника


def test_клиент_clickhouse_работает():
    assert ch_client().command("SELECT 1") == 1


def test_соединение_с_источником_запрещает_запись():
    """Гарантия того, что синхронизатор физически не может испортить боевую базу."""
    with pg_source() as conn:
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            conn.execute("CREATE TABLE не_должно_создаться (x int)")


def test_служебная_база_писать_разрешает():
    with pg_app() as conn:
        conn.execute("CREATE TEMP TABLE проверка (x int)")
