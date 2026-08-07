"""Создание схемы витрины из SQL-файлов."""
from __future__ import annotations

import re
from pathlib import Path
from urllib.parse import unquote, urlparse

from clickhouse_connect.driver import Client

from .config import get_settings

SCHEMA_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "schema"

# Единый порядок колонок для чтения из PostgreSQL и вставки в ClickHouse.
# Любое расхождение здесь приведёт к тихой порче данных, поэтому источник
# истины один — эта константа.
SALES_COLUMNS: tuple[str, ...] = (
    "src_id", "pdate", "client", "store_no", "xcode",
    "salesitem", "salesvalue", "opt",
)


def _statements(sql: str) -> list[str]:
    """Режет файл на операторы: ClickHouse принимает их только по одному.

    Границей считается точка с запятой в конце строки, а не то, что идёт
    следом: между операторами стоят комментарии, и они просто прилипают
    к началу следующего — это допустимо.
    """
    return [s for s in re.split(r";\s*\n", sql) if s.strip()]


def _run_sql_file(client: Client, path: Path) -> None:
    for statement in _statements(path.read_text(encoding="utf-8")):
        client.command(statement)


def create_sales_tables(client: Client) -> None:
    """Идемпотентно: повторный вызов не трогает существующие данные."""
    _run_sql_file(client, SCHEMA_DIR / "01_sales.sql")


def _dsn_parts(dsn: str) -> dict[str, str]:
    u = urlparse(dsn)
    return {"host": u.hostname or "localhost", "port": str(u.port or 5432),
            "user": unquote(u.username or ""),
            "password": unquote(u.password or ""),
            "db": (u.path or "/").lstrip("/")}


def create_dictionaries(client: Client) -> None:
    """Словари читают PostgreSQL напрямую. Пароли подставляются здесь и
    в репозиторий не попадают."""
    s = get_settings()
    src, app = _dsn_parts(s.pg_source_dsn), _dsn_parts(s.pg_app_dsn)
    sql = (SCHEMA_DIR / "02_dictionaries.sql").read_text(encoding="utf-8")
    sql = sql.format(
        # Хост и порт берутся из настроек словарей, а не из DSN: DSN описывает
        # путь от синхронизатора, а ходить по нему будет сервер ClickHouse.
        host=s.dict_source_host, port=s.dict_source_port,
        user=src["user"], password=src["password"], db=src["db"],
        app_host=s.dict_app_host, app_port=s.dict_app_port,
        app_user=app["user"], app_password=app["password"], app_db=app["db"],
    )
    for statement in _statements(sql):
        client.command(statement)
