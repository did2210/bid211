"""Создание схемы витрины из SQL-файлов."""
from __future__ import annotations

from pathlib import Path

from clickhouse_connect.driver import Client

SCHEMA_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "schema"

# Единый порядок колонок для чтения из PostgreSQL и вставки в ClickHouse.
# Любое расхождение здесь приведёт к тихой порче данных, поэтому источник
# истины один — эта константа.
SALES_COLUMNS: tuple[str, ...] = (
    "src_id", "pdate", "client", "store_no", "xcode",
    "salesitem", "salesvalue", "opt",
)


def _run_sql_file(client: Client, path: Path) -> None:
    for statement in path.read_text(encoding="utf-8").split(";"):
        if statement.strip():
            client.command(statement)


def create_sales_tables(client: Client) -> None:
    """Идемпотентно: повторный вызов не трогает существующие данные."""
    _run_sql_file(client, SCHEMA_DIR / "01_sales.sql")
