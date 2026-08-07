"""Заливка куска через промежуточную таблицу.

Порядок операций выбран так, чтобы витрина ни в какой момент не показывала
недолитые данные:

    1. чистим промежуточную партицию (могли остаться следы прошлого сбоя);
    2. льём туда данные из источника;
    3. сверяем контрольные суммы с источником;
    4. только при совпадении — REPLACE PARTITION, операция атомарна;
    5. чистим за собой.

Если процесс убить на любом шаге до четвёртого, боевая таблица не пострадает.
"""
from __future__ import annotations

import time
from decimal import Decimal
from typing import NamedTuple

from .chunks import Chunk
from .clients import ch_client
from .reader import Stats, read_chunk, source_stats
from .schema import SALES_COLUMNS

_INSERT_SETTINGS = {
    # Заливка не должна выдавливать память у PostgreSQL.
    "max_insert_threads": 4,
    "max_memory_usage": 4 * 1024**3,
}


class LoadResult(NamedTuple):
    chunk: Chunk
    rows: int
    seconds: float
    replaced: bool
    error: str | None


def _partition_literal(chunk: Chunk) -> str:
    ym, chain = chunk.partition_id
    return f"({ym}, '{chain.replace(chr(39), chr(39) * 2)}')"


def target_stats(chunk: Chunk, table: str = "sales") -> Stats:
    row = ch_client().query(
        f"SELECT count(), coalesce(sum(salesvalue), 0), coalesce(sum(salesitem), 0) "
        f"FROM {table} WHERE toYYYYMM(pdate) = %(ym)s AND client = %(chain)s",
        parameters={"ym": chunk.partition_id[0], "chain": chunk.chain},
    ).result_rows[0]
    return Stats(int(row[0]), Decimal(str(row[1])), Decimal(str(row[2])))


def create_shadow(client) -> None:
    """Теневая копия витрины для полной перезаливки."""
    client.command("CREATE TABLE IF NOT EXISTS sales_shadow AS sales")


def swap_shadow(client) -> None:
    """Атомарно меняет местами боевую таблицу и теневую.

    До этого момента пользователи видят старые данные, после — новые.
    Промежуточного состояния нет: EXCHANGE TABLES выполняется под блокировкой.
    """
    client.command("EXCHANGE TABLES sales AND sales_shadow")


def load_chunk(chunk: Chunk, target: str = "sales") -> LoadResult:
    """Заливает кусок в указанную таблицу.

    target="sales" — обычная работа, подменяется партиция боевой таблицы.
    target="sales_shadow" — полная перезаливка, боевая таблица не трогается.
    """
    started = time.monotonic()
    ch = ch_client()
    part = _partition_literal(chunk)

    try:
        ch.command(f"ALTER TABLE sales_staging DROP PARTITION {part}")

        rows = 0
        for block in read_chunk(chunk):
            if not block:
                continue
            ch.raw_insert("sales_staging", column_names=list(SALES_COLUMNS),
                          insert_block=block, fmt="CSV", settings=_INSERT_SETTINGS)
            rows += block.count(b"\n")

        src = source_stats(chunk)
        got = target_stats(chunk, table="sales_staging")
        if got != src:
            ch.command(f"ALTER TABLE sales_staging DROP PARTITION {part}")
            return LoadResult(
                chunk, got.rows, time.monotonic() - started, False,
                f"расхождение с источником: в источнике {src}, залито {got}",
            )

        # Атомарная подмена. Пустая партиция в staging корректно очищает
        # партицию в цели — сеть, переставшая присылать данные, обнуляется.
        ch.command(f"ALTER TABLE {target} REPLACE PARTITION {part} FROM sales_staging")
        ch.command(f"ALTER TABLE sales_staging DROP PARTITION {part}")

        return LoadResult(chunk, src.rows, time.monotonic() - started, True, None)

    except Exception as exc:                      # noqa: BLE001 — причина уходит в журнал
        try:
            ch.command(f"ALTER TABLE sales_staging DROP PARTITION {part}")
        except Exception:                          # noqa: BLE001
            pass
        return LoadResult(chunk, 0, time.monotonic() - started, False, str(exc))
