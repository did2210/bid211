"""Чтение куска из PostgreSQL.

Данные идут потоком через COPY ... TO STDOUT: Python не разбирает строки,
а перекладывает байты. На миллионах строк это на порядок быстрее курсора.

Соединение с источником открыто только на чтение (см. clients.pg_source).
"""
from __future__ import annotations

from decimal import Decimal
from typing import Iterator, NamedTuple

from .chunks import Chunk
from .clients import pg_source
from .config import EXCLUDED_CHAINS

# Порядок выражений строго соответствует schema.SALES_COLUMNS.
# Приведения делаются на стороне PostgreSQL, чтобы Python не трогал данные.
_SELECT = """
    SELECT id,
           pdate,
           upper(trim(client)),
           store_no,
           xcode,
           salesitem,
           salesvalue,
           CASE WHEN opt IS NOT NULL AND opt <> '' THEN 1 ELSE 0 END
    FROM public.sales
    WHERE pdate >= '{date_from}' AND pdate < '{date_to}'
      AND upper(trim(client)) = '{chain}'
      AND upper(trim(client)) NOT IN ({excluded})
"""


class Stats(NamedTuple):
    rows: int
    sum_value: Decimal
    sum_items: Decimal


def _excluded_literal() -> str:
    return ", ".join(f"'{c}'" for c in EXCLUDED_CHAINS)


def _select_sql(chunk: Chunk) -> str:
    # Имена сетей приходят из справочника, а не от пользователя, но апостроф
    # в названии всё равно возможен — экранируем.
    return _SELECT.format(
        date_from=chunk.date_from, date_to=chunk.date_to,
        chain=chunk.chain.replace("'", "''"), excluded=_excluded_literal(),
    )


def build_copy_sql(chunk: Chunk) -> str:
    return f"COPY ({_select_sql(chunk)}) TO STDOUT WITH (FORMAT csv)"


def read_chunk(chunk: Chunk) -> Iterator[bytes]:
    """Отдаёт куски CSV. Пустой поток — законный результат: сети могло
    не быть в этом месяце, или она в списке исключённых."""
    with pg_source() as conn, conn.cursor().copy(build_copy_sql(chunk)) as cp:
        for data in cp:
            yield bytes(data)


def source_stats(chunk: Chunk) -> Stats:
    """Контрольные суммы источника — с ними сверяется залитое."""
    sql = f"""
        SELECT count(*), coalesce(sum(salesvalue), 0), coalesce(sum(salesitem), 0)
        FROM ({_select_sql(chunk)}) t (id, pdate, client, store_no, xcode,
                                       salesitem, salesvalue, opt)
    """
    with pg_source() as conn:
        rows, value, items = conn.execute(sql).fetchone()
    return Stats(rows, Decimal(value), Decimal(items))
