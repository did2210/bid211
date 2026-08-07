"""Обнаружение изменений по журналу загрузок источника.

Файл в load_log — сигнал «здесь что-то поменялось». Какие именно куски
затронуты, спрашиваем у самой таблицы sales по колонке name_file: имя файла
может быть каким угодно, а данные не врут.

Имя всё же используется — но только чтобы сузить поиск. Индекса по
name_file в боевой базе нет и не будет (менять её нельзя), поэтому запрос
без сужения означает полный скан таблицы продаж каждые десять минут: он
и сам по себе долгий, и вымывает кеш страниц у PostgreSQL, которому мы
обещали не мешать. Разобрав «СЕТЬ_ГГГГ_ММ.xlsx», мы попадаем в индекс
(upper(trim(client)), pdate) и читаем только нужный кусок.

Если имя соврало и данные лежат в другом месяце, кусок всё равно будет
найден — сверкой (verify_recent каждые десять минут, verify_all ночью).
Детектор здесь быстрый путь, а гарантию даёт сверка.
"""
from __future__ import annotations

import re
from datetime import date, datetime, timedelta, timezone
from pathlib import PurePosixPath

from .chunks import Chunk
from .clients import pg_source
from .config import EXCLUDED_CHAINS

# СЕТЬ_ГГГГ_ММ.xlsx — как файлы называет система загрузки.
_ИМЯ_ФАЙЛА = re.compile(r"^(?P<chain>.+)_(?P<year>\d{4})_(?P<month>\d{2})\.[^.]+$")


class _Подсказка(tuple):
    """Сеть и месяц, вычитанные из имени файла."""

    __slots__ = ()

    def __new__(cls, chain: str, date_from: date, date_to: date):
        return super().__new__(cls, (chain, date_from, date_to))

    chain = property(lambda self: self[0])
    date_from = property(lambda self: self[1])
    date_to = property(lambda self: self[2])


def _подсказка(имя: str) -> _Подсказка | None:
    m = _ИМЯ_ФАЙЛА.match(имя)
    if not m:
        return None
    year, month = int(m["year"]), int(m["month"])
    if not 1 <= month <= 12:
        return None
    date_from = date(year, month, 1)
    date_to = (date(year + 1, 1, 1) if month == 12 else date(year, month + 1, 1))
    return _Подсказка(m["chain"].strip().upper(), date_from, date_to)


def new_files(since: datetime | None = None) -> list[dict]:
    if since is None:
        since = datetime.now(timezone.utc) - timedelta(hours=24)
    with pg_source() as conn:
        rows = conn.execute("""
            SELECT file_path, file_hash, rows_loaded, loaded_at
            FROM public.load_log
            WHERE loaded_at > %s AND status = 'completed'
            ORDER BY loaded_at
        """, (since,)).fetchall()
    keys = ("file_path", "file_hash", "rows_loaded", "loaded_at")
    return [dict(zip(keys, r)) for r in rows]


def _куски_запросом(conn, names: list[str], сужение: str, параметры: tuple
                    ) -> list[Chunk]:
    excluded = ", ".join(f"'{c}'" for c in EXCLUDED_CHAINS)
    rows = conn.execute(f"""
        SELECT DISTINCT
               extract(year FROM pdate)::int,
               extract(month FROM pdate)::int,
               upper(trim(client))
        FROM public.sales
        WHERE name_file = ANY(%s)
          AND pdate IS NOT NULL
          AND upper(trim(client)) NOT IN ({excluded})
          {сужение}
    """, (names, *параметры)).fetchall()
    return [Chunk(y, m, c) for y, m, c in rows]


def chunks_from_files(files: list[dict]) -> list[Chunk]:
    """Спрашиваем у sales, какие пары «месяц × сеть» пришли из этих файлов."""
    names = [PurePosixPath(f["file_path"]).name for f in files]
    if not names:
        return []

    понятные = {имя: п for имя in names if (п := _подсказка(имя))}
    прочие = [имя for имя in names if имя not in понятные]

    куски: list[Chunk] = []
    with pg_source() as conn:
        if понятные:
            подсказки = list(понятные.values())
            куски += _куски_запросом(
                conn, list(понятные),
                "AND upper(trim(client)) = ANY(%s) AND pdate >= %s AND pdate < %s",
                ([п.chain for п in подсказки],
                 min(п.date_from for п in подсказки),
                 max(п.date_to for п in подсказки)),
            )
        if прочие:
            # Имя ни о чём не говорит — придётся искать по всей таблице.
            куски += _куски_запросом(conn, прочие, "", ())
    return куски


def pending_chunks() -> list[Chunk]:
    """Куски, затронутые файлами за последние сутки, без повторов."""
    return sorted(set(chunks_from_files(new_files())))
