"""Сверка витрины с источником.

Ловит изменения независимо от того, как они произошли: через загрузку файла,
правкой руками или процедурой пересчёта опта. Это последний рубеж против
самой неприятной поломки — когда цифры устарели, а никто об этом не знает.
"""
from __future__ import annotations

from datetime import date
from typing import NamedTuple

from .chunks import Chunk, chunks_in_period
from .clients import pg_source
from .config import EXCLUDED_CHAINS, get_settings
from .loader import LoadResult, load_chunk, target_stats
from .reader import Stats, source_stats


class Mismatch(NamedTuple):
    chunk: Chunk
    source: Stats
    target: Stats


def known_chains(date_from: date | None = None,
                 date_to: date | None = None) -> list[str]:
    """Сети, встречающиеся в источнике, кроме исключённых.

    Период стоит задавать всегда, когда он известен: без него запрос
    вычитывает таблицу продаж целиком, а с ним планировщик уходит
    в индекс (upper(trim(client)), pdate) и читает только нужный кусок.
    """
    excluded = ", ".join(f"'{c}'" for c in EXCLUDED_CHAINS)
    период = ""
    параметры: tuple = ()
    if date_from is not None and date_to is not None:
        период = "AND pdate >= %s AND pdate < %s"
        параметры = (date_from, date_to)
    with pg_source() as conn:
        rows = conn.execute(
            f"SELECT DISTINCT upper(trim(client)) FROM public.sales "
            f"WHERE upper(trim(client)) NOT IN ({excluded}) {период}",
            параметры,
        ).fetchall()
    return sorted(r[0] for r in rows)


def verify_chunks(chunks: list[Chunk]) -> list[Mismatch]:
    out: list[Mismatch] = []
    for chunk in chunks:
        src, dst = source_stats(chunk), target_stats(chunk)
        if src != dst:
            out.append(Mismatch(chunk, src, dst))
    return out


def recent_period(months: int, today: date | None = None) -> tuple[date, date]:
    """Полуинтервал из последних `months` календарных месяцев, включая текущий.

    Вынесено отдельно, потому что вся тонкость здесь — переход через год,
    а промах означает молча несверенный месяц.
    """
    today = today or date.today()
    year, month = today.year, today.month - months + 1
    while month < 1:
        year, month = year - 1, month + 12
    конец = (date(today.year + 1, 1, 1) if today.month == 12
             else date(today.year, today.month + 1, 1))
    return date(year, month, 1), конец


def verify_recent(months: int = 3) -> list[Mismatch]:
    """Сверка свежего хвоста — дёшево, гоняется каждые десять минут."""
    s = get_settings()
    начало, конец = recent_period(months)
    # За границы заливки не выходим: месяцев вне периода в витрине нет
    # по определению, и сверять их означало бы искать расхождение там,
    # где его не может быть.
    начало = max(начало, s.load_date_from)
    конец = min(конец, s.load_date_to)
    return verify_chunks(
        chunks_in_period(начало, конец, known_chains(начало, конец)))


def verify_all() -> list[Mismatch]:
    """Полная сверка. Сканирует источник целиком, поэтому только ночью."""
    s = get_settings()
    return verify_chunks(
        chunks_in_period(s.load_date_from, s.load_date_to, known_chains()))


def repair(mismatches: list[Mismatch]) -> list[LoadResult]:
    return [load_chunk(m.chunk) for m in mismatches]
