"""Чтение из источника. Здесь же проверяются правила фильтрации —
ошибка в них означает неверные цифры во всём BI."""
import csv
import io
import os
from datetime import date

import psycopg

from gfd_sync.chunks import Chunk
from gfd_sync.reader import build_copy_sql, read_chunk, source_stats


def строки_куска(chunk):
    поток = b"".join(read_chunk(chunk)).decode("utf-8")
    return list(csv.reader(io.StringIO(поток)))


def test_запрос_ограничен_месяцем_и_сетью():
    sql = build_copy_sql(Chunk(2026, 7, "МАГНИТ"))
    assert "2026-07-01" in sql and "2026-08-01" in sql
    assert "МАГНИТ" in sql


def test_запрос_исключает_запрещённые_сети():
    sql = build_copy_sql(Chunk(2026, 7, "МАГНИТ"))
    for chain in ("ДОМ ЛЕНТА", "КАРУСЕЛЬ", "ЛЕНТА ЗООМАРКЕТ", "ПЯТЁРОЧКА РЦ"):
        assert chain in sql, "исключаемые сети должны быть в условии"


def test_запрос_не_фильтрует_категорию():
    """Категория правится в справочнике, поэтому фильтр по ней при заливке
    запрещён — иначе товар из «прочее» не появится без перезаливки."""
    sql = build_copy_sql(Chunk(2026, 7, "МАГНИТ"))
    assert "category" not in sql.lower()
    assert "прочее" not in sql


def test_исключённая_сеть_не_читается():
    got = b"".join(read_chunk(Chunk(2026, 7, "КАРУСЕЛЬ")))
    assert got == b"", "запрещённая сеть не должна отдавать ни строки"


def test_читается_csv_с_нужным_числом_колонок():
    первая = строки_куска(Chunk(2026, 7, "МАГНИТ"))[0]
    assert len(первая) == 8


def test_прочитанное_попадает_в_границы_куска():
    """Текст запроса можно вычитать глазами, а вот что он реально отобрал —
    нет. Перепутанные границы месяца или сравнение сети без trim прошли бы
    все проверки по тексту SQL и молча увезли бы в витрину чужие строки.
    """
    chunk = Chunk(2026, 7, "МАГНИТ")
    строки = строки_куска(chunk)
    assert строки, "кусок не должен быть пустым — синтетика покрывает все месяцы"
    даты = {date.fromisoformat(строка[1]) for строка in строки}
    сети = {строка[2] for строка in строки}
    assert min(даты) >= chunk.date_from
    assert max(даты) < chunk.date_to
    assert сети == {chunk.chain}


def test_статистика_совпадает_с_прямым_запросом():
    chunk = Chunk(2026, 7, "МАГНИТ")
    stats = source_stats(chunk)
    with psycopg.connect(os.environ["PG_SOURCE_DSN"]) as conn:
        rows, val = conn.execute("""
            SELECT count(*), coalesce(sum(salesvalue), 0) FROM sales
            WHERE pdate >= %s AND pdate < %s AND upper(client) = %s
        """, (chunk.date_from, chunk.date_to, chunk.chain)).fetchone()
    assert stats.rows == rows
    assert stats.sum_value == val


def test_флаг_опта_превращается_в_ноль_или_единицу():
    opts = {строка[7] for строка in строки_куска(Chunk(2026, 7, "МАГНИТ"))}
    assert opts <= {"0", "1"}
