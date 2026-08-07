"""Сверка ловит любые изменения в источнике, как бы они ни произошли."""
import os
from contextlib import contextmanager
from datetime import date

import psycopg
import pytest

from gfd_sync.chunks import Chunk
from gfd_sync.clients import ch_client
from gfd_sync.loader import load_chunk, target_stats
from gfd_sync.reader import source_stats
from gfd_sync.schema import create_dictionaries, create_sales_tables
from gfd_sync.verifier import known_chains, recent_period, repair, verify_chunks


@pytest.fixture(autouse=True)
def подготовка():
    ch = ch_client()
    create_sales_tables(ch)
    create_dictionaries(ch)


@contextmanager
def правка_в_источнике(chunk, надбавка=1000):
    """Меняет одну строку и возвращает всё назад.

    Синтетика — общий ресурс всех тестов: оставленная правка всплыла бы
    в чужом тесте расхождением, которого там быть не должно.
    """
    with psycopg.connect(os.environ["PG_SOURCE_DSN"], autocommit=True) as conn:
        (строка,) = conn.execute("""
            SELECT id FROM sales
            WHERE pdate >= %s AND pdate < %s AND upper(trim(client)) = %s
            ORDER BY id LIMIT 1
        """, (chunk.date_from, chunk.date_to, chunk.chain)).fetchone()
        conn.execute(
            "UPDATE sales SET salesvalue = salesvalue + %s WHERE id = %s",
            (надбавка, строка))
        try:
            yield строка
        finally:
            conn.execute(
                "UPDATE sales SET salesvalue = salesvalue - %s WHERE id = %s",
                (надбавка, строка))


def test_совпадающий_кусок_расхождений_не_даёт():
    chunk = Chunk(2026, 7, "МАГНИТ")
    load_chunk(chunk)
    assert verify_chunks([chunk]) == []


def test_незалитый_кусок_показывается_как_расхождение():
    chunk = Chunk(2026, 9, "МАГНИТ")
    ch_client().command("ALTER TABLE sales DROP PARTITION (202609, 'МАГНИТ')")
    got = verify_chunks([chunk])
    assert len(got) == 1
    assert got[0].target.rows == 0
    assert got[0].source.rows > 0


def test_правка_в_источнике_обнаруживается():
    """Сценарий: данные поправили руками, мимо загрузки файлов."""
    chunk = Chunk(2026, 8, "ЛЕНТА")
    load_chunk(chunk)
    assert verify_chunks([chunk]) == []

    with правка_в_источнике(chunk):
        assert len(verify_chunks([chunk])) == 1

    load_chunk(chunk)      # возвращаем витрину к исходной синтетике


def test_починка_устраняет_расхождение():
    chunk = Chunk(2026, 8, "ЛЕНТА")
    load_chunk(chunk)

    with правка_в_источнике(chunk):
        mismatches = verify_chunks([chunk])
        assert mismatches, "правка обязана дать расхождение"
        results = repair(mismatches)
        assert all(r.replaced for r in results)
        assert verify_chunks([chunk]) == []
        assert target_stats(chunk) == source_stats(chunk)

    load_chunk(chunk)


def test_исключённые_сети_не_попадают_в_перечисление():
    """Попади исключённая сеть в список — сверка нашла бы у неё расхождение
    (в источнике строки есть, в витрине их не должно быть), и починка
    заливала бы её снова и снова. Вечный ложный алярм.
    """
    сети = known_chains()
    for сеть in ("ДОМ ЛЕНТА", "КАРУСЕЛЬ", "ЛЕНТА ЗООМАРКЕТ", "ПЯТЁРОЧКА РЦ"):
        assert сеть not in сети
    assert "МАГНИТ" in сети, "обычные сети перечисляться обязаны"


def test_перечисление_за_период_не_шире_полного():
    сети_месяца = known_chains(date(2026, 7, 1), date(2026, 8, 1))
    assert set(сети_месяца) <= set(known_chains())
    assert сети_месяца, "в июле сети торговали — список пустым быть не может"


@pytest.mark.parametrize("сегодня, месяцев, ожидание", [
    (date(2026, 8, 7), 3, (date(2026, 6, 1), date(2026, 9, 1))),
    (date(2026, 1, 15), 3, (date(2025, 11, 1), date(2026, 2, 1))),
    (date(2026, 3, 1), 1, (date(2026, 3, 1), date(2026, 4, 1))),
    (date(2026, 12, 31), 2, (date(2026, 11, 1), date(2027, 1, 1))),
])
def test_свежий_хвост_считается_по_календарю(сегодня, месяцев, ожидание):
    """Переход через год — место, где такие расчёты ошибаются чаще всего,
    а промах здесь означает молча несверенный месяц."""
    assert recent_period(месяцев, today=сегодня) == ожидание
