"""Кусок — минимальная единица заливки. От него зависит подмена партиций,
поэтому границы и идентификаторы проверяем придирчиво."""
import dataclasses
from datetime import date

import pytest

from gfd_sync.chunks import Chunk, chunk_from_key, chunk_of, chunks_in_period


def test_идентификатор_партиции():
    assert Chunk(2026, 7, "МАГНИТ").partition_id == (202607, "МАГНИТ")


def test_границы_месяца_полуинтервал():
    c = Chunk(2026, 7, "МАГНИТ")
    assert c.date_from == date(2026, 7, 1)
    assert c.date_to == date(2026, 8, 1)


def test_границы_декабря_переходят_на_следующий_год():
    c = Chunk(2026, 12, "ЛЕНТА")
    assert c.date_from == date(2026, 12, 1)
    assert c.date_to == date(2027, 1, 1)


def test_ключ_и_разбор_ключа():
    c = Chunk(2026, 7, "X5 ЧИЖИК")
    assert c.key == "202607/X5 ЧИЖИК"
    assert chunk_from_key(c.key) == c


def test_кусок_по_дате():
    assert chunk_of(date(2026, 7, 31), "ЛЕНТА") == Chunk(2026, 7, "ЛЕНТА")


def test_перечисление_за_период():
    got = chunks_in_period(date(2026, 5, 1), date(2026, 8, 1), ["МАГНИТ", "ЛЕНТА"])
    assert len(got) == 6
    assert Chunk(2026, 5, "МАГНИТ") in got
    assert Chunk(2026, 7, "ЛЕНТА") in got
    assert Chunk(2026, 8, "ЛЕНТА") not in got, "верхняя граница не включается"


def test_период_с_середины_месяца_берёт_месяц_целиком():
    """Кусок — всегда полный месяц; частичные периоды округляются вниз."""
    got = chunks_in_period(date(2026, 5, 17), date(2026, 6, 3), ["ЛЕНТА"])
    assert got == [Chunk(2026, 5, "ЛЕНТА"), Chunk(2026, 6, "ЛЕНТА")]


def test_вырожденный_период_не_даёт_кусков():
    """Пустой полуинтервал — ничего не заливаем. Иначе повторный запуск
    без новых данных подменил бы партицию впустую."""
    assert chunks_in_period(date(2026, 8, 1), date(2026, 8, 1), ["ЛЕНТА"]) == []
    assert chunks_in_period(date(2026, 9, 1), date(2026, 8, 1), ["ЛЕНТА"]) == []


def test_кривой_месяц_отвергается_сразу():
    """Кусок с месяцем 13 создался бы молча и всплыл бы уже именем партиции
    или пустой выборкой — то есть далеко от места ошибки."""
    with pytest.raises(ValueError):
        Chunk(2026, 13, "ЛЕНТА")
    with pytest.raises(ValueError):
        chunk_from_key("202600/ЛЕНТА")


def test_кусок_неизменяем():
    with pytest.raises(dataclasses.FrozenInstanceError):
        Chunk(2026, 7, "ЛЕНТА").month = 8
