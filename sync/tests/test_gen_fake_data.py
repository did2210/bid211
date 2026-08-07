"""Генератор должен воспроизводить схему боевой базы и давать правдоподобные данные."""
import os

import psycopg
import pytest

from tools.gen_fake_data import generate

СТРОК = 50_000
ГОД = 2026


@pytest.fixture(scope="module")
def сгенерированная_база():
    dsn = os.environ["PG_SOURCE_DSN"]
    counts = generate(dsn, rows=СТРОК, year=ГОД, seed=42)
    return dsn, counts


def test_создаёт_все_таблицы(сгенерированная_база):
    dsn, _ = сгенерированная_база
    with psycopg.connect(dsn) as conn:
        names = {r[0] for r in conn.execute(
            "SELECT tablename FROM pg_tables WHERE schemaname='public'").fetchall()}
    assert {"sales", "product", "address", "load_log", "kib_monthly_data"} <= names


def test_внешний_ключ_на_product_соблюдён(сгенерированная_база):
    dsn, _ = сгенерированная_база
    with psycopg.connect(dsn) as conn:
        orphans = conn.execute("""
            SELECT count(*) FROM sales s
            LEFT JOIN product p ON p.xcode = s.xcode
            WHERE p.xcode IS NULL
        """).fetchone()[0]
    assert orphans == 0, "у каждой продажи должна быть карточка товара"


def test_данные_только_за_указанный_год(сгенерированная_база):
    dsn, _ = сгенерированная_база
    with psycopg.connect(dsn) as conn:
        years = {r[0] for r in conn.execute(
            "SELECT DISTINCT extract(year FROM pdate)::int FROM sales").fetchall()}
    assert years == {ГОД}


def test_есть_исключаемые_сети(сгенерированная_база):
    """Генератор обязан создавать сети, которые заливка должна отсеять,
    иначе фильтр невозможно проверить."""
    dsn, _ = сгенерированная_база
    with psycopg.connect(dsn) as conn:
        n = conn.execute(
            "SELECT count(*) FROM sales WHERE upper(client) = 'КАРУСЕЛЬ'").fetchone()[0]
    assert n > 0


def test_есть_категория_прочее(сгенерированная_база):
    """Категория 'прочее' должна присутствовать: она НЕ фильтруется при заливке."""
    dsn, _ = сгенерированная_база
    with psycopg.connect(dsn) as conn:
        n = conn.execute(
            "SELECT count(*) FROM product WHERE category = 'прочее'").fetchone()[0]
    assert n > 0


def test_каждый_кусок_месяц_на_сеть_наполнен(сгенерированная_база):
    """Заливка идёт кусками «месяц × сеть» — пустых клеток быть не должно,
    иначе проверять подмену партиций будет не на чем."""
    dsn, _ = сгенерированная_база
    with psycopg.connect(dsn) as conn:
        месяцев, сетей, кусков = conn.execute("""
            SELECT count(DISTINCT extract(month FROM pdate)),
                   count(DISTINCT upper(trim(client))),
                   count(DISTINCT (extract(month FROM pdate),
                                   upper(trim(client))))
            FROM sales
        """).fetchone()
    assert месяцев == 12
    assert сетей == 12
    assert кусков == 144


def test_номер_точки_не_повторяется_между_сетями(сгенерированная_база):
    """АКБ считается как uniq(store_no), без оглядки на сеть.

    Если номер точки повторяется у двух сетей, точки склеиваются и АКБ
    занижается. Опаснее другое: склейка пришлась бы на пары
    «ЛЕНТА / ЛЕНТА ЗООМАРКЕТ» и «ПЯТЁРОЧКА / ПЯТЁРОЧКА РЦ», то есть на
    включаемую и исключаемую сеть. Сломайся фильтр сетей — лишние точки
    схлопнулись бы в существующие, и АКБ не изменился бы вовсе.
    """
    dsn, _ = сгенерированная_база
    with psycopg.connect(dsn) as conn:
        всего, уникальных = conn.execute(
            "SELECT count(*), count(DISTINCT store_no) FROM address").fetchone()
    assert всего == уникальных


def test_счётчики_совпадают_с_содержимым_базы(сгенерированная_база):
    """Возвращённые числа — контракт функции: на них опираются сверки."""
    dsn, counts = сгенерированная_база
    with psycopg.connect(dsn) as conn:
        факт = {
            таблица: conn.execute(
                f"SELECT count(*) FROM {таблица}").fetchone()[0]
            for таблица in ("product", "address", "sales", "load_log")
        }
    assert counts == факт
    assert факт["sales"] == СТРОК


def test_повторный_запуск_даёт_те_же_данные(сгенерированная_база):
    """Один seed — одни данные, иначе тесты сверки будут плавать."""
    dsn, _ = сгенерированная_база
    with psycopg.connect(dsn) as conn:
        first = conn.execute("SELECT sum(salesvalue) FROM sales").fetchone()[0]
    generate(dsn, rows=СТРОК, year=ГОД, seed=42)
    with psycopg.connect(dsn) as conn:
        second = conn.execute("SELECT sum(salesvalue) FROM sales").fetchone()[0]
    assert first == second
