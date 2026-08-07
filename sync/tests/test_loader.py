"""Заливка. Главное требование: витрина никогда не показывает
недолитый кусок — либо старые данные, либо новые целиком."""
import os
from decimal import Decimal

import psycopg
import pytest

from gfd_sync.chunks import Chunk
from gfd_sync.clients import ch_client
from gfd_sync.loader import create_shadow, load_chunk, swap_shadow, target_stats
from gfd_sync.reader import Stats, source_stats
from gfd_sync.schema import create_dictionaries, create_sales_tables


@pytest.fixture(autouse=True)
def подготовка():
    ch = ch_client()
    create_sales_tables(ch)
    create_dictionaries(ch)


@pytest.fixture
def без_теневой():
    """Теневая таблица — временная снасть, а не состояние витрины."""
    yield
    ch_client().command("DROP TABLE IF EXISTS sales_shadow")


def test_кусок_заливается_и_цифры_сходятся():
    chunk = Chunk(2026, 7, "МАГНИТ")
    result = load_chunk(chunk)
    assert result.error is None
    assert result.replaced is True
    assert target_stats(chunk) == source_stats(chunk)


def test_залитое_совпадает_с_источником_по_ключевым_полям():
    """Три агрегата сходятся и при перепутанных колонках.

    Поменяй местами store_no и xcode — count, сумма продаж и сумма объёма
    останутся теми же, а витрина будет врать в каждом разрезе. Поэтому
    сверяем ещё и то, что различает колонки между собой.
    """
    chunk = Chunk(2026, 7, "ПЕРЕКРЁСТОК")
    load_chunk(chunk)
    ch = ch_client()
    товаров, точек, сумма_id = ch.query(
        "SELECT uniqExact(xcode), uniqExact(store_no), sum(src_id) FROM sales "
        "WHERE toYYYYMM(pdate) = %(ym)s AND client = %(chain)s",
        parameters={"ym": chunk.partition_id[0], "chain": chunk.chain},
    ).result_rows[0]
    with psycopg.connect(os.environ["PG_SOURCE_DSN"]) as conn:
        ожидаемое = conn.execute("""
            SELECT count(DISTINCT xcode), count(DISTINCT store_no), sum(id)
            FROM sales
            WHERE pdate >= %s AND pdate < %s AND upper(trim(client)) = %s
        """, (chunk.date_from, chunk.date_to, chunk.chain)).fetchone()
    assert (товаров, точек, сумма_id) == tuple(ожидаемое)


def test_кусок_заливается_немногими_вставками():
    """COPY отдаёт данные построчно, и наивная перекладка шлёт по запросу
    на строку: 54 тысячи запросов на кусок, 388 секунд впустую и столько же
    мелких партов, от которых ClickHouse начинает притормаживать вставки.
    Считаем запросы, а не секунды: время зависит от машины, а это — нет.
    """
    ch = ch_client()
    chunk = Chunk(2026, 7, "ДИКСИ")
    ch.command("SYSTEM FLUSH LOGS")
    запрос = ("SELECT count() FROM system.query_log WHERE type = 'QueryFinish' "
              "AND query_kind = 'Insert' AND event_time > now() - INTERVAL 1 HOUR")
    было = int(ch.command(запрос))

    result = load_chunk(chunk)
    assert result.replaced, "кусок должен залиться"

    ch.command("SYSTEM FLUSH LOGS")
    вставок = int(ch.command(запрос)) - было
    assert вставок <= 10, f"на один кусок ушло {вставок} вставок"


def test_повторная_заливка_не_задваивает():
    """Подмена партиции, а не досыпка: два прогона дают тот же результат."""
    chunk = Chunk(2026, 7, "ЛЕНТА")
    load_chunk(chunk)
    first = target_stats(chunk)
    load_chunk(chunk)
    assert target_stats(chunk) == first


def test_соседние_куски_не_затронуты():
    magnit, lenta = Chunk(2026, 7, "МАГНИТ"), Chunk(2026, 7, "ЛЕНТА")
    load_chunk(magnit)
    load_chunk(lenta)
    before = target_stats(magnit)
    load_chunk(lenta)
    assert target_stats(magnit) == before


def test_пустой_кусок_очищает_партицию():
    """Сеть перестала присылать данные за месяц — старые строки должны уйти."""
    chunk = Chunk(2026, 7, "КАРУСЕЛЬ")   # исключённая сеть, источник пуст
    result = load_chunk(chunk)
    assert result.rows == 0
    assert result.error is None
    assert target_stats(chunk).rows == 0


def test_исчезнувшие_данные_убираются_из_витрины(monkeypatch):
    """Проверка того же, что и тест выше, но на партиции с данными.

    «Пустой кусок» на сети, которая никогда не заливалась, ничего не
    доказывает: партиция и так пуста. Здесь кусок сперва заливается,
    а потом источник пустеет — и витрина обязана опустеть следом,
    иначе снятый с продажи месяц остался бы в отчётах навсегда.
    """
    from gfd_sync import loader
    chunk = Chunk(2026, 7, "ВЕРНЫЙ")
    load_chunk(chunk)
    assert target_stats(chunk).rows > 0, "кусок должен был залиться"

    monkeypatch.setattr(loader, "read_chunk", lambda c: iter(()))
    monkeypatch.setattr(loader, "source_stats",
                        lambda c: Stats(0, Decimal("0"), Decimal("0")))
    result = load_chunk(chunk)

    assert result.error is None
    assert result.replaced is True
    assert target_stats(chunk).rows == 0


def test_расхождение_отменяет_подмену(monkeypatch):
    """Если залитое не сошлось с источником, партиция остаётся прежней."""
    from gfd_sync import loader
    chunk = Chunk(2026, 7, "ДИКСИ")
    load_chunk(chunk)
    было = target_stats(chunk)

    monkeypatch.setattr(loader, "source_stats",
                        lambda c: Stats(999_999, Decimal("1"), Decimal("1")))
    result = load_chunk(chunk)

    assert result.replaced is False
    assert result.error is not None and "расхождение" in result.error.lower()
    assert target_stats(chunk) == было, "данные обязаны остаться прежними"


def test_промежуточная_таблица_очищается():
    ch = ch_client()
    # Чистим до, а не только проверяем после: прерванная заливка оставляет
    # в промежуточной таблице партиции чужих кусков, и они копятся.
    ch.command("TRUNCATE TABLE sales_staging")
    load_chunk(Chunk(2026, 7, "АШАН"))
    assert ch.command("SELECT count() FROM sales_staging") == 0


def test_заливка_в_теневую_не_видна_в_основной(без_теневой):
    """Полная перезаливка идёт мимо боевой таблицы: пока она не кончится,
    люди работают на старых данных."""
    ch = ch_client()
    chunk = Chunk(2026, 7, "ОКЕЙ")
    load_chunk(chunk)
    было = target_stats(chunk)

    create_shadow(ch)
    ch.command("TRUNCATE TABLE sales_shadow")
    load_chunk(chunk, target="sales_shadow")

    assert target_stats(chunk) == было, "основная таблица не должна измениться"
    assert target_stats(chunk, table="sales_shadow").rows == было.rows


def test_обмен_таблиц_переключает_данные_разом(без_теневой):
    ch = ch_client()
    было_в_витрине = ch.command("SELECT count() FROM sales")
    create_shadow(ch)
    ch.command("TRUNCATE TABLE sales_shadow")
    load_chunk(Chunk(2026, 7, "ОКЕЙ"), target="sales_shadow")
    в_теневой = ch.command("SELECT count() FROM sales_shadow")

    swap_shadow(ch)
    assert ch.command("SELECT count() FROM sales") == в_теневой

    # Возвращаем витрину на место. Тест, оставляющий её усечённой, — это
    # уже не тест: один прогон на сервере стёр бы боевые данные.
    swap_shadow(ch)
    assert ch.command("SELECT count() FROM sales") == было_в_витрине
