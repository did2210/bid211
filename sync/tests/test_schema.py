"""Схема витрины. Ключ партиционирования и порядок сортировки —
основа всей дальнейшей работы, поэтому проверяются явно."""
from gfd_sync.clients import ch_client
from gfd_sync.schema import SALES_COLUMNS, create_sales_tables


def колонки(ch, таблица):
    # Фильтр по базе обязателен: одноимённые таблицы есть и в витрине,
    # и в тестовой базе, а без него в выборку попадут обе.
    return [(r[0], r[1]) for r in ch.query(
        f"SELECT name, type FROM system.columns WHERE table = '{таблица}' "
        "AND database = currentDatabase() ORDER BY position").result_rows]


def test_таблицы_создаются():
    ch = ch_client()
    create_sales_tables(ch)
    names = {r[0] for r in ch.query("SHOW TABLES").result_rows}
    assert {"sales", "sales_staging"} <= names


def test_ключ_партиционирования_месяц_и_сеть():
    ch = ch_client()
    create_sales_tables(ch)
    key = ch.command(
        "SELECT partition_key FROM system.tables WHERE name = 'sales' AND database = currentDatabase()")
    assert "toYYYYMM(pdate)" in key
    assert "client" in key


def test_сортировка_по_коду_товара():
    ch = ch_client()
    create_sales_tables(ch)
    key = ch.command(
        "SELECT sorting_key FROM system.tables WHERE name = 'sales' AND database = currentDatabase()")
    assert key.startswith("xcode")


def test_структура_промежуточной_совпадает_с_основной():
    """REPLACE PARTITION требует идентичной структуры, иначе подмена упадёт."""
    ch = ch_client()
    create_sales_tables(ch)
    assert колонки(ch, "sales") == колонки(ch, "sales_staging")


def test_порядок_колонок_зафиксирован():
    assert SALES_COLUMNS == ("src_id", "pdate", "client", "store_no",
                             "xcode", "salesitem", "salesvalue", "opt")


def test_порядок_колонок_совпадает_с_таблицей():
    """Вставка идёт по позициям, а не по именам.

    Разойдись SALES_COLUMNS с порядком колонок в SQL — значения молча
    поменяются местами: цена уедет в количество, а сеть в номер точки.
    Ошибка такого рода не падает, а портит витрину.
    """
    ch = ch_client()
    create_sales_tables(ch)
    assert SALES_COLUMNS == tuple(имя for имя, _ in колонки(ch, "sales"))


def test_повторный_вызов_не_ломает_данные():
    ch = ch_client()
    create_sales_tables(ch)
    ch.command("TRUNCATE TABLE IF EXISTS sales")
    ch.command("INSERT INTO sales VALUES "
               "(1, '2026-07-01', 'ЛЕНТА', 'S1', 'X000001', 1, 100, 0)")
    create_sales_tables(ch)
    assert ch.command("SELECT count() FROM sales") == 1
    ch.command("TRUNCATE TABLE sales")
