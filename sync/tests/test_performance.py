"""Замеры на объёме, близком к боевому. Запускаются отдельно: pytest -m slow"""
import time

import pytest

from gfd_sync.clients import ch_client
from gfd_sync.schema import create_dictionaries, create_sales_tables

pytestmark = pytest.mark.slow

# Порог объёма: на домашней машине витрина наполняется 12 млн строк
# (см. docs/измерения.md). На сервере цифры снимаются на боевых данных.
МИНИМУМ_СТРОК = 10_000_000

# Запрос, который сейчас кормит верхний график в Qlik: объём в литрах
# по месяцам и годам. Литраж берётся из словаря — та самая проверка,
# что отказ от денормализации не стоит нам скорости.
ГЛАВНЫЙ_ЗАПРОС = """
    SELECT toYYYYMM(pdate) AS ym,
           sum(salesitem * dictGetFloat64('dict_product', 'litrag', tuple(xcode))) AS litres
    FROM sales
    WHERE pdate >= '2026-01-01' AND pdate < '2027-01-01'
      AND dictGetString('dict_product', 'category', tuple(xcode)) = 'ЭНЕРГЕТИКИ'
    GROUP BY ym ORDER BY ym
"""

АКБ_ЗАПРОС = """
    SELECT toYYYYMM(pdate) AS ym,
           uniqExact(store_no) AS akb
    FROM sales
    WHERE pdate >= '2026-01-01' AND pdate < '2027-01-01'
      AND dictGetString('dict_product', 'brand', tuple(xcode)) = 'ADRENALINE'
    GROUP BY ym ORDER BY ym
"""


@pytest.fixture(scope="module")
def подготовленная_витрина():
    ch = ch_client()
    create_sales_tables(ch)
    create_dictionaries(ch)
    n = ch.command("SELECT count() FROM sales")
    assert n > МИНИМУМ_СТРОК, (
        f"в витрине {n} строк. Сначала: python tools/gen_fake_data.py --rows 20000000, "
        f"затем gfd-sync init && gfd-sync full-reload --yes")
    return ch


def test_главный_запрос_быстрее_секунды(подготовленная_витрина):
    ch = подготовленная_витрина
    ch.command("SYSTEM DROP MARK CACHE")
    started = time.monotonic()
    rows = ch.query(ГЛАВНЫЙ_ЗАПРОС).result_rows
    elapsed = time.monotonic() - started
    assert len(rows) > 0
    assert elapsed < 1.0, f"главный запрос занял {elapsed:.2f} с"


def test_акб_быстрее_двух_секунд(подготовленная_витрина):
    ch = подготовленная_витрина
    started = time.monotonic()
    ch.query(АКБ_ЗАПРОС).result_rows
    elapsed = time.monotonic() - started
    assert elapsed < 2.0, f"АКБ занял {elapsed:.2f} с"


def test_витрина_компактнее_источника(подготовленная_витрина):
    ch = подготовленная_витрина
    gb = ch.command("""
        SELECT round(sum(bytes_on_disk) / 1024 / 1024 / 1024, 2)
        FROM system.parts WHERE table = 'sales' AND active
    """)
    assert float(gb) < 20, f"витрина занимает {gb} ГБ, ожидалось меньше 20"
