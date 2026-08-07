"""Словари — живая ссылка на PostgreSQL. Проверяем, что данные видны
и что правка в источнике доезжает без перезаливки продаж."""
import os

import psycopg

from gfd_sync.clients import ch_client
from gfd_sync.schema import create_dictionaries


def test_словари_созданы():
    ch = ch_client()
    create_dictionaries(ch)
    names = {r[0] for r in ch.query("SHOW DICTIONARIES").result_rows}
    assert {"dict_product", "dict_address", "dict_client_map"} <= names


def test_атрибуты_товара_доступны():
    ch = ch_client()
    create_dictionaries(ch)
    brand = ch.command(
        "SELECT dictGetString('dict_product', 'brand', tuple('X000001'))")
    assert brand != "", "бренд должен подтянуться из PostgreSQL"


def test_бренд_приводится_к_верхнему_регистру():
    ch = ch_client()
    create_dictionaries(ch)
    brand = ch.command(
        "SELECT dictGetString('dict_product', 'brand', tuple('X000001'))")
    assert brand == brand.upper()


def test_литраж_числовой():
    ch = ch_client()
    create_dictionaries(ch)
    litrag = ch.command(
        "SELECT dictGetFloat64('dict_product', 'litrag', tuple('X000001'))")
    # command() отдаёт скалярный ответ текстом, поэтому приводим явно.
    assert float(litrag) > 0


def test_правка_в_источнике_видна_после_перезагрузки_словаря():
    """Это и есть обещанное «поправил вкус — через минуту в BI»."""
    ch = ch_client()
    create_dictionaries(ch)
    with psycopg.connect(os.environ["PG_SOURCE_DSN"], autocommit=True) as conn:
        было = conn.execute(
            "SELECT vkus FROM product WHERE xcode = 'X000001'").fetchone()[0]
        conn.execute(
            "UPDATE product SET vkus = 'ПРОВЕРКА СЛОВАРЯ' WHERE xcode = 'X000001'")
        try:
            ch.command("SYSTEM RELOAD DICTIONARY dict_product")
            assert ch.command(
                "SELECT dictGetString('dict_product', 'vkus', tuple('X000001'))"
            ) == "ПРОВЕРКА СЛОВАРЯ"
        finally:
            # Синтетика должна остаться такой же, какой её сделал генератор:
            # иначе следующий тест поймает чужую правку и не поймёт, откуда она.
            conn.execute(
                "UPDATE product SET vkus = %s WHERE xcode = 'X000001'", (было,))
            ch.command("SYSTEM RELOAD DICTIONARY dict_product")


def test_адрес_по_составному_ключу():
    """Ключ словаря адресов — пара (store_no, client), а не одно поле:
    один и тот же номер магазина встречается у разных сетей."""
    ch = ch_client()
    create_dictionaries(ch)
    with psycopg.connect(os.environ["PG_SOURCE_DSN"]) as conn:
        store_no, client = conn.execute(
            "SELECT store_no, upper(client) FROM address LIMIT 1").fetchone()
    city = ch.command(
        "SELECT dictGetString('dict_address', 'city', tuple(%(s)s, %(c)s))",
        parameters={"s": store_no, "c": client})
    assert city != ""


def test_переименование_сети_подтягивается():
    """Словарь создаётся лениво: SHOW DICTIONARIES покажет его и при
    недоступном источнике или отсутствующей таблице client_map.
    Значение спрашиваем явно — иначе правила переименования могли бы
    молча не приехать, а сети остались бы под старыми именами.
    """
    ch = ch_client()
    create_dictionaries(ch)
    assert ch.command(
        "SELECT dictGetString('dict_client_map', 'new_client', tuple('ЧИЖИК'))"
    ) == "X5 ЧИЖИК"
