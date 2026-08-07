"""Генератор синтетических данных, повторяющих боевую gfd_database_scandata.

Данные из рабочего контура не выносятся, поэтому вся разработка идёт на этом.
Объём задаётся параметром — можно проверить поведение и на объёмах больше боевых.
"""
from __future__ import annotations

import os
import random
from datetime import date

import psycopg

MARKER = "synthetic_data_marker"

SCHEMA = f"""
DROP TABLE IF EXISTS sales, address, kib_monthly_data, load_log, product CASCADE;

-- Метка синтетики. По ней генератор отличает свою базу от чужой и
-- отказывается сносить данные, которых не создавал.
CREATE TABLE IF NOT EXISTS {MARKER} (
    created_at timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE product (
    id bigserial PRIMARY KEY,
    xcode varchar(50) NOT NULL UNIQUE,
    xname text,
    catlitrag varchar(50),
    brand varchar(100),
    category varchar(25),
    proizvod varchar(100),
    litrag numeric(10,3),
    pack varchar(10),
    vkus varchar(100),
    sku text GENERATED ALWAYS AS (
        trim(coalesce(brand,'') || ' ' || coalesce(vkus,''))) STORED
);

CREATE TABLE address (
    id bigserial PRIMARY KEY,
    store_no varchar(50) NOT NULL,
    client varchar(100) NOT NULL,
    store_add text NOT NULL,
    store_format varchar(100),
    region varchar(100),
    oblast varchar(100),
    city varchar(100),
    first_seen date,
    last_seen date,
    UNIQUE (store_no, client, store_add)
);

CREATE TABLE sales (
    id bigserial PRIMARY KEY,
    store_no varchar(50) NOT NULL,
    xcode varchar(50) NOT NULL REFERENCES product(xcode),
    pdate date,
    salesitem numeric(15,2),
    salesvalue numeric(15,2),
    client varchar(100) NOT NULL,
    name_file varchar(255),
    opt varchar(5)
);
CREATE INDEX idx_sales_client_date ON sales (upper(trim(client)), pdate);
CREATE INDEX idx_sales_xcode ON sales (xcode);

CREATE TABLE kib_monthly_data (
    id bigserial PRIMARY KEY,
    date date NOT NULL,
    product text NOT NULL,
    akb integer NOT NULL CHECK (akb >= 0),
    created_at timestamptz DEFAULT now(),
    UNIQUE (date, product)
);

CREATE TABLE load_log (
    id serial PRIMARY KEY,
    file_path text NOT NULL UNIQUE,
    file_hash varchar(32) NOT NULL,
    rows_loaded integer,
    loaded_at timestamp DEFAULT now(),
    status varchar(20) DEFAULT 'completed'
);
"""

BRANDS = ["ADRENALINE", "ЧЕРНОГОЛОВКА", "GORILLA", "TORNADO", "VOLT",
          "DRIVE ME", "FLASH UP", "BURN", "RED BULL", "MONSTER"]
MAKERS = ["ПЕПСИКО", "ЧЕРНОГОЛОВКА", "КОКА-КОЛА", "ГЛОБАЛ ФУДС", "ОЧАКОВО"]
VKUS = ["RUSH", "ORIGINAL", "MANGO", "ЛЕСНЫЕ ЯГОДЫ", "ТРОПИК", "ЦИТРУС", "БЕЗ САХАРА"]
CATEGORIES = ["энергетики", "газировка", "вода", "соки", "прочее"]
PACKS = ["ЖБ", "ПЭТ", "СТ"]
LITRAG = [0.25, 0.33, 0.449, 0.5, 1.0, 1.5, 2.0]

# Сети с весами. Первые четыре из EXCLUDED должны отсеиваться при заливке —
# они здесь специально, чтобы фильтр было чем проверять.
CHAINS = [("МАГНИТ", 27), ("ПЯТЁРОЧКА", 24), ("ЛЕНТА", 12), ("ПЕРЕКРЁСТОК", 9),
          ("ДИКСИ", 7), ("АШАН", 5), ("ОКЕЙ", 4), ("ВЕРНЫЙ", 4),
          ("КАРУСЕЛЬ", 3), ("ДОМ ЛЕНТА", 2), ("ЛЕНТА ЗООМАРКЕТ", 2), ("ПЯТЁРОЧКА РЦ", 1)]
CITIES = [("МОСКВА", "МОСКОВСКАЯ"), ("САНКТ-ПЕТЕРБУРГ", "ЛЕНИНГРАДСКАЯ"),
          ("КАЗАНЬ", "ТАТАРСТАН"), ("НОВОСИБИРСК", "НОВОСИБИРСКАЯ"),
          ("ЕКАТЕРИНБУРГ", "СВЕРДЛОВСКАЯ")]
FORMATS = ["ГИПЕРМАРКЕТ", "СУПЕРМАРКЕТ", "У ДОМА", "ДИСКАУНТЕР"]
SEASON = [.82, .80, .94, 1.00, 1.12, 1.24, 1.31, 1.28, 1.05, .92, .85, .97]


def _убедиться_что_база_синтетическая(conn) -> None:
    """Отказывается работать с базой, которую генератор не создавал.

    Первое, что делает генератор, — сносит sales, product и address. Дай
    ему боевой DSN (а он лежит в той же переменной окружения, что и для
    синхронизатора) — и данных не станет. Своя база помечена таблицей
    synthetic_data_marker; нет метки, но есть продажи — значит база чужая.
    """
    есть_продажи = conn.execute(
        "SELECT to_regclass('public.sales') IS NOT NULL").fetchone()[0]
    есть_метка = conn.execute(
        f"SELECT to_regclass('public.{MARKER}') IS NOT NULL").fetchone()[0]
    if есть_продажи and not есть_метка:
        if os.environ.get("GFD_FAKE_DATA_FORCE") == "1":
            return          # осознанное разрешение: база тестовая, метки ещё нет
        raise RuntimeError(
            "в базе есть таблица sales, но нет метки синтетики "
            f"({MARKER}). Похоже на боевую базу — пересоздавать отказываюсь. "
            "Если база точно тестовая, повторите с GFD_FAKE_DATA_FORCE=1."
        )


def generate(dsn: str, rows: int, year: int = 2026, seed: int = 42) -> dict[str, int]:
    """Пересоздаёт схему и наполняет её. Один seed — одни и те же данные."""
    rnd = random.Random(seed)

    products = []
    for i in range(600):
        brand = rnd.choice(BRANDS)
        vkus = rnd.choice(VKUS)
        lit = rnd.choice(LITRAG)
        products.append((
            f"X{i:06d}",                                    # xcode
            f"{brand} {vkus} {lit}Л {rnd.choice(PACKS)}",   # xname
            f"{lit}Л",                                      # catlitrag
            brand, rnd.choice(CATEGORIES), rnd.choice(MAKERS),
            lit, rnd.choice(PACKS), vkus,
        ))

    stores = []
    # Номер сети в коде точки обязателен: АКБ считается как uniq(store_no),
    # без оглядки на сеть. С одним лишь буквенным префиксом «ЛЕНТА» и
    # «ЛЕНТА ЗООМАРКЕТ» (равно как «ПЯТЁРОЧКА» и «ПЯТЁРОЧКА РЦ») делили бы
    # номера, а это пары «включаемая сеть / исключаемая»: сломайся фильтр
    # сетей, лишние точки схлопнулись бы в существующие и АКБ бы не дрогнул.
    for ci, (chain, _) in enumerate(CHAINS):
        for i in range(220):
            city, oblast = rnd.choice(CITIES)
            stores.append((f"{chain[:3]}{ci:02d}{i:04d}", chain,
                           f"{city}, ул. Тестовая, {i}", rnd.choice(FORMATS),
                           "ЦФО", oblast, city))

    chain_names = [c for c, _ in CHAINS]
    chain_weights = [w for _, w in CHAINS]
    by_chain: dict[str, list[str]] = {}
    for store_no, chain, *_ in stores:
        by_chain.setdefault(chain, []).append(store_no)

    with psycopg.connect(dsn, autocommit=True) as conn:
        _убедиться_что_база_синтетическая(conn)
        conn.execute(SCHEMA)

        with conn.cursor().copy(
            "COPY product (xcode, xname, catlitrag, brand, category, proizvod, "
            "litrag, pack, vkus) FROM STDIN"
        ) as cp:
            for p in products:
                cp.write_row(p)

        with conn.cursor().copy(
            "COPY address (store_no, client, store_add, store_format, region, "
            "oblast, city) FROM STDIN"
        ) as cp:
            for s in stores:
                cp.write_row(s)

        with conn.cursor().copy(
            "COPY sales (store_no, xcode, pdate, salesitem, salesvalue, client, "
            "name_file, opt) FROM STDIN"
        ) as cp:
            for _ in range(rows):
                chain = rnd.choices(chain_names, weights=chain_weights)[0]
                store_no = rnd.choice(by_chain[chain])
                prod = rnd.choice(products)
                month = rnd.randrange(1, 13)
                items = round(rnd.uniform(1, 40) * SEASON[month - 1], 2)
                price = round(rnd.uniform(45, 190), 2)
                cp.write_row((
                    # В боевой базе продажи агрегированы по месяцам: дата —
                    # всегда первое число. Синтетика обязана быть такой же,
                    # иначе замеры сжатия и сортировки врут в нашу пользу.
                    store_no, prod[0], date(year, month, 1),
                    items, round(items * price, 2), chain,
                    f"{chain}_{year}_{month:02d}.xlsx",
                    "1" if rnd.random() < 0.04 else None,
                ))

        # журнал загрузок: по файлу на каждую пару «сеть × месяц»
        with conn.cursor().copy(
            "COPY load_log (file_path, file_hash, rows_loaded) FROM STDIN"
        ) as cp:
            n_log = 0
            for chain in chain_names:
                for month in range(1, 13):
                    cp.write_row((
                        f"/mnt/gfd_data/in/{chain}_{year}_{month:02d}.xlsx",
                        f"{rnd.getrandbits(128):032x}",
                        rnd.randrange(1000, 90000),
                    ))
                    n_log += 1

        conn.execute("ANALYZE")

    return {"product": len(products), "address": len(stores),
            "sales": rows, "load_log": n_log}


if __name__ == "__main__":
    import argparse
    import os

    ap = argparse.ArgumentParser(description="Наполнить источник синтетикой")
    ap.add_argument("--rows", type=int, default=1_000_000)
    ap.add_argument("--year", type=int, default=2026)
    ap.add_argument("--seed", type=int, default=42)
    args = ap.parse_args()

    result = generate(os.environ["PG_SOURCE_DSN"], args.rows, args.year, args.seed)
    print(f"создано: {result}")
