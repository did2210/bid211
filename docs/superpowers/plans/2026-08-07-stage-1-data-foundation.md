# GFD BI · Этап 1 · Фундамент данных — план реализации

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Построить читающую витрину на ClickHouse поверх боевой `gfd_database_scandata` и синхронизатор, который наполняет её кусками «месяц × сеть», сам находит расхождения и чинит их.

**Architecture:** Синхронизатор на Python читает PostgreSQL через `COPY ... TO STDOUT` и переливает поток в ClickHouse без построчной обработки. Каждый кусок заливается в промежуточную таблицу, сверяется с источником и подменяется атомарной операцией `REPLACE PARTITION`. Справочники не копируются — подключены словарями поверх PostgreSQL с перечиткой раз в минуту.

**Tech Stack:** ClickHouse 25.x, PostgreSQL 16 (служебный, в контейнере), Python 3.12, psycopg 3, clickhouse-connect, click, pytest, Docker Compose.

## Global Constraints

- **Боевой PostgreSQL только читается.** Ни `INSERT`, ни `UPDATE`, ни `ALTER`, ни изменение настроек, ни перезапуск. Любой код, пишущий в источник, — дефект.
- **Данные из рабочего контура не выносятся.** Разработка ведётся исключительно на синтетике из `tools/gen_fake_data.py`.
- **Секреты в git не попадают.** Только `.env.example` с пустыми значениями.
- Фильтры заливки: дата (тест — 2026 год), исключение сетей `ДОМ ЛЕНТА`, `КАРУСЕЛЬ`, `ЛЕНТА ЗООМАРКЕТ`, `ПЯТЁРОЧКА РЦ`. **Фильтр по категории не применяется.**
- Партиционирование: `(toYYYYMM(pdate), client)`, где `client` — **исходное** имя сети из источника в верхнем регистре, до применения `ClientMap`.
- Лимиты ClickHouse: `max_server_memory_usage=40Gi`, `max_memory_usage=10Gi`, `max_threads=8`, `max_execution_time=60`.
- Код и комментарии — на русском, идентификаторы — латиницей.
- Пути в коде — только POSIX-стиль, целевая система Ubuntu.

---

## Структура файлов

```
gfd-bi/
├── docker-compose.yml              контейнеры: clickhouse, pg_app, pg_source (dev)
├── .env.example                    шаблон настроек
├── clickhouse/
│   ├── config.d/limits.xml         лимиты памяти и процессора
│   └── schema/
│       ├── 01_sales.sql            таблица продаж и промежуточная
│       └── 02_dictionaries.sql     словари product / address / client_map
├── pg_app/
│   └── init/01_journal.sql         таблица журнала синхронизации
├── sync/
│   ├── pyproject.toml
│   ├── src/gfd_sync/
│   │   ├── config.py               настройки из окружения
│   │   ├── clients.py              соединения с PostgreSQL и ClickHouse
│   │   ├── chunks.py               модель куска «месяц × сеть»
│   │   ├── schema.py               создание таблиц и словарей
│   │   ├── reader.py               чтение куска из PostgreSQL
│   │   ├── loader.py               заливка и атомарная подмена
│   │   ├── verifier.py             сверка агрегатов
│   │   ├── journal.py              журнал синхронизации
│   │   ├── detector.py             обнаружение изменений по load_log
│   │   └── cli.py                  команды
│   └── tests/
└── tools/
    └── gen_fake_data.py            генератор синтетических данных
```

---

### Task 1: Каркас проекта и контейнеры

**Files:**
- Create: `docker-compose.yml`, `.env.example`, `clickhouse/config.d/limits.xml`
- Create: `clickhouse/users.d/limits.xml`
- Create: `sync/pyproject.toml`, `sync/src/gfd_sync/__init__.py`
- Test: `sync/tests/test_smoke.py`

**Interfaces:**
- Consumes: ничего
- Produces: поднятые сервисы `clickhouse` (порт 8123), `pg_app` (5433), `pg_source` (5434, только профиль `dev`)

- [ ] **Step 1: Написать падающий тест**

`sync/tests/test_smoke.py`:

Проверяем не только «сервисы отвечают», но и что каждая настройка из наших
конфигов действительно доехала до сервера: и config.d, и users.d молча
игнорируют то, что положено не туда.

```python
"""Проверка, что инфраструктура поднята и отвечает."""
import os

import clickhouse_connect
import psycopg
import pytest

# Потолок памяти сервера из clickhouse/config.d/limits.xml.
ПОТОЛОК_ПАМЯТИ = 40 * 1024**3


def ch_клиент():
    return clickhouse_connect.get_client(
        host=os.environ["CH_HOST"], port=int(os.environ["CH_PORT"]),
        username=os.environ["CH_USER"], password=os.environ["CH_PASSWORD"],
    )


def test_clickhouse_отвечает():
    assert ch_клиент().command("SELECT 1") == 1


def test_clickhouse_потолок_памяти():
    """Потолок памяти сервера — 40 ГБ, но не больше, чем позволяет машина.

    ClickHouse сам опускает его до 90 % доступной оперативной памяти, если
    машина меньше боевой. На сервере (62 ГБ) сработает наши 40 ГБ,
    на машине разработчика — урезанное значение.
    """
    client = ch_клиент()
    предел = int(client.command(
        "SELECT value FROM system.server_settings WHERE name = 'max_server_memory_usage'"
    ))
    всего_памяти = int(client.command(
        "SELECT value FROM system.asynchronous_metrics WHERE metric = 'OSMemoryTotal'"
    ))
    ожидаемый = min(ПОТОЛОК_ПАМЯТИ, int(всего_памяти * 0.9))
    assert предел == pytest.approx(ожидаемый, rel=0.05)


@pytest.mark.parametrize("настройка, ожидание", [
    ("background_pool_size", "8"),
    ("background_merges_mutations_concurrency_ratio", "2"),
])
def test_clickhouse_фоновый_пул_урезан(настройка, ожидание):
    """Фоновые слияния делят диск с PostgreSQL, поэтому пул урезан вдвое."""
    значение = ch_клиент().command(
        f"SELECT value FROM system.server_settings WHERE name = '{настройка}'"
    )
    # command() приводит числовые строки к int, поэтому сравниваем как текст.
    assert str(значение) == ожидание


@pytest.mark.parametrize("настройка, ожидание", [
    ("number_of_free_entries_in_pool_to_execute_mutation", "10"),
    ("number_of_free_entries_in_pool_to_lower_max_size_of_merge", "4"),
    ("number_of_free_entries_in_pool_to_execute_optimize_entire_partition", "12"),
])
def test_clickhouse_пороги_merge_tree_согласованы_с_пулом(настройка, ожидание):
    """Пороги считаются от размера пула: с дефолтами сервер не стартует вовсе."""
    значение = ch_клиент().command(
        f"SELECT value FROM system.merge_tree_settings WHERE name = '{настройка}'"
    )
    assert str(значение) == ожидание


@pytest.mark.parametrize("настройка, ожидание", [
    ("max_memory_usage", str(10 * 1024**3)),
    ("max_threads", "8"),
    ("max_execution_time", "60"),
])
def test_clickhouse_ограничения_профиля_применены(настройка, ожидание):
    """Ограничения на отдельный запрос: профили задаются в users.d, не в config.d."""
    значение = ch_клиент().command(
        f"SELECT value FROM system.settings WHERE name = '{настройка}'"
    )
    assert str(значение) == ожидание


def test_служебный_postgres_отвечает():
    with psycopg.connect(os.environ["PG_APP_DSN"]) as conn:
        assert conn.execute("SELECT 1").fetchone()[0] == 1
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `cd sync && pytest tests/test_smoke.py -v`
Expected: FAIL — соединение отклонено, сервисов нет.

- [ ] **Step 3: Написать `docker-compose.yml`**

```yaml
services:
  clickhouse:
    image: clickhouse/clickhouse-server:25.3
    container_name: gfd_clickhouse
    ports:
      - "${CH_PORT:-8123}:8123"
      - "9000:9000"
    volumes:
      # Именно файлом, а не каталогом: монтирование каталога затирает штатный
      # docker_related_config.xml образа, который открывает прослушивание
      # наружу. Без него сервер слышен только внутри контейнера.
      - ./clickhouse/config.d/limits.xml:/etc/clickhouse-server/config.d/limits.xml:ro
      - ./clickhouse/users.d/limits.xml:/etc/clickhouse-server/users.d/limits.xml:ro
      - ch_data:/var/lib/clickhouse
    environment:
      CLICKHOUSE_USER: ${CH_USER}
      CLICKHOUSE_PASSWORD: ${CH_PASSWORD}
      CLICKHOUSE_DEFAULT_ACCESS_MANAGEMENT: 1
    ulimits:
      nofile: { soft: 262144, hard: 262144 }
    healthcheck:
      # Стучимся по сетевому имени, а не в localhost: сервер, слушающий
      # только loopback, отвечает на localhost и выглядит здоровым,
      # оставаясь недоступным и с хоста, и из соседних контейнеров.
      test: ["CMD", "wget", "--spider", "-q", "http://clickhouse:8123/ping"]
      interval: 5s
      retries: 12

  pg_app:
    image: postgres:16
    container_name: gfd_pg_app
    ports:
      - "${PG_APP_PORT:-5433}:5432"
    environment:
      POSTGRES_USER: ${PG_APP_USER}
      POSTGRES_PASSWORD: ${PG_APP_PASSWORD}
      POSTGRES_DB: ${PG_APP_DB}
    volumes:
      - pg_app_data:/var/lib/postgresql/data
      - ./pg_app/init:/docker-entrypoint-initdb.d:ro
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${PG_APP_USER}"]
      interval: 5s
      retries: 12

  # Только для разработки: имитация боевой базы. На сервере не поднимается.
  pg_source:
    image: postgres:16
    container_name: gfd_pg_source
    profiles: ["dev"]
    ports:
      - "${PG_SOURCE_PORT:-5434}:5432"
    environment:
      POSTGRES_USER: ${PG_SOURCE_USER}
      POSTGRES_PASSWORD: ${PG_SOURCE_PASSWORD}
      POSTGRES_DB: ${PG_SOURCE_DB}
    volumes:
      - pg_source_data:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U ${PG_SOURCE_USER}"]
      interval: 5s
      retries: 12

volumes:
  ch_data:
  pg_app_data:
  pg_source_data:
```

- [ ] **Step 4: Написать `clickhouse/config.d/limits.xml`**

```xml
<clickhouse>
    <!-- Потолок памяти всего сервера. Не резервирование: пока запросов нет,
         память остаётся боевому PostgreSQL под кеш страниц.
         22 ГБ запаса от 62 ГБ машины — страховка от того, что системный
         OOM killer выберет жертвой PostgreSQL. -->
    <max_server_memory_usage>42949672960</max_server_memory_usage>

    <!-- Фоновые слияния лезут на тот же диск, где работает PostgreSQL. -->
    <background_pool_size>8</background_pool_size>
    <background_merges_mutations_concurrency_ratio>2</background_merges_mutations_concurrency_ratio>

    <!-- Пороги свободных слотов пула считаются от дефолтного пула в 32 задачи
         (16 × 2). Мы урезали пул вдвое, до 16, — с дефолтами (20, 8, 25)
         сервер не стартует вовсе: проверка на старте считает такую
         конфигурацию неработоспособной. Значения уменьшены в той же
         пропорции. -->
    <merge_tree>
        <number_of_free_entries_in_pool_to_execute_mutation>10</number_of_free_entries_in_pool_to_execute_mutation>
        <number_of_free_entries_in_pool_to_lower_max_size_of_merge>4</number_of_free_entries_in_pool_to_lower_max_size_of_merge>
        <number_of_free_entries_in_pool_to_execute_optimize_entire_partition>12</number_of_free_entries_in_pool_to_execute_optimize_entire_partition>
    </merge_tree>

    <!-- Ограничения на отдельный запрос — в clickhouse/users.d/limits.xml:
         секция <profiles> действует только там. -->
</clickhouse>
```

- [ ] **Step 4б: Написать `clickhouse/users.d/limits.xml`**

Профили читаются только из `users.xml` и `users.d`. Та же секция, положенная
в `config.d`, применяется молча-никак: сервер стартует, а ограничения остаются
дефолтными.

```xml
<clickhouse>
    <profiles>
        <default>
            <!-- Один запрос не должен выедать весь потолок сервера: место
                 нужно остальным двадцати пяти пользователям. -->
            <max_memory_usage>10737418240</max_memory_usage>
            <max_threads>8</max_threads>
            <!-- Отчёт, считающийся дольше минуты, — ошибка в запросе,
                 а не терпеливый пользователь. -->
            <max_execution_time>60</max_execution_time>
        </default>
    </profiles>
</clickhouse>
```

- [ ] **Step 5: Написать `.env.example`**

```bash
# ClickHouse
CH_HOST=localhost
CH_PORT=8123
CH_USER=gfd
CH_PASSWORD=
CH_DB=default

# Служебная база приложения (журнал синхронизации)
PG_APP_USER=gfd_app
PG_APP_PASSWORD=
PG_APP_DB=gfd_app
PG_APP_PORT=5433
PG_APP_DSN=postgresql://gfd_app:@localhost:5433/gfd_app

# Источник. Локально — контейнер pg_source; на сервере — боевая база.
PG_SOURCE_USER=postgres
PG_SOURCE_PASSWORD=
PG_SOURCE_DB=gfd_database_scandata
PG_SOURCE_PORT=5434
PG_SOURCE_DSN=postgresql://postgres:@localhost:5434/gfd_database_scandata

# Границы заливки
LOAD_DATE_FROM=2026-01-01
LOAD_DATE_TO=2027-01-01
```

- [ ] **Step 6: Написать `sync/pyproject.toml`**

```toml
[project]
name = "gfd-sync"
version = "0.1.0"
requires-python = ">=3.12"
dependencies = [
    "psycopg[binary]>=3.2",
    "clickhouse-connect>=0.8",
    "pydantic-settings>=2.4",
    "click>=8.1",
]

[project.optional-dependencies]
dev = ["pytest>=8.3", "python-dotenv>=1.0"]

[project.scripts]
gfd-sync = "gfd_sync.cli:cli"

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.build.targets.wheel]
packages = ["src/gfd_sync"]

[tool.pytest.ini_options]
testpaths = ["tests"]
# ".." даёт тестам доступ к пакету tools в корне проекта
pythonpath = ["src", ".."]
```

- [ ] **Step 7: Написать `sync/tests/conftest.py`**

Без него тесты не увидят настроек: они лежат в `.env` в корне проекта, а pytest
переменные окружения сам не читает.

```python
"""Общая подготовка тестов: настройки берутся из .env в корне проекта."""
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")
```

- [ ] **Step 8: Поднять сервисы и прогнать тест**

Окружение создаётся через `uv` — он уже стоит на машине и сам поставит нужную
версию Python. Системный интерпретатор не трогаем: 3.14 слишком свежий, часть
пакетов ещё без готовых сборок под него.

```bash
cp .env.example .env      # заполнить пароли
docker compose --profile dev up -d

cd sync
uv venv --python 3.12
uv pip install -e ".[dev]"
uv run pytest tests/test_smoke.py -v
```

Дальше во всех задачах вместо `pytest ...` запускать `uv run pytest ...`
из каталога `sync`.

Expected: 11 тестов PASS.

- [ ] **Step 9: Коммит**

```bash
git add docker-compose.yml .env.example clickhouse/ sync/
git commit -m "Каркас: контейнеры ClickHouse и PostgreSQL, лимиты памяти"
```

---

### Task 2: Генератор синтетических данных

**Files:**
- Create: `tools/gen_fake_data.py`
- Test: `sync/tests/test_gen_fake_data.py`

**Interfaces:**
- Consumes: `PG_SOURCE_DSN`
- Produces: функция `generate(dsn: str, rows: int, year: int = 2026, seed: int = 42) -> dict[str, int]` — создаёт схему боевой базы и наполняет её; возвращает `{"product": n, "address": n, "sales": n, "load_log": n}`

- [ ] **Step 1: Написать падающий тест**

`sync/tests/test_gen_fake_data.py`:

```python
"""Генератор должен воспроизводить схему боевой базы и давать правдоподобные данные."""
import os
import psycopg
import pytest
from tools.gen_fake_data import generate


@pytest.fixture(scope="module")
def сгенерированная_база():
    dsn = os.environ["PG_SOURCE_DSN"]
    counts = generate(dsn, rows=50_000, year=2026, seed=42)
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
    assert years == {2026}


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
    assert факт["sales"] == 50_000


def test_повторный_запуск_даёт_те_же_данные(сгенерированная_база):
    """Один seed — одни данные, иначе тесты сверки будут плавать."""
    dsn, _ = сгенерированная_база
    with psycopg.connect(dsn) as conn:
        first = conn.execute("SELECT sum(salesvalue) FROM sales").fetchone()[0]
    generate(dsn, rows=50_000, year=2026, seed=42)
    with psycopg.connect(dsn) as conn:
        second = conn.execute("SELECT sum(salesvalue) FROM sales").fetchone()[0]
    assert first == second
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `cd sync && pytest tests/test_gen_fake_data.py -v`
Expected: FAIL — `ModuleNotFoundError: tools.gen_fake_data`

- [ ] **Step 3: Написать генератор**

`tools/gen_fake_data.py`:

```python
"""Генератор синтетических данных, повторяющих боевую gfd_database_scandata.

Данные из рабочего контура не выносятся, поэтому вся разработка идёт на этом.
Объём задаётся параметром — можно проверить поведение и на объёмах больше боевых.
"""
from __future__ import annotations

import random
from datetime import date, timedelta

import psycopg

SCHEMA = """
DROP TABLE IF EXISTS sales, address, kib_monthly_data, load_log, product CASCADE;

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
                day = rnd.randrange(1, 29)
                items = round(rnd.uniform(1, 40) * SEASON[month - 1], 2)
                price = round(rnd.uniform(45, 190), 2)
                cp.write_row((
                    store_no, prod[0], date(year, month, day),
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
```

- [ ] **Step 4: Прогнать тесты**

Run: `cd sync && pytest tests/test_gen_fake_data.py -v`
Expected: девять тестов PASS.

- [ ] **Step 5: Коммит**

```bash
git add tools/gen_fake_data.py sync/tests/test_gen_fake_data.py
git commit -m "Генератор синтетических данных по схеме боевой базы"
```

---

### Task 3: Конфигурация и клиенты подключений

**Files:**
- Create: `sync/src/gfd_sync/config.py`, `sync/src/gfd_sync/clients.py`
- Test: `sync/tests/test_config.py`

**Interfaces:**
- Consumes: переменные окружения из `.env`
- Produces:
  - `Settings` — поля `ch_host: str`, `ch_port: int`, `ch_user: str`, `ch_password: str`, `ch_db: str`, `pg_source_dsn: str`, `pg_app_dsn: str`, `load_date_from: date`, `load_date_to: date`, `excluded_chains: tuple[str, ...]`
  - `get_settings() -> Settings`
  - `ch_client() -> clickhouse_connect.driver.Client`
  - `pg_source() -> psycopg.Connection` (соединение только для чтения)
  - `pg_app() -> psycopg.Connection`

- [ ] **Step 1: Написать падающий тест**

`sync/tests/test_config.py`:

```python
"""Настройки читаются из окружения, соединение с источником — только на чтение."""
import psycopg
import pytest

from gfd_sync.clients import ch_client, pg_app, pg_source
from gfd_sync.config import get_settings
from tools.gen_fake_data import CHAINS


def test_настройки_читаются():
    s = get_settings()
    assert s.ch_port > 0
    assert s.pg_source_dsn.startswith("postgresql://")


def test_исключённые_сети_заданы():
    s = get_settings()
    assert set(s.excluded_chains) == {
        "ДОМ ЛЕНТА", "КАРУСЕЛЬ", "ЛЕНТА ЗООМАРКЕТ", "ПЯТЁРОЧКА РЦ"}


def test_исключённые_сети_есть_в_источнике():
    """Написание должно совпадать посимвольно с тем, что лежит в данных.

    Фильтр сравнивает названия как текст, поэтому «ПЯТЁРОЧКА РЦ» через «Е»
    вместо «Ё» не отсеет ничего и сделает это молча: заливка пройдёт,
    а в витрину приедут лишние сети.
    """
    сети_источника = {название for название, _ in CHAINS}
    assert set(get_settings().excluded_chains) <= сети_источника


def test_клиент_clickhouse_работает():
    assert ch_client().command("SELECT 1") == 1


def test_соединение_с_источником_запрещает_запись():
    """Гарантия того, что синхронизатор физически не может испортить боевую базу."""
    with pg_source() as conn:
        with pytest.raises(psycopg.errors.ReadOnlySqlTransaction):
            conn.execute("CREATE TABLE не_должно_создаться (x int)")


def test_служебная_база_писать_разрешает():
    with pg_app() as conn:
        conn.execute("CREATE TEMP TABLE проверка (x int)")
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `cd sync && pytest tests/test_config.py -v`
Expected: FAIL — `ModuleNotFoundError: gfd_sync.config`

- [ ] **Step 3: Написать `config.py`**

```python
"""Настройки синхронизатора. Всё берётся из окружения, ничего не зашито."""
from __future__ import annotations

from datetime import date
from pathlib import Path

from pydantic_settings import BaseSettings, SettingsConfigDict

# Сети, которые не попадают в витрину. Правило стабильное, поэтому применяется
# при заливке, а не в слое метрик.
EXCLUDED_CHAINS = ("ДОМ ЛЕНТА", "КАРУСЕЛЬ", "ЛЕНТА ЗООМАРКЕТ", "ПЯТЁРОЧКА РЦ")

# Путь считается от расположения модуля, а не от текущего каталога: команды
# синхронизатора запускаются и из cron, и из любого места на сервере,
# а «../.env» нашёлся бы только при запуске из каталога sync.
ENV_FILE = Path(__file__).resolve().parents[3] / ".env"


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=ENV_FILE, extra="ignore")

    ch_host: str = "localhost"
    ch_port: int = 8123
    ch_user: str = "gfd"
    ch_password: str = ""
    ch_db: str = "default"

    pg_source_dsn: str
    pg_app_dsn: str

    load_date_from: date = date(2026, 1, 1)
    load_date_to: date = date(2027, 1, 1)

    @property
    def excluded_chains(self) -> tuple[str, ...]:
        return EXCLUDED_CHAINS


def get_settings() -> Settings:
    return Settings()
```

- [ ] **Step 4: Написать `clients.py`**

```python
"""Соединения. Источник открывается строго на чтение — это защита от опечатки,
которая иначе может испортить боевую базу."""
from __future__ import annotations

import clickhouse_connect
import psycopg
from clickhouse_connect.driver import Client

from .config import get_settings


def ch_client() -> Client:
    s = get_settings()
    return clickhouse_connect.get_client(
        host=s.ch_host, port=s.ch_port,
        username=s.ch_user, password=s.ch_password, database=s.ch_db,
    )


def pg_source() -> psycopg.Connection:
    """Соединение с источником. read_only включён на уровне сессии:
    любая попытка записи упадёт с ReadOnlySqlTransaction."""
    conn = psycopg.connect(get_settings().pg_source_dsn)
    conn.read_only = True
    return conn


def pg_app() -> psycopg.Connection:
    return psycopg.connect(get_settings().pg_app_dsn, autocommit=True)
```

- [ ] **Step 5: Прогнать тесты**

Run: `cd sync && pytest tests/test_config.py -v`
Expected: шесть тестов PASS.

- [ ] **Step 6: Коммит**

```bash
git add sync/src/gfd_sync/config.py sync/src/gfd_sync/clients.py sync/tests/test_config.py
git commit -m "Настройки и соединения; источник открывается только на чтение"
```

---

### Task 4: Модель куска «месяц × сеть»

**Files:**
- Create: `sync/src/gfd_sync/chunks.py`
- Test: `sync/tests/test_chunks.py`

**Interfaces:**
- Consumes: `Settings` из Task 3
- Produces:
  - `Chunk` — неизменяемый класс с полями `year: int`, `month: int`, `chain: str`; свойства `partition_id -> tuple[int, str]` (например `(202607, "МАГНИТ")`), `date_from -> date`, `date_to -> date` (полуинтервал), `key -> str` (вид `"202607/МАГНИТ"`)
  - `chunk_from_key(key: str) -> Chunk`
  - `chunks_in_period(date_from, date_to, chains) -> list[Chunk]`
  - `chunk_of(pdate: date, chain: str) -> Chunk`

- [ ] **Step 1: Написать падающий тест**

`sync/tests/test_chunks.py`:

```python
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
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `cd sync && pytest tests/test_chunks.py -v`
Expected: FAIL — `ModuleNotFoundError: gfd_sync.chunks`

- [ ] **Step 3: Написать `chunks.py`**

```python
"""Кусок данных — пара «календарный месяц + торговая сеть».

Такая нарезка выбрана потому, что файлы от сетей приходят в разное время:
пришёл Магнит за июль — подменяем только его, остальное не трогаем.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True, order=True)
class Chunk:
    year: int
    month: int
    chain: str

    def __post_init__(self) -> None:
        # Кусок с месяцем 13 дожил бы до имени партиции или до пустой выборки
        # и всплыл бы далеко от места, где его собрали.
        if not 1 <= self.month <= 12:
            raise ValueError(f"месяц вне 1..12: {self.month}")

    @property
    def partition_id(self) -> tuple[int, str]:
        """Значение ключа партиции ClickHouse: (toYYYYMM(pdate), client)."""
        return self.year * 100 + self.month, self.chain

    @property
    def date_from(self) -> date:
        return date(self.year, self.month, 1)

    @property
    def date_to(self) -> date:
        """Верхняя граница не включается."""
        return date(self.year + 1, 1, 1) if self.month == 12 \
            else date(self.year, self.month + 1, 1)

    @property
    def key(self) -> str:
        return f"{self.year * 100 + self.month}/{self.chain}"


def chunk_from_key(key: str) -> Chunk:
    ym, chain = key.split("/", 1)
    return Chunk(int(ym[:4]), int(ym[4:]), chain)


def chunk_of(pdate: date, chain: str) -> Chunk:
    return Chunk(pdate.year, pdate.month, chain)


def chunks_in_period(date_from: date, date_to: date,
                     chains: list[str]) -> list[Chunk]:
    """Все куски, пересекающиеся с полуинтервалом [date_from, date_to)."""
    out: list[Chunk] = []
    y, m = date_from.year, date_from.month
    while date(y, m, 1) < date_to:
        for chain in chains:
            out.append(Chunk(y, m, chain))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out
```

- [ ] **Step 4: Прогнать тесты**

Run: `cd sync && pytest tests/test_chunks.py -v`
Expected: десять тестов PASS.

- [ ] **Step 5: Коммит**

```bash
git add sync/src/gfd_sync/chunks.py sync/tests/test_chunks.py
git commit -m "Модель куска «месяц × сеть»"
```

---

### Task 5: Схема ClickHouse

**Files:**
- Create: `clickhouse/schema/01_sales.sql`, `sync/src/gfd_sync/schema.py`
- Test: `sync/tests/test_schema.py`

**Interfaces:**
- Consumes: `ch_client()` из Task 3
- Produces:
  - `create_sales_tables(client) -> None` — создаёт `sales` и `sales_staging`
  - `SALES_COLUMNS: tuple[str, ...]` — порядок колонок, общий для чтения и вставки: `("src_id", "pdate", "client", "store_no", "xcode", "salesitem", "salesvalue", "opt")`

- [ ] **Step 1: Написать падающий тест**

`sync/tests/test_schema.py`:

```python
"""Схема витрины. Ключ партиционирования и порядок сортировки —
основа всей дальнейшей работы, поэтому проверяются явно."""
from gfd_sync.clients import ch_client
from gfd_sync.schema import SALES_COLUMNS, create_sales_tables


def колонки(ch, таблица):
    return [(r[0], r[1]) for r in ch.query(
        f"SELECT name, type FROM system.columns WHERE table = '{таблица}' "
        "ORDER BY position").result_rows]


def test_таблицы_создаются():
    ch = ch_client()
    create_sales_tables(ch)
    names = {r[0] for r in ch.query("SHOW TABLES").result_rows}
    assert {"sales", "sales_staging"} <= names


def test_ключ_партиционирования_месяц_и_сеть():
    ch = ch_client()
    create_sales_tables(ch)
    key = ch.command(
        "SELECT partition_key FROM system.tables WHERE name = 'sales'")
    assert "toYYYYMM(pdate)" in key
    assert "client" in key


def test_сортировка_по_коду_товара():
    ch = ch_client()
    create_sales_tables(ch)
    key = ch.command(
        "SELECT sorting_key FROM system.tables WHERE name = 'sales'")
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
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `cd sync && pytest tests/test_schema.py -v`
Expected: FAIL — `ModuleNotFoundError: gfd_sync.schema`

- [ ] **Step 3: Написать `clickhouse/schema/01_sales.sql`**

```sql
-- Витрина продаж. Хранятся только факты: справочники подключены словарями,
-- поэтому бренд и вкус здесь не дублируются и правятся без перезаливки.
CREATE TABLE IF NOT EXISTS sales
(
    src_id     Int64                  COMMENT 'id из PostgreSQL, нужен для сверки',
    pdate      Date,
    client     LowCardinality(String) COMMENT 'исходное имя сети, до ClientMap',
    store_no   String CODEC(ZSTD(3)),
    xcode      LowCardinality(String),
    salesitem  Decimal(15, 2),
    salesvalue Decimal(15, 2),
    opt        UInt8                  COMMENT '1 — оптовая продажа'
)
ENGINE = MergeTree
PARTITION BY (toYYYYMM(pdate), client)
ORDER BY (xcode, store_no, pdate)
SETTINGS index_granularity = 8192;

-- Промежуточная таблица. Структура обязана совпадать с основной:
-- REPLACE PARTITION работает только между идентичными таблицами.
CREATE TABLE IF NOT EXISTS sales_staging AS sales;
```

- [ ] **Step 4: Написать `schema.py`**

```python
"""Создание схемы витрины из SQL-файлов."""
from __future__ import annotations

from pathlib import Path

from clickhouse_connect.driver import Client

SCHEMA_DIR = Path(__file__).resolve().parents[3] / "clickhouse" / "schema"

# Единый порядок колонок для чтения из PostgreSQL и вставки в ClickHouse.
# Любое расхождение здесь приведёт к тихой порче данных, поэтому источник
# истины один — эта константа.
SALES_COLUMNS: tuple[str, ...] = (
    "src_id", "pdate", "client", "store_no", "xcode",
    "salesitem", "salesvalue", "opt",
)


def _run_sql_file(client: Client, path: Path) -> None:
    for statement in path.read_text(encoding="utf-8").split(";"):
        if statement.strip():
            client.command(statement)


def create_sales_tables(client: Client) -> None:
    """Идемпотентно: повторный вызов не трогает существующие данные."""
    _run_sql_file(client, SCHEMA_DIR / "01_sales.sql")
```

- [ ] **Step 5: Прогнать тесты**

Run: `cd sync && pytest tests/test_schema.py -v`
Expected: семь тестов PASS.

- [ ] **Step 6: Коммит**

```bash
git add clickhouse/schema/01_sales.sql sync/src/gfd_sync/schema.py sync/tests/test_schema.py
git commit -m "Схема витрины: партиции «месяц × сеть», сортировка по коду товара"
```

---

### Task 6: Словари справочников

**Files:**
- Create: `clickhouse/schema/02_dictionaries.sql`
- Modify: `sync/src/gfd_sync/schema.py` — добавить `create_dictionaries(client, pg_dsn_parts) -> None`
- Test: `sync/tests/test_dictionaries.py`

**Interfaces:**
- Consumes: `create_sales_tables` из Task 5, источник из Task 3
- Produces: `create_dictionaries(client) -> None`; словари `dict_product`, `dict_address`, `dict_client_map`

- [ ] **Step 1: Написать падающий тест**

`sync/tests/test_dictionaries.py`:

```python
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
        conn.execute("UPDATE product SET vkus = 'ПРОВЕРКА СЛОВАРЯ' WHERE xcode = 'X000001'")
    ch.command("SYSTEM RELOAD DICTIONARY dict_product")
    assert ch.command(
        "SELECT dictGetString('dict_product', 'vkus', tuple('X000001'))"
    ) == "ПРОВЕРКА СЛОВАРЯ"


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
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `cd sync && pytest tests/test_dictionaries.py -v`
Expected: FAIL — `ImportError: cannot import name 'create_dictionaries'`

- [ ] **Step 3: Написать `clickhouse/schema/02_dictionaries.sql`**

Плейсхолдеры `{host}`, `{port}`, `{db}`, `{user}`, `{password}` подставляются кодом.

```sql
-- Справочники не копируются в витрину, а читаются напрямую из PostgreSQL
-- с перечиткой раз в минуту. Правка бренда или вкуса появляется в отчётах
-- без перезаливки сотен миллионов строк продаж.

CREATE DICTIONARY IF NOT EXISTS dict_product
(
    xcode     String,
    xname     String,
    brand     String,
    category  String,
    proizvod  String,
    litrag    Float64,
    pack      String,
    vkus      String,
    catlitrag String
)
PRIMARY KEY xcode
SOURCE(POSTGRESQL(
    host '{host}' port {port} user '{user}' password '{password}' db '{db}'
    query 'SELECT xcode,
                  coalesce(xname, '''') AS xname,
                  upper(coalesce(brand, '''')) AS brand,
                  upper(coalesce(category, '''')) AS category,
                  upper(coalesce(proizvod, '''')) AS proizvod,
                  coalesce(litrag, 0)::float8 AS litrag,
                  upper(coalesce(pack, '''')) AS pack,
                  upper(coalesce(vkus, '''')) AS vkus,
                  coalesce(catlitrag, '''') AS catlitrag
           FROM public.product'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 50 MAX 70);

CREATE DICTIONARY IF NOT EXISTS dict_address
(
    store_no     String,
    client       String,
    store_format String,
    oblast       String,
    city         String,
    region       String
)
PRIMARY KEY store_no, client
SOURCE(POSTGRESQL(
    host '{host}' port {port} user '{user}' password '{password}' db '{db}'
    query 'SELECT DISTINCT ON (store_no, client)
                  store_no,
                  upper(client) AS client,
                  upper(coalesce(store_format, '''')) AS store_format,
                  upper(coalesce(oblast, '''')) AS oblast,
                  upper(coalesce(city, '''')) AS city,
                  upper(coalesce(region, '''')) AS region
           FROM public.address
           ORDER BY store_no, client, last_seen DESC NULLS LAST'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 50 MAX 70);

-- Переименование сетей. В Qlik это было зашито в скрипт загрузки; здесь —
-- таблица в служебной базе, которую можно править без перезаливки.
CREATE DICTIONARY IF NOT EXISTS dict_client_map
(
    old_client String,
    new_client String
)
PRIMARY KEY old_client
SOURCE(POSTGRESQL(
    host '{app_host}' port {app_port} user '{app_user}'
    password '{app_password}' db '{app_db}'
    query 'SELECT upper(old_client) AS old_client, upper(new_client) AS new_client
           FROM public.client_map'
))
LAYOUT(COMPLEX_KEY_HASHED())
LIFETIME(MIN 50 MAX 70);
```

- [ ] **Step 4: Добавить в `pg_app/init/01_journal.sql` таблицу правил переименования**

```sql
CREATE TABLE IF NOT EXISTS client_map (
    old_client text PRIMARY KEY,
    new_client text NOT NULL
);

INSERT INTO client_map (old_client, new_client) VALUES
    ('БАТОН',            'X5 БАТОН'),
    ('ВИНГАРАЖ',         'ЛЕНТА ВИНГАРАЖ'),
    ('НАЛЕТУ',           'X5 НАЛЕТУ'),
    ('НАША РАДУГА',      'АШАН НАША РАДУГА'),
    ('ПЕРВЫЙ ВЫБОР В1',  'МАГНИТ ПЕРВЫЙ ВЫБОР'),
    ('СЛАТА',            'X5 СЛАТА'),
    ('ТОЧКА ТАБАКА',     'ЛЕНТА ТОЧКА ТАБАКА'),
    ('ХЛЕБСОЛЬ',         'X5 ХЛЕБСОЛЬ'),
    ('ЧИЖИК',            'X5 ЧИЖИК')
ON CONFLICT (old_client) DO NOTHING;
```

- [ ] **Step 5: Дописать `create_dictionaries` в `schema.py`**

За словарями ходит **сам сервер ClickHouse**, из своего контейнера. Хост
и порт поэтому берутся не из DSN (тот описывает путь от синхронизатора):
`localhost` из DSN указал бы ClickHouse на самого себя. В `Settings`
добавляются поля `dict_source_host`, `dict_source_port`, `dict_app_host`,
`dict_app_port` — локально это имена сервисов docker-сети (`pg_source`,
`pg_app`, порт 5432), на сервере — адрес машины с боевой базой. Те же
переменные добавляются в `.env.example`.

```python
import re
from urllib.parse import unquote, urlparse

def _statements(sql: str) -> list[str]:
    """Режет файл на операторы: ClickHouse принимает их только по одному.

    Границей считается точка с запятой в конце строки, а не то, что идёт
    следом: между операторами стоят комментарии, и они просто прилипают
    к началу следующего — это допустимо.
    """
    return [s for s in re.split(r";\s*\n", sql) if s.strip()]


def _dsn_parts(dsn: str) -> dict[str, str]:
    u = urlparse(dsn)
    return {"host": u.hostname or "localhost", "port": str(u.port or 5432),
            "user": unquote(u.username or ""),
            "password": unquote(u.password or ""),
            "db": (u.path or "/").lstrip("/")}


def create_dictionaries(client: Client) -> None:
    """Словари читают PostgreSQL напрямую. Пароли подставляются здесь и
    в репозиторий не попадают."""
    s = get_settings()
    src, app = _dsn_parts(s.pg_source_dsn), _dsn_parts(s.pg_app_dsn)
    sql = (SCHEMA_DIR / "02_dictionaries.sql").read_text(encoding="utf-8")
    sql = sql.format(
        # Хост и порт берутся из настроек словарей, а не из DSN: DSN описывает
        # путь от синхронизатора, а ходить по нему будет сервер ClickHouse.
        host=s.dict_source_host, port=s.dict_source_port,
        user=src["user"], password=src["password"], db=src["db"],
        app_host=s.dict_app_host, app_port=s.dict_app_port,
        app_user=app["user"], app_password=app["password"], app_db=app["db"],
    )
    for statement in _statements(sql):
        client.command(statement)
```

Добавить в начало файла: `from .config import get_settings`. Тем же
`_statements` начинает пользоваться и `_run_sql_file`.

**Служебную базу придётся пересоздать:** init-скрипты PostgreSQL выполняются
только на пустом каталоге данных, а том `pg_app` к этому моменту уже создан.
`docker compose rm -sf pg_app && docker volume rm gfd-bi_pg_app_data &&
docker compose --profile dev up -d pg_app`.

- [ ] **Step 6: Прогнать тесты**

Run: `cd sync && pytest tests/test_dictionaries.py -v`
Expected: семь тестов PASS.

- [ ] **Step 7: Коммит**

```bash
git add clickhouse/schema/02_dictionaries.sql pg_app/init/01_journal.sql \
        sync/src/gfd_sync/schema.py sync/tests/test_dictionaries.py
git commit -m "Словари product, address и переименования сетей поверх PostgreSQL"
```

---

### Task 7: Чтение куска из PostgreSQL

**Files:**
- Create: `sync/src/gfd_sync/reader.py`
- Test: `sync/tests/test_reader.py`

**Interfaces:**
- Consumes: `Chunk` (Task 4), `SALES_COLUMNS` (Task 5), `pg_source()` (Task 3)
- Produces:
  - `build_copy_sql(chunk: Chunk) -> str` — текст `COPY (...) TO STDOUT WITH CSV`
  - `read_chunk(chunk: Chunk) -> Iterator[bytes]` — блоки CSV из источника
  - `source_stats(chunk: Chunk) -> Stats`, где `Stats` — `NamedTuple` с полями `rows: int`, `sum_value: Decimal`, `sum_items: Decimal`

- [ ] **Step 1: Написать падающий тест**

`sync/tests/test_reader.py`:

```python
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
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `cd sync && pytest tests/test_reader.py -v`
Expected: FAIL — `ModuleNotFoundError: gfd_sync.reader`

- [ ] **Step 3: Написать `reader.py`**

```python
"""Чтение куска из PostgreSQL.

Данные идут потоком через COPY ... TO STDOUT: Python не разбирает строки,
а перекладывает байты. На миллионах строк это на порядок быстрее курсора.

Соединение с источником открыто только на чтение (см. clients.pg_source).
"""
from __future__ import annotations

from decimal import Decimal
from typing import Iterator, NamedTuple

from .chunks import Chunk
from .clients import pg_source
from .config import EXCLUDED_CHAINS

# Порядок выражений строго соответствует schema.SALES_COLUMNS.
# Приведения делаются на стороне PostgreSQL, чтобы Python не трогал данные.
_SELECT = """
    SELECT id,
           pdate,
           upper(trim(client)),
           store_no,
           xcode,
           salesitem,
           salesvalue,
           CASE WHEN opt IS NOT NULL AND opt <> '' THEN 1 ELSE 0 END
    FROM public.sales
    WHERE pdate >= '{date_from}' AND pdate < '{date_to}'
      AND upper(trim(client)) = '{chain}'
      AND upper(trim(client)) NOT IN ({excluded})
"""


class Stats(NamedTuple):
    rows: int
    sum_value: Decimal
    sum_items: Decimal


def _excluded_literal() -> str:
    return ", ".join(f"'{c}'" for c in EXCLUDED_CHAINS)


def _select_sql(chunk: Chunk) -> str:
    # Имена сетей приходят из справочника, а не от пользователя, но апостроф
    # в названии всё равно возможен — экранируем.
    return _SELECT.format(
        date_from=chunk.date_from, date_to=chunk.date_to,
        chain=chunk.chain.replace("'", "''"), excluded=_excluded_literal(),
    )


def build_copy_sql(chunk: Chunk) -> str:
    return f"COPY ({_select_sql(chunk)}) TO STDOUT WITH (FORMAT csv)"


def read_chunk(chunk: Chunk) -> Iterator[bytes]:
    """Отдаёт куски CSV. Пустой поток — законный результат: сети могло
    не быть в этом месяце, или она в списке исключённых."""
    with pg_source() as conn, conn.cursor().copy(build_copy_sql(chunk)) as cp:
        for data in cp:
            yield bytes(data)


def source_stats(chunk: Chunk) -> Stats:
    """Контрольные суммы источника — с ними сверяется залитое."""
    sql = f"""
        SELECT count(*), coalesce(sum(salesvalue), 0), coalesce(sum(salesitem), 0)
        FROM ({_select_sql(chunk)}) t (id, pdate, client, store_no, xcode,
                                       salesitem, salesvalue, opt)
    """
    with pg_source() as conn:
        rows, value, items = conn.execute(sql).fetchone()
    return Stats(rows, Decimal(value), Decimal(items))
```

- [ ] **Step 4: Прогнать тесты**

Run: `cd sync && pytest tests/test_reader.py -v`
Expected: восемь тестов PASS.

- [ ] **Step 5: Коммит**

```bash
git add sync/src/gfd_sync/reader.py sync/tests/test_reader.py
git commit -m "Чтение куска из PostgreSQL потоком COPY, фильтры заливки"
```

---

### Task 8: Заливка куска с атомарной подменой

**Files:**
- Create: `sync/src/gfd_sync/loader.py`
- Test: `sync/tests/test_loader.py`

**Interfaces:**
- Consumes: `read_chunk`, `source_stats` (Task 7), `SALES_COLUMNS` (Task 5)
- Produces:
  - `LoadResult` — `NamedTuple` с полями `chunk: Chunk`, `rows: int`, `seconds: float`, `replaced: bool`, `error: str | None`
  - `load_chunk(chunk: Chunk) -> LoadResult`
  - `target_stats(chunk: Chunk) -> Stats` — те же три числа, но из витрины

- [ ] **Step 1: Написать падающий тест**

`sync/tests/test_loader.py`:

```python
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
    load_chunk(Chunk(2026, 7, "АШАН"))
    assert ch_client().command("SELECT count() FROM sales_staging") == 0


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
    create_shadow(ch)
    ch.command("TRUNCATE TABLE sales_shadow")
    load_chunk(Chunk(2026, 7, "ОКЕЙ"), target="sales_shadow")
    в_теневой = ch.command("SELECT count() FROM sales_shadow")

    swap_shadow(ch)

    assert ch.command("SELECT count() FROM sales") == в_теневой
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `cd sync && pytest tests/test_loader.py -v`
Expected: FAIL — `ModuleNotFoundError: gfd_sync.loader`

- [ ] **Step 3: Написать `loader.py`**

```python
"""Заливка куска через промежуточную таблицу.

Порядок операций выбран так, чтобы витрина ни в какой момент не показывала
недолитые данные:

    1. чистим промежуточную партицию (могли остаться следы прошлого сбоя);
    2. льём туда данные из источника;
    3. сверяем контрольные суммы с источником;
    4. только при совпадении — REPLACE PARTITION, операция атомарна;
    5. чистим за собой.

Если процесс убить на любом шаге до четвёртого, боевая таблица не пострадает.
"""
from __future__ import annotations

import time
from typing import NamedTuple

from .chunks import Chunk
from .clients import ch_client
from .reader import Stats, read_chunk, source_stats
from .schema import SALES_COLUMNS

_INSERT_SETTINGS = {
    # Заливка не должна выдавливать память у PostgreSQL.
    "max_insert_threads": 4,
    "max_memory_usage": 4 * 1024**3,
}


class LoadResult(NamedTuple):
    chunk: Chunk
    rows: int
    seconds: float
    replaced: bool
    error: str | None


def _partition_literal(chunk: Chunk) -> str:
    ym, chain = chunk.partition_id
    return f"({ym}, '{chain.replace(chr(39), chr(39) * 2)}')"


def target_stats(chunk: Chunk, table: str = "sales") -> Stats:
    from decimal import Decimal
    row = ch_client().query(
        f"SELECT count(), coalesce(sum(salesvalue), 0), coalesce(sum(salesitem), 0) "
        f"FROM {table} WHERE toYYYYMM(pdate) = %(ym)s AND client = %(chain)s",
        parameters={"ym": chunk.partition_id[0], "chain": chunk.chain},
    ).result_rows[0]
    return Stats(int(row[0]), Decimal(str(row[1])), Decimal(str(row[2])))


def create_shadow(client) -> None:
    """Теневая копия витрины для полной перезаливки."""
    client.command("CREATE TABLE IF NOT EXISTS sales_shadow AS sales")


def swap_shadow(client) -> None:
    """Атомарно меняет местами боевую таблицу и теневую.

    До этого момента пользователи видят старые данные, после — новые.
    Промежуточного состояния нет: EXCHANGE TABLES выполняется под блокировкой.
    """
    client.command("EXCHANGE TABLES sales AND sales_shadow")


def load_chunk(chunk: Chunk, target: str = "sales") -> LoadResult:
    """Заливает кусок в указанную таблицу.

    target="sales" — обычная работа, подменяется партиция боевой таблицы.
    target="sales_shadow" — полная перезаливка, боевая таблица не трогается.
    """
    started = time.monotonic()
    ch = ch_client()
    part = _partition_literal(chunk)

    try:
        ch.command(f"ALTER TABLE sales_staging DROP PARTITION {part}")

        rows = 0
        for block in read_chunk(chunk):
            if not block:
                continue
            ch.raw_insert("sales_staging", column_names=list(SALES_COLUMNS),
                          insert_block=block, fmt="CSV", settings=_INSERT_SETTINGS)
            rows += block.count(b"\n")

        src = source_stats(chunk)
        got = target_stats(chunk, table="sales_staging")
        if got != src:
            ch.command(f"ALTER TABLE sales_staging DROP PARTITION {part}")
            return LoadResult(
                chunk, got.rows, time.monotonic() - started, False,
                f"расхождение с источником: в источнике {src}, залито {got}",
            )

        # Атомарная подмена. Пустая партиция в staging корректно очищает
        # партицию в цели — сеть, переставшая присылать данные, обнуляется.
        ch.command(f"ALTER TABLE {target} REPLACE PARTITION {part} FROM sales_staging")
        ch.command(f"ALTER TABLE sales_staging DROP PARTITION {part}")

        return LoadResult(chunk, src.rows, time.monotonic() - started, True, None)

    except Exception as exc:                      # noqa: BLE001 — причина уходит в журнал
        try:
            ch.command(f"ALTER TABLE sales_staging DROP PARTITION {part}")
        except Exception:                          # noqa: BLE001
            pass
        return LoadResult(chunk, 0, time.monotonic() - started, False, str(exc))
```

- [ ] **Step 4: Прогнать тесты**

Run: `cd sync && pytest tests/test_loader.py -v`
Expected: десять тестов PASS (около минуты — льются настоящие данные).

- [ ] **Step 5: Коммит**

```bash
git add sync/src/gfd_sync/loader.py sync/tests/test_loader.py
git commit -m "Заливка куска через промежуточную таблицу с атомарной подменой"
```

---

### Task 9: Сверка витрины с источником

**Files:**
- Create: `sync/src/gfd_sync/verifier.py`
- Test: `sync/tests/test_verifier.py`

**Interfaces:**
- Consumes: `source_stats` (Task 7), `target_stats` (Task 8)
- Produces:
  - `Mismatch` — `NamedTuple` с полями `chunk: Chunk`, `source: Stats`, `target: Stats`
  - `verify_chunks(chunks: list[Chunk]) -> list[Mismatch]`
  - `verify_recent(months: int = 3) -> list[Mismatch]`
  - `repair(mismatches: list[Mismatch]) -> list[LoadResult]`

- [ ] **Step 1: Написать падающий тест**

`sync/tests/test_verifier.py`:

```python
"""Сверка ловит любые изменения в источнике, как бы они ни произошли."""
import os
from decimal import Decimal

import psycopg
import pytest
from gfd_sync.chunks import Chunk
from gfd_sync.clients import ch_client
from gfd_sync.loader import load_chunk, target_stats
from gfd_sync.reader import source_stats
from gfd_sync.schema import create_sales_tables, create_dictionaries
from gfd_sync.verifier import verify_chunks, repair


@pytest.fixture(autouse=True)
def подготовка():
    ch = ch_client()
    create_sales_tables(ch)
    create_dictionaries(ch)


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

    with psycopg.connect(os.environ["PG_SOURCE_DSN"], autocommit=True) as conn:
        conn.execute("""
            UPDATE sales SET salesvalue = salesvalue + 1000
            WHERE id = (SELECT id FROM sales
                        WHERE pdate >= '2026-08-01' AND pdate < '2026-09-01'
                          AND upper(client) = 'ЛЕНТА' LIMIT 1)
        """)
    assert len(verify_chunks([chunk])) == 1


def test_починка_устраняет_расхождение():
    chunk = Chunk(2026, 8, "ЛЕНТА")
    mismatches = verify_chunks([chunk])
    assert mismatches, "к этому моменту расхождение должно быть"
    results = repair(mismatches)
    assert all(r.replaced for r in results)
    assert verify_chunks([chunk]) == []
    assert target_stats(chunk) == source_stats(chunk)
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `cd sync && pytest tests/test_verifier.py -v`
Expected: FAIL — `ModuleNotFoundError: gfd_sync.verifier`

- [ ] **Step 3: Написать `verifier.py`**

```python
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


def known_chains() -> list[str]:
    """Сети, встречающиеся в источнике, кроме исключённых."""
    excluded = ", ".join(f"'{c}'" for c in EXCLUDED_CHAINS)
    with pg_source() as conn:
        rows = conn.execute(
            f"SELECT DISTINCT upper(trim(client)) FROM public.sales "
            f"WHERE upper(trim(client)) NOT IN ({excluded})"
        ).fetchall()
    return sorted(r[0] for r in rows)


def verify_chunks(chunks: list[Chunk]) -> list[Mismatch]:
    out: list[Mismatch] = []
    for chunk in chunks:
        src, dst = source_stats(chunk), target_stats(chunk)
        if src != dst:
            out.append(Mismatch(chunk, src, dst))
    return out


def verify_recent(months: int = 3) -> list[Mismatch]:
    """Сверка свежего хвоста — дёшево, гоняется каждые десять минут."""
    s = get_settings()
    today = date.today()
    year, month = today.year, today.month - months + 1
    while month < 1:
        year, month = year - 1, month + 12
    date_from = max(date(year, month, 1), s.load_date_from)
    return verify_chunks(chunks_in_period(date_from, s.load_date_to, known_chains()))


def verify_all() -> list[Mismatch]:
    """Полная сверка. Сканирует источник целиком, поэтому только ночью."""
    s = get_settings()
    return verify_chunks(
        chunks_in_period(s.load_date_from, s.load_date_to, known_chains()))


def repair(mismatches: list[Mismatch]) -> list[LoadResult]:
    return [load_chunk(m.chunk) for m in mismatches]
```

- [ ] **Step 4: Прогнать тесты**

Run: `cd sync && pytest tests/test_verifier.py -v`
Expected: четыре теста PASS.

- [ ] **Step 5: Коммит**

```bash
git add sync/src/gfd_sync/verifier.py sync/tests/test_verifier.py
git commit -m "Сверка витрины с источником и автоматическая починка"
```

---

### Task 10: Журнал синхронизации

**Files:**
- Create: `sync/src/gfd_sync/journal.py`
- Modify: `pg_app/init/01_journal.sql` — добавить таблицу `sync_journal`
- Test: `sync/tests/test_journal.py`

**Interfaces:**
- Consumes: `pg_app()` (Task 3), `LoadResult` (Task 8)
- Produces:
  - `record(result: LoadResult, operation: str) -> int` — возвращает id записи
  - `last_runs(limit: int = 50) -> list[dict]`
  - `last_success(chunk: Chunk) -> datetime | None`
  - `has_failures(since_minutes: int = 60) -> bool`

- [ ] **Step 1: Написать падающий тест**

`sync/tests/test_journal.py`:

```python
"""Журнал: тихих поломок быть не должно — всё видно в истории."""
from gfd_sync.chunks import Chunk
from gfd_sync.journal import record, last_runs, last_success, has_failures
from gfd_sync.loader import LoadResult


def test_успешный_запуск_попадает_в_журнал():
    chunk = Chunk(2026, 7, "МАГНИТ")
    rid = record(LoadResult(chunk, 12345, 3.2, True, None), operation="load")
    assert rid > 0
    top = last_runs(1)[0]
    assert top["chunk_key"] == chunk.key
    assert top["rows"] == 12345
    assert top["status"] == "ok"


def test_ошибка_пишется_с_текстом():
    chunk = Chunk(2026, 7, "ЛЕНТА")
    record(LoadResult(chunk, 0, 1.0, False, "расхождение с источником"), operation="load")
    top = last_runs(1)[0]
    assert top["status"] == "error"
    assert "расхождение" in top["error"]


def test_время_последнего_успеха():
    chunk = Chunk(2026, 6, "ДИКСИ")
    assert last_success(chunk) is None
    record(LoadResult(chunk, 10, 1.0, True, None), operation="load")
    assert last_success(chunk) is not None


def test_флаг_свежих_ошибок():
    record(LoadResult(Chunk(2026, 5, "АШАН"), 0, 1.0, False, "источник недоступен"),
           operation="load")
    assert has_failures(since_minutes=60) is True
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `cd sync && pytest tests/test_journal.py -v`
Expected: FAIL — `ModuleNotFoundError: gfd_sync.journal`

- [ ] **Step 3: Дописать таблицу в `pg_app/init/01_journal.sql`**

```sql
CREATE TABLE IF NOT EXISTS sync_journal (
    id          bigserial PRIMARY KEY,
    started_at  timestamptz NOT NULL DEFAULT now(),
    operation   text        NOT NULL,   -- load | verify | full_reload
    chunk_key   text,                   -- 202607/МАГНИТ, пусто для общих операций
    rows        bigint      NOT NULL DEFAULT 0,
    seconds     numeric(10,3) NOT NULL DEFAULT 0,
    status      text        NOT NULL,   -- ok | error
    error       text
);

CREATE INDEX IF NOT EXISTS idx_journal_started ON sync_journal (started_at DESC);
CREATE INDEX IF NOT EXISTS idx_journal_chunk   ON sync_journal (chunk_key, started_at DESC);
```

- [ ] **Step 4: Написать `journal.py`**

```python
"""Журнал синхронизации в служебной базе.

Каждая операция оставляет след: что делали, сколько строк, сколько времени,
чем кончилось. Админка первого этапа — это выборки отсюда.
"""
from __future__ import annotations

from datetime import datetime

from .chunks import Chunk
from .clients import pg_app
from .loader import LoadResult


def record(result: LoadResult, operation: str) -> int:
    with pg_app() as conn:
        return conn.execute("""
            INSERT INTO sync_journal (operation, chunk_key, rows, seconds, status, error)
            VALUES (%s, %s, %s, %s, %s, %s) RETURNING id
        """, (operation, result.chunk.key if result.chunk else None,
              result.rows, round(result.seconds, 3),
              "ok" if result.error is None else "error", result.error)).fetchone()[0]


def last_runs(limit: int = 50) -> list[dict]:
    with pg_app() as conn:
        rows = conn.execute("""
            SELECT id, started_at, operation, chunk_key, rows, seconds, status, error
            FROM sync_journal ORDER BY started_at DESC, id DESC LIMIT %s
        """, (limit,)).fetchall()
    keys = ("id", "started_at", "operation", "chunk_key", "rows",
            "seconds", "status", "error")
    return [dict(zip(keys, r)) for r in rows]


def last_success(chunk: Chunk) -> datetime | None:
    with pg_app() as conn:
        row = conn.execute("""
            SELECT max(started_at) FROM sync_journal
            WHERE chunk_key = %s AND status = 'ok'
        """, (chunk.key,)).fetchone()
    return row[0] if row else None


def has_failures(since_minutes: int = 60) -> bool:
    with pg_app() as conn:
        row = conn.execute("""
            SELECT count(*) FROM sync_journal
            WHERE status = 'error'
              AND started_at > now() - make_interval(mins => %s)
        """, (since_minutes,)).fetchone()
    return row[0] > 0
```

- [ ] **Step 5: Прогнать тесты**

Run: `cd sync && pytest tests/test_journal.py -v`
Expected: четыре теста PASS.

- [ ] **Step 6: Коммит**

```bash
git add pg_app/init/01_journal.sql sync/src/gfd_sync/journal.py sync/tests/test_journal.py
git commit -m "Журнал синхронизации в служебной базе"
```

---

### Task 11: Обнаружение изменений по load_log

**Files:**
- Create: `sync/src/gfd_sync/detector.py`
- Test: `sync/tests/test_detector.py`

**Interfaces:**
- Consumes: `pg_source()` (Task 3), `Chunk` (Task 4), `journal` (Task 10)
- Produces:
  - `new_files(since: datetime | None) -> list[dict]` — записи `load_log` новее указанного времени
  - `chunks_from_files(files: list[dict]) -> list[Chunk]` — затронутые куски
  - `pending_chunks() -> list[Chunk]` — что нужно перезалить прямо сейчас

- [ ] **Step 1: Написать падающий тест**

`sync/tests/test_detector.py`:

```python
"""Обнаружение изменений через load_log: новые файлы сами говорят,
какие куски перезалить."""
import os
from datetime import datetime, timedelta, timezone

import psycopg
from gfd_sync.chunks import Chunk
from gfd_sync.detector import new_files, chunks_from_files, pending_chunks


def test_новые_файлы_находятся():
    with psycopg.connect(os.environ["PG_SOURCE_DSN"], autocommit=True) as conn:
        conn.execute("""
            INSERT INTO load_log (file_path, file_hash, rows_loaded)
            VALUES ('/mnt/gfd_data/in/МАГНИТ_2026_07.xlsx', 'abc123', 5000)
            ON CONFLICT (file_path) DO UPDATE SET loaded_at = now()
        """)
    got = new_files(since=datetime.now(timezone.utc) - timedelta(minutes=5))
    assert any("МАГНИТ_2026_07" in f["file_path"] for f in got)


def test_куски_определяются_по_данным_а_не_по_имени_файла():
    """Имя файла — подсказка, но источником истины остаётся сама таблица sales:
    определяем, какие месяцы и сети реально пришли из этого файла."""
    files = [{"file_path": "/mnt/gfd_data/in/МАГНИТ_2026_07.xlsx"}]
    got = chunks_from_files(files)
    assert Chunk(2026, 7, "МАГНИТ") in got


def test_неизвестный_файл_не_роняет_обработку():
    got = chunks_from_files([{"file_path": "/mnt/gfd_data/in/мусор.txt"}])
    assert got == []


def test_ожидающие_куски_не_повторяются():
    got = pending_chunks()
    assert len(got) == len(set(got))
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `cd sync && pytest tests/test_detector.py -v`
Expected: FAIL — `ModuleNotFoundError: gfd_sync.detector`

- [ ] **Step 3: Написать `detector.py`**

```python
"""Обнаружение изменений по журналу загрузок источника.

Файл в load_log — сигнал «здесь что-то поменялось». Какие именно куски
затронуты, спрашиваем у самой таблицы sales по колонке name_file: имя файла
может быть каким угодно, а данные не врут.
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from pathlib import PurePosixPath

from .chunks import Chunk
from .clients import pg_source
from .config import EXCLUDED_CHAINS


def new_files(since: datetime | None = None) -> list[dict]:
    if since is None:
        since = datetime.now(timezone.utc) - timedelta(hours=24)
    with pg_source() as conn:
        rows = conn.execute("""
            SELECT file_path, file_hash, rows_loaded, loaded_at
            FROM public.load_log
            WHERE loaded_at > %s AND status = 'completed'
            ORDER BY loaded_at
        """, (since,)).fetchall()
    keys = ("file_path", "file_hash", "rows_loaded", "loaded_at")
    return [dict(zip(keys, r)) for r in rows]


def chunks_from_files(files: list[dict]) -> list[Chunk]:
    """Спрашиваем у sales, какие пары «месяц × сеть» пришли из этих файлов."""
    names = [PurePosixPath(f["file_path"]).name for f in files]
    if not names:
        return []
    excluded = ", ".join(f"'{c}'" for c in EXCLUDED_CHAINS)
    with pg_source() as conn:
        rows = conn.execute(f"""
            SELECT DISTINCT
                   extract(year FROM pdate)::int,
                   extract(month FROM pdate)::int,
                   upper(trim(client))
            FROM public.sales
            WHERE name_file = ANY(%s)
              AND pdate IS NOT NULL
              AND upper(trim(client)) NOT IN ({excluded})
        """, (names,)).fetchall()
    return [Chunk(y, m, c) for y, m, c in rows]


def pending_chunks() -> list[Chunk]:
    """Куски, затронутые файлами за последние сутки, без повторов."""
    return sorted(set(chunks_from_files(new_files())))
```

- [ ] **Step 4: Прогнать тесты**

Run: `cd sync && pytest tests/test_detector.py -v`
Expected: четыре теста PASS.

- [ ] **Step 5: Коммит**

```bash
git add sync/src/gfd_sync/detector.py sync/tests/test_detector.py
git commit -m "Обнаружение изменений по load_log"
```

---

### Task 12: Командный интерфейс

**Files:**
- Create: `sync/src/gfd_sync/cli.py`
- Test: `sync/tests/test_cli.py`

**Interfaces:**
- Consumes: всё предыдущее
- Produces: команды `gfd-sync init`, `load`, `sync`, `verify`, `full-reload`, `status`

- [ ] **Step 1: Написать падающий тест**

`sync/tests/test_cli.py`:

```python
"""Командный интерфейс — то, чем пользуются расписание и админка."""
from click.testing import CliRunner
from gfd_sync.cli import cli


def test_init_создаёт_схему():
    r = CliRunner().invoke(cli, ["init"])
    assert r.exit_code == 0
    assert "готово" in r.output.lower()


def test_load_принимает_ключ_куска():
    r = CliRunner().invoke(cli, ["load", "--chunk", "202607/МАГНИТ"])
    assert r.exit_code == 0
    assert "202607/МАГНИТ" in r.output


def test_verify_без_расхождений_возвращает_ноль():
    CliRunner().invoke(cli, ["load", "--chunk", "202607/МАГНИТ"])
    r = CliRunner().invoke(cli, ["verify", "--chunk", "202607/МАГНИТ"])
    assert r.exit_code == 0
    assert "расхождений нет" in r.output.lower()


def test_verify_с_флагом_repair_чинит():
    r = CliRunner().invoke(cli, ["verify", "--months", "1", "--repair"])
    assert r.exit_code == 0


def test_full_reload_требует_подтверждения():
    """Полная перезаливка читает сотни гигабайт с боевого диска —
    случайный запуск недопустим."""
    r = CliRunner().invoke(cli, ["full-reload"], input="нет\n")
    assert "отменено" in r.output.lower()


def test_status_показывает_историю():
    r = CliRunner().invoke(cli, ["status"])
    assert r.exit_code == 0
```

- [ ] **Step 2: Запустить тест и убедиться, что он падает**

Run: `cd sync && pytest tests/test_cli.py -v`
Expected: FAIL — `ModuleNotFoundError: gfd_sync.cli`

- [ ] **Step 3: Написать `cli.py`**

```python
"""Команды синхронизатора.

    gfd-sync init                       создать схему и словари
    gfd-sync load --chunk 202607/МАГНИТ залить один кусок
    gfd-sync sync                       залить всё, что затронуто новыми файлами
    gfd-sync verify [--repair]          сверить и при желании починить
    gfd-sync full-reload                перезалить всё с нуля
    gfd-sync status                     последние операции
"""
from __future__ import annotations

import click

from . import journal
from .chunks import chunk_from_key, chunks_in_period
from .clients import ch_client
from .config import get_settings
from .detector import pending_chunks
from .loader import create_shadow, load_chunk, swap_shadow
from .schema import create_dictionaries, create_sales_tables
from .verifier import known_chains, repair, verify_all, verify_chunks, verify_recent


@click.group()
def cli() -> None:
    """Синхронизация витрины ClickHouse с боевым PostgreSQL."""


@cli.command()
def init() -> None:
    """Создать таблицы и словари."""
    ch = ch_client()
    create_sales_tables(ch)
    create_dictionaries(ch)
    click.echo("Готово: таблицы и словари созданы")


@cli.command()
@click.option("--chunk", "chunk_key", required=True, help="ключ вида 202607/МАГНИТ")
def load(chunk_key: str) -> None:
    """Залить один кусок."""
    result = load_chunk(chunk_from_key(chunk_key))
    journal.record(result, operation="load")
    if result.error:
        click.echo(f"{chunk_key}: ОШИБКА — {result.error}")
        raise SystemExit(1)
    click.echo(f"{chunk_key}: {result.rows} строк за {result.seconds:.1f} с")


@cli.command()
def sync() -> None:
    """Залить всё, что затронуто новыми файлами, и сверить свежий хвост."""
    chunks = pending_chunks()
    for chunk in chunks:
        result = load_chunk(chunk)
        journal.record(result, operation="load")
        click.echo(f"{chunk.key}: {'ок' if not result.error else result.error}")

    mismatches = verify_recent(months=3)
    if mismatches:
        click.echo(f"расхождений: {len(mismatches)}, чиню")
        for result in repair(mismatches):
            journal.record(result, operation="load")
    click.echo(f"обработано кусков: {len(chunks)}, исправлено: {len(mismatches)}")


@cli.command()
@click.option("--chunk", "chunk_key", default=None, help="проверить один кусок")
@click.option("--months", default=3, help="сколько последних месяцев проверять")
@click.option("--all", "check_all", is_flag=True, help="проверить всю историю")
@click.option("--repair", "do_repair", is_flag=True, help="чинить найденное")
def verify(chunk_key: str | None, months: int, check_all: bool, do_repair: bool) -> None:
    """Сверить витрину с источником."""
    if chunk_key:
        mismatches = verify_chunks([chunk_from_key(chunk_key)])
    elif check_all:
        mismatches = verify_all()
    else:
        mismatches = verify_recent(months=months)

    if not mismatches:
        click.echo("Расхождений нет")
        return

    for m in mismatches:
        click.echo(f"{m.chunk.key}: источник {m.source.rows} строк, "
                   f"витрина {m.target.rows}")
    if do_repair:
        for result in repair(mismatches):
            journal.record(result, operation="load")
            click.echo(f"{result.chunk.key}: {'починено' if result.replaced else result.error}")


@cli.command("full-reload")
@click.option("--yes", is_flag=True, help="не спрашивать подтверждения")
def full_reload(yes: bool) -> None:
    """Перезалить всё с нуля. Читает источник целиком — только вне рабочих часов."""
    if not yes:
        answer = click.prompt(
            "Полная перезаливка нагрузит боевой диск на десятки минут. "
            "Продолжить? (да/нет)", default="нет")
        if answer.strip().lower() not in {"да", "yes", "y"}:
            click.echo("Отменено")
            return

    # Льём в теневую таблицу: пока идёт перезаливка, люди работают
    # на старых данных и переключаются на новые одномоментно.
    ch = ch_client()
    create_shadow(ch)
    ch.command("TRUNCATE TABLE sales_shadow")

    s = get_settings()
    chunks = chunks_in_period(s.load_date_from, s.load_date_to, known_chains())
    failed = []
    with click.progressbar(chunks, label="перезаливка") as bar:
        for chunk in bar:
            result = load_chunk(chunk, target="sales_shadow")
            journal.record(result, operation="full_reload")
            if result.error:
                failed.append(result)

    if failed:
        click.echo(f"ОШИБКА: не залилось кусков {len(failed)}, обмен не выполнен. "
                   f"Боевые данные не тронуты, подробности в gfd-sync status")
        raise SystemExit(1)

    swap_shadow(ch)
    ch.command("TRUNCATE TABLE sales_shadow")
    click.echo(f"перезалито кусков: {len(chunks)}, данные переключены")


@cli.command()
@click.option("--limit", default=20)
def status(limit: int) -> None:
    """Последние операции синхронизации."""
    for row in journal.last_runs(limit):
        mark = "ok " if row["status"] == "ok" else "ERR"
        click.echo(f"{row['started_at']:%d.%m %H:%M} {mark} {row['operation']:<12} "
                   f"{row['chunk_key'] or '':<20} {row['rows']:>10} строк "
                   f"{row['seconds']:>7.1f} с {row['error'] or ''}")
```

- [ ] **Step 4: Прогнать тесты**

Run: `cd sync && pytest tests/test_cli.py -v`
Expected: шесть тестов PASS.

- [ ] **Step 5: Коммит**

```bash
git add sync/src/gfd_sync/cli.py sync/tests/test_cli.py
git commit -m "Командный интерфейс синхронизатора"
```

---

### Task 13: Замер на боевом объёме

**Files:**
- Create: `sync/tests/test_performance.py`, `docs/измерения.md`
- Test: `sync/tests/test_performance.py`

**Interfaces:**
- Consumes: всё предыдущее
- Produces: `docs/измерения.md` с числами; подтверждение, что критерий готовности этапа достигнут

- [ ] **Step 1: Написать тест производительности**

`sync/tests/test_performance.py`:

```python
"""Замеры на объёме, близком к боевому. Запускаются отдельно: pytest -m slow"""
import os
import time

import pytest
from gfd_sync.clients import ch_client
from gfd_sync.schema import create_dictionaries, create_sales_tables

pytestmark = pytest.mark.slow

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
    assert n > 50_000_000, (
        f"в витрине {n} строк. Сначала: python tools/gen_fake_data.py --rows 140000000, "
        f"затем gfd-sync full-reload --yes")
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
```

- [ ] **Step 2: Зарегистрировать метку `slow`**

В `sync/pyproject.toml`, секция `[tool.pytest.ini_options]`:

```toml
markers = ["slow: замеры на большом объёме, запускать отдельно"]
addopts = "-m 'not slow'"
```

- [ ] **Step 3: Сгенерировать объём и залить**

```bash
python tools/gen_fake_data.py --rows 140000000 --year 2026
cd sync && gfd-sync init && gfd-sync full-reload --yes
```

**Про объём.** 140 млн строк — это примерно 25 ГБ в PostgreSQL и около 25 минут
генерации. Если на домашней машине столько места нет, генерируй **20 млн**
(`--rows 20000000`, порядка 3,5 ГБ) и снижай порог в фикстуре до 15 млн:
на таком объёме проверяется правильность, а настоящие цифры всё равно
снимаются на сервере, где данные боевые. **Порог времени запросов при этом
не трогай** — на меньшем объёме он должен выполняться с запасом, и если
не выполняется, значит что-то не так с самой схемой.

- [ ] **Step 4: Прогнать замеры**

Run: `cd sync && pytest tests/test_performance.py -v -m slow`
Expected: три теста PASS. Если главный запрос не уложился в секунду — не подгонять порог, а разбираться: проверить `system.query_log` на объём прочитанного и число прочитанных партиций.

- [ ] **Step 5: Записать результаты в `docs/измерения.md`**

```markdown
# Измерения

## Этап 1, замер от <дата>

Условия: <синтетика | боевые данные>, <N> строк за 2026 год, <M> сетей.
Железо: 8 ядер, 62 ГБ RAM, диск /mnt/gfd_data (виртуальный).

| Что | Результат |
|---|---|
| Объём витрины на диске | ГБ |
| Полная заливка | мин |
| Заливка одного куска «месяц × сеть» | с |
| Главный запрос (литры по месяцам) | мс |
| АКБ по бренду за год | мс |
| Сверка последних трёх месяцев | с |
| Полная сверка | мин |

Выводы и что тормозит:
```

- [ ] **Step 6: Коммит**

```bash
git add sync/tests/test_performance.py sync/pyproject.toml docs/измерения.md
git commit -m "Замеры на боевом объёме; критерий готовности этапа 1"
```

---

## Критерий готовности этапа

1. `gfd-sync init && gfd-sync full-reload --yes` наполняет витрину с нуля.
2. Главный запрос отвечает быстрее секунды на полном объёме.
3. Правка `vkus` в PostgreSQL видна в ClickHouse через минуту без перезаливки.
4. `gfd-sync verify --all` на нетронутых данных не находит расхождений; после ручной правки в источнике находит её и чинит флагом `--repair`.
5. Убийство процесса посреди заливки не портит витрину — предыдущие данные на месте.
6. Все тесты, кроме помеченных `slow`, проходят.
7. Замеры записаны в `docs/измерения.md`.

## Что делать после

Расписание (`cron` или systemd timer): `gfd-sync sync` каждые 10 минут,
`gfd-sync verify --all --repair` ночью. Настраивается на пятом этапе,
вместе с остальной эксплуатацией.

Второй этап — слой метрик поверх готовой витрины.
