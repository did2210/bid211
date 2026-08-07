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
