-- Служебная база синхронизатора. Здесь лежит то, что правится людьми
-- или пишется самой заливкой, — в боевой источник не пишем ничего.
--
-- Файл обязан оставаться идемпотентным: его применяет и docker-энтрипоинт
-- при первом запуске, и сам синхронизатор через journal.ensure_app_schema().
-- Второй путь — единственный рабочий на сервере, где база уже развёрнута:
-- init-скрипты PostgreSQL выполняются только на пустом каталоге данных.

-- Журнал синхронизации: что делали, сколько строк, сколько времени,
-- чем кончилось. Админка первого этапа — это выборки отсюда.
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

-- Переименование сетей. В Qlik это было зашито в скрипт загрузки;
-- здесь — таблица, которую можно править без перезаливки продаж.
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
