-- Справочники не копируются в витрину, а читаются напрямую из PostgreSQL
-- с перечиткой раз в минуту. Правка бренда или вкуса появляется в отчётах
-- без перезаливки сотен миллионов строк продаж.
--
-- Хост и порт здесь — адрес PostgreSQL с точки зрения контейнера ClickHouse,
-- а не с точки зрения машины разработчика: за словарями ходит сам сервер.

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
