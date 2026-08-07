"""Журнал: тихих поломок быть не должно — всё видно в истории."""
import pytest

from gfd_sync.chunks import Chunk
from gfd_sync.clients import pg_app
from gfd_sync.journal import (ensure_app_schema, has_failures, last_runs,
                              last_success, record)
from gfd_sync.loader import LoadResult


@pytest.fixture(autouse=True)
def чистый_журнал():
    """Журнал — накопительная таблица, и прошлые прогоны в ней остаются.

    Без очистки проверки вроде «ошибок нет» зеленели бы или краснели
    в зависимости от того, что записал предыдущий запуск тестов.
    """
    ensure_app_schema()
    with pg_app() as conn:
        conn.execute("TRUNCATE TABLE sync_journal RESTART IDENTITY")
    yield


def test_схема_служебной_базы_создаётся_кодом():
    """Init-скрипты PostgreSQL выполняются только на пустом каталоге данных.

    На сервере база уже развёрнута, поэтому единственный способ получить
    таблицу журнала — создать её кодом. Иначе синхронизатор молча
    поедет без истории.
    """
    with pg_app() as conn:
        conn.execute("DROP TABLE IF EXISTS sync_journal")
    ensure_app_schema()
    with pg_app() as conn:
        есть = conn.execute(
            "SELECT to_regclass('public.sync_journal') IS NOT NULL").fetchone()[0]
    assert есть


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
    record(LoadResult(chunk, 0, 1.0, False, "расхождение с источником"),
           operation="load")
    top = last_runs(1)[0]
    assert top["status"] == "error"
    assert "расхождение" in top["error"]


def test_время_последнего_успеха():
    chunk = Chunk(2026, 6, "ДИКСИ")
    assert last_success(chunk) is None
    record(LoadResult(chunk, 10, 1.0, True, None), operation="load")
    assert last_success(chunk) is not None


def test_неудачная_заливка_не_считается_успехом():
    """Иначе «когда этот кусок последний раз обновлялся» показывало бы время
    падения, и устаревшие данные выглядели бы свежими."""
    chunk = Chunk(2026, 6, "ОКЕЙ")
    record(LoadResult(chunk, 0, 1.0, False, "источник недоступен"),
           operation="load")
    assert last_success(chunk) is None


def test_флаг_свежих_ошибок():
    assert has_failures(since_minutes=60) is False, "журнал пуст — ошибок нет"
    record(LoadResult(Chunk(2026, 5, "АШАН"), 0, 1.0, False, "источник недоступен"),
           operation="load")
    assert has_failures(since_minutes=60) is True


def test_старые_ошибки_не_поднимают_флаг():
    """Флаг говорит «горит сейчас», а не «когда-то горело»."""
    record(LoadResult(Chunk(2026, 5, "АШАН"), 0, 1.0, False, "давняя беда"),
           operation="load")
    with pg_app() as conn:
        conn.execute(
            "UPDATE sync_journal SET started_at = now() - interval '3 hours'")
    assert has_failures(since_minutes=60) is False
    assert has_failures(since_minutes=60 * 24) is True
