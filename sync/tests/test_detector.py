"""Обнаружение изменений через load_log: новые файлы сами говорят,
какие куски перезалить."""
import os
from datetime import datetime, timedelta, timezone

import psycopg

from gfd_sync.chunks import Chunk
from gfd_sync.detector import chunks_from_files, new_files, pending_chunks


def отметить_файл(имя):
    with psycopg.connect(os.environ["PG_SOURCE_DSN"], autocommit=True) as conn:
        conn.execute("""
            INSERT INTO load_log (file_path, file_hash, rows_loaded)
            VALUES (%s, 'abc123', 5000)
            ON CONFLICT (file_path) DO UPDATE SET loaded_at = now()
        """, (f"/mnt/gfd_data/in/{имя}",))


def test_новые_файлы_находятся():
    отметить_файл("МАГНИТ_2026_07.xlsx")
    got = new_files(since=datetime.now(timezone.utc) - timedelta(minutes=5))
    assert any("МАГНИТ_2026_07" in f["file_path"] for f in got)


def test_старые_файлы_не_попадают_в_новые():
    """Окно должно работать, иначе каждый прогон брал бы всю историю
    загрузок и перезаливал витрину целиком."""
    отметить_файл("МАГНИТ_2026_07.xlsx")
    got = new_files(since=datetime.now(timezone.utc) + timedelta(minutes=5))
    assert got == []


def test_куски_определяются_по_данным_а_не_по_имени_файла():
    """Имя файла — подсказка, но источником истины остаётся сама таблица sales:
    определяем, какие месяцы и сети реально пришли из этого файла."""
    files = [{"file_path": "/mnt/gfd_data/in/МАГНИТ_2026_07.xlsx"}]
    got = chunks_from_files(files)
    assert Chunk(2026, 7, "МАГНИТ") in got


def test_неизвестный_файл_не_роняет_обработку():
    got = chunks_from_files([{"file_path": "/mnt/gfd_data/in/мусор.txt"}])
    assert got == []


def test_исключённые_сети_не_попадают_в_куски():
    """Файлы от исключённых сетей приходят наравне с прочими. Пропусти их
    сюда — и каждый такой файл заводил бы перезаливку куска, которого
    в витрине быть не должно."""
    files = [{"file_path": "/mnt/gfd_data/in/КАРУСЕЛЬ_2026_07.xlsx"}]
    assert chunks_from_files(files) == []


def test_файл_с_непонятным_именем_всё_равно_разбирается():
    """Имя файла сужает поиск, но не заменяет данные. Файл, названный
    не по шаблону «СЕТЬ_ГГГГ_ММ», обязан находиться по содержимому —
    иначе его правки тихо не доехали бы до витрины."""
    имя = "выгрузка_вручную.xlsx"
    with psycopg.connect(os.environ["PG_SOURCE_DSN"], autocommit=True) as conn:
        (строка,) = conn.execute("""
            SELECT id FROM sales
            WHERE pdate >= '2026-07-01' AND pdate < '2026-08-01'
              AND upper(trim(client)) = 'МАГНИТ'
            ORDER BY id LIMIT 1
        """).fetchone()
        было = conn.execute(
            "SELECT name_file FROM sales WHERE id = %s", (строка,)).fetchone()[0]
        conn.execute("UPDATE sales SET name_file = %s WHERE id = %s", (имя, строка))
        try:
            got = chunks_from_files([{"file_path": f"/mnt/gfd_data/in/{имя}"}])
            assert got == [Chunk(2026, 7, "МАГНИТ")]
        finally:
            conn.execute(
                "UPDATE sales SET name_file = %s WHERE id = %s", (было, строка))


def test_ожидающие_куски_не_повторяются():
    отметить_файл("МАГНИТ_2026_07.xlsx")
    got = pending_chunks()
    assert got, "только что отмеченный файл обязан дать хотя бы один кусок"
    assert len(got) == len(set(got))
    assert Chunk(2026, 7, "МАГНИТ") in got
