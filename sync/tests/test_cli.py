"""Командный интерфейс — то, чем пользуются расписание и админка."""
from click.testing import CliRunner

from gfd_sync import cli as cli_module
from gfd_sync.chunks import Chunk
from gfd_sync.cli import cli
from gfd_sync.clients import pg_app


def test_init_создаёт_схему():
    r = CliRunner().invoke(cli, ["init"])
    assert r.exit_code == 0
    assert "готово" in r.output.lower()


def test_init_создаёт_и_служебную_схему():
    """На сервере init-скрипты PostgreSQL не отработают: база уже развёрнута.
    Не создай init таблицу журнала — первая же заливка упала бы на записи
    в историю, уже перелив данные."""
    with pg_app() as conn:
        conn.execute("DROP TABLE IF EXISTS sync_journal")
    assert CliRunner().invoke(cli, ["init"]).exit_code == 0
    with pg_app() as conn:
        есть = conn.execute(
            "SELECT to_regclass('public.sync_journal') IS NOT NULL").fetchone()[0]
    assert есть


def test_load_принимает_ключ_куска():
    r = CliRunner().invoke(cli, ["load", "--chunk", "202607/МАГНИТ"])
    assert r.exit_code == 0
    assert "202607/МАГНИТ" in r.output


def test_кривой_ключ_куска_объясняется_по_человечески():
    """Опечатка в ключе — обычное дело при ручном запуске. Пользователь
    должен увидеть, что не так, а не трассировку стека."""
    r = CliRunner().invoke(cli, ["load", "--chunk", "202613/МАГНИТ"])
    assert r.exit_code != 0
    assert "202613/МАГНИТ" in r.output
    assert "Traceback" not in r.output


def test_verify_без_расхождений_возвращает_ноль():
    CliRunner().invoke(cli, ["load", "--chunk", "202607/МАГНИТ"])
    r = CliRunner().invoke(cli, ["verify", "--chunk", "202607/МАГНИТ"])
    assert r.exit_code == 0
    assert "расхождений нет" in r.output.lower()


def test_verify_показывает_расхождение_и_чинит_по_флагу():
    chunk = Chunk(2026, 7, "ВЕРНЫЙ")
    CliRunner().invoke(cli, ["load", "--chunk", chunk.key])
    cli_module.ch_client().command(
        f"ALTER TABLE sales DROP PARTITION (202607, '{chunk.chain}')")

    r = CliRunner().invoke(cli, ["verify", "--chunk", chunk.key])
    assert chunk.key in r.output, "расхождение обязано быть названо"

    r = CliRunner().invoke(cli, ["verify", "--chunk", chunk.key, "--repair"])
    assert r.exit_code == 0
    assert CliRunner().invoke(cli, ["verify", "--chunk", chunk.key]).output.lower() \
        .count("расхождений нет") == 1


def test_sync_заливает_ожидающие_куски(monkeypatch):
    """Настоящий sync разгребает всё, что пришло за сутки; в тесте
    ограничиваем список одним куском, иначе прогон занял бы минуты."""
    chunk = Chunk(2026, 7, "ОКЕЙ")
    monkeypatch.setattr(cli_module, "pending_chunks", lambda: [chunk])
    monkeypatch.setattr(cli_module, "verify_recent", lambda months=3: [])
    r = CliRunner().invoke(cli, ["sync"])
    assert r.exit_code == 0
    assert chunk.key in r.output


def test_full_reload_требует_подтверждения():
    """Полная перезаливка читает сотни гигабайт с боевого диска —
    случайный запуск недопустим."""
    r = CliRunner().invoke(cli, ["full-reload"], input="нет\n")
    assert "отменено" in r.output.lower()


def test_status_показывает_историю():
    r = CliRunner().invoke(cli, ["status"])
    assert r.exit_code == 0
