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
from .chunks import Chunk, chunk_from_key, chunks_in_period
from .clients import ch_client
from .config import get_settings
from .detector import pending_chunks
from .loader import create_shadow, load_chunk, swap_shadow
from .schema import create_dictionaries, create_sales_tables
from .verifier import known_chains, repair, verify_all, verify_chunks, verify_recent


def _кусок(ключ: str) -> Chunk:
    """Разбирает ключ куска, объясняя ошибку по-человечески.

    Ключ приходит из рук человека или из расписания; опечатка не должна
    выглядеть как падение программы.
    """
    try:
        return chunk_from_key(ключ)
    except (ValueError, IndexError) as exc:
        raise click.BadParameter(
            f"{ключ}: не похоже на ключ куска (ждём вид 202607/МАГНИТ) — {exc}"
        ) from exc


@click.group()
def cli() -> None:
    """Синхронизация витрины ClickHouse с боевым PostgreSQL."""


@cli.command()
def init() -> None:
    """Создать таблицы и словари."""
    ch = ch_client()
    create_sales_tables(ch)
    create_dictionaries(ch)
    # Схема служебной базы — тоже часть init: на сервере init-скрипты
    # PostgreSQL не отработают, база там уже развёрнута.
    journal.ensure_app_schema()
    click.echo("Готово: таблицы, словари и журнал созданы")


@cli.command()
@click.option("--chunk", "chunk_key", required=True, help="ключ вида 202607/МАГНИТ")
def load(chunk_key: str) -> None:
    """Залить один кусок."""
    result = load_chunk(_кусок(chunk_key))
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
        mismatches = verify_chunks([_кусок(chunk_key)])
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
            click.echo(f"{result.chunk.key}: "
                       f"{'починено' if result.replaced else result.error}")


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
    # Прерванная заливка оставляет в промежуточной таблице партиции тех
    # кусков, на которых её убили. Сами по себе они не мешают, но занимают
    # место и копятся от сбоя к сбою — перед долгой операцией убираем.
    ch.command("TRUNCATE TABLE sales_staging")

    s = get_settings()
    chunks = chunks_in_period(s.load_date_from, s.load_date_to,
                              known_chains(s.load_date_from, s.load_date_to))
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
