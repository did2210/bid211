"""Общая подготовка тестов: настройки берутся из .env в корне проекта."""
import os
from pathlib import Path

import pytest
from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")

# Тесты заливают, чистят и подменяют таблицы — держать их в боевой базе
# нельзя: один прогон на сервере стёр бы витрину целиком. Работают они
# в отдельной базе ClickHouse, а замеры (метка slow) смотрят на настоящую.
ТЕСТОВАЯ_БАЗА = "gfd_test"


@pytest.fixture(scope="session", autouse=True)
def создать_тестовую_базу():
    import clickhouse_connect

    клиент = clickhouse_connect.get_client(
        host=os.environ["CH_HOST"], port=int(os.environ["CH_PORT"]),
        username=os.environ["CH_USER"], password=os.environ["CH_PASSWORD"],
    )
    клиент.command(f"CREATE DATABASE IF NOT EXISTS {ТЕСТОВАЯ_БАЗА}")


@pytest.fixture(autouse=True)
def тестовая_база(request, monkeypatch):
    """Уводит тесты в отдельную базу — кроме замеров, которым нужна витрина."""
    if "slow" in request.keywords:
        return
    monkeypatch.setenv("CH_DB", ТЕСТОВАЯ_БАЗА)
