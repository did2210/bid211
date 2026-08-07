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
