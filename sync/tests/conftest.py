"""Общая подготовка тестов: настройки берутся из .env в корне проекта."""
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).resolve().parents[2] / ".env")
