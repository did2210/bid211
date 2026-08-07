"""Кусок данных — пара «календарный месяц + торговая сеть».

Такая нарезка выбрана потому, что файлы от сетей приходят в разное время:
пришёл Магнит за июль — подменяем только его, остальное не трогаем.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True, order=True)
class Chunk:
    year: int
    month: int
    chain: str

    def __post_init__(self) -> None:
        # Кусок с месяцем 13 дожил бы до имени партиции или до пустой выборки
        # и всплыл бы далеко от места, где его собрали.
        if not 1 <= self.month <= 12:
            raise ValueError(f"месяц вне 1..12: {self.month}")

    @property
    def partition_id(self) -> tuple[int, str]:
        """Значение ключа партиции ClickHouse: (toYYYYMM(pdate), client)."""
        return self.year * 100 + self.month, self.chain

    @property
    def date_from(self) -> date:
        return date(self.year, self.month, 1)

    @property
    def date_to(self) -> date:
        """Верхняя граница не включается."""
        return date(self.year + 1, 1, 1) if self.month == 12 \
            else date(self.year, self.month + 1, 1)

    @property
    def key(self) -> str:
        return f"{self.year * 100 + self.month}/{self.chain}"


def chunk_from_key(key: str) -> Chunk:
    ym, chain = key.split("/", 1)
    return Chunk(int(ym[:4]), int(ym[4:]), chain)


def chunk_of(pdate: date, chain: str) -> Chunk:
    return Chunk(pdate.year, pdate.month, chain)


def chunks_in_period(date_from: date, date_to: date,
                     chains: list[str]) -> list[Chunk]:
    """Все куски, пересекающиеся с полуинтервалом [date_from, date_to)."""
    out: list[Chunk] = []
    y, m = date_from.year, date_from.month
    while date(y, m, 1) < date_to:
        for chain in chains:
            out.append(Chunk(y, m, chain))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out
