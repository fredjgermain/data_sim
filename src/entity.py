
from dataclasses import dataclass
from typing import Any



@dataclass(frozen=True)
class PrimaryKey:
    strategy: str = "sequential"  # sequential | random | uuid | regex
    pattern: str | None = None    # required if strategy == "regex"


@dataclass(frozen=True)
class ForeignKey:
    entity: type                  # the referenced dataclass e.g. Customer
    attribute: str                # the referenced attribute name e.g. "customer_id"


@dataclass(frozen=True)
class Unique:
    pass                          # marker, no parameters needed


@dataclass(frozen=True)
class Faker:
    provider: str                 # e.g. "email", "first_name", "address"
    locale: str = "en_US"


@dataclass(frozen=True)
class Distribution:
    kind: str                     # normal | uniform | skewed
    mean: float | None = None
    std: float | None = None
    min: float | None = None
    max: float | None = None
    skewness: float | None = None


@dataclass(frozen=True)
class Categorical:
    values: tuple[str, ...]       # tuple not list — keeps frozen=True valid
    distribution: tuple[float, ...] | None = None
    encoding: dict[str, int] | None = None


@dataclass(frozen=True)
class Temporal:
    before: str | None = None     # intra-entity: attribute name on same entity
    after: str | None = None      # intra-entity: attribute name on same entity
    anchor: bool = False          # True = this is the chronological sort key

