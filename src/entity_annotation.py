from dataclasses import dataclass, field
import datetime


from src.entity_common import IEntity


# ---------------------------------------------------------------------------
# Annotations
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CreationTime:
    """Marks the single datetime field that records when an entity was created.
    
    Args:
        start: Earliest possible creation datetime (inclusive).
        end:   Latest possible creation datetime (inclusive).
               Defaults to now if omitted.
    """
    start: datetime.datetime = datetime.datetime(2000, 1, 1)
    end: datetime.datetime = field(default_factory=datetime.datetime.now)


@dataclass(frozen=True)
class PrimaryKey:
    pass


@dataclass(frozen=True)
class Unique:
    pass


@dataclass(frozen=True)
class Faker:
    method: str
    locale: str = "en_US"


@dataclass(frozen=True)
class ForeignKey:
    entity: type[IEntity]


# ! indicates that a field depends on a foreign field.
@dataclass(frozen=True)
class ForeignFields:
    columns: list[str]
    entity: type[IEntity]


@dataclass(frozen=True)
class Pattern:
    regex: str