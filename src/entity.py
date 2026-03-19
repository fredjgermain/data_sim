import pandas as pd
from dataclasses import dataclass, fields, field

import typing

#from src.entity_annotation import PrimaryKey, CreationTime, Faker, ForeignFields, ForeignKey, Pattern, Unique 



class Entity:
    """Base class for all simulated entities.

    Provides classmethod introspection and enforces annotation uniqueness
    (at most one PrimaryKey, at most one CreationTime) via __init_subclass__,
    which runs automatically at class definition time.
    """

    # def __init_subclass__(cls, **kwargs):
    #     super().__init_subclass__(**kwargs)
    #     # ! Validate that there's no more than one primarykey and one creation time on the same entity 
    #     for annotation, label in [(PrimaryKey, "PrimaryKey"), (CreationTime, "CreationTime")]:
    #         flagged = [f.name for f in cls.inspect().values() if f.has(annotation)]
    #         if len(flagged) > 1:
    #             raise TypeError(f"'{cls.__name__}' has multiple {label} fields: {flagged}.")

    @classmethod
    def inspect(cls) -> dict[str, EntityField]:
        result = {}
        hints = typing.get_type_hints(cls, include_extras=True)
        for f in fields(cls):
            hint = hints[f.name]
            if typing.get_origin(hint) is typing.Annotated:
                base_type, *annotations = typing.get_args(hint)
                func = next((a for a in annotations if callable(a)), None)
                meta = {type(a): a for a in annotations if not callable(a)}
                result[f.name] = EntityField(name=f.name, base_type=base_type, func=func, annotations=meta)
            else:
                result[f.name] = EntityField(name=f.name, base_type=hint)
        return result

    @classmethod  
    def get_fields_with_annotation(cls, *annotations) -> list[EntityField]:
        return [fld for fld in cls.inspect().values() if any(a in fld.annotations for a in annotations)]

    @classmethod
    def primary_key_field(cls) -> EntityField | None:
        return next((f for f in cls.inspect().values() if PrimaryKey in f.annotations ), None)

    @classmethod
    def primary_time_field(cls) -> EntityField | None:
        return next((f for f in cls.inspect().values() if CreationTime in f.annotations ), None)



@dataclass
class EntityField[T]:
    name: str
    base_type: type[T]
    func: callable | None = None
    annotations: dict = field(default_factory=dict)


from dataclasses import dataclass, field
import datetime


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
    start: datetime.datetime = datetime.datetime(2020, 1, 1)
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
class ForeignKey[E:Entity]:
    entity: type[Entity]


# ! indicates that a field depends on a foreign field.
@dataclass(frozen=True)
class ForeignFields[E:Entity]:
    columns: list[str]
    entity: type[Entity]


@dataclass(frozen=True)
class Pattern:
    regex: str