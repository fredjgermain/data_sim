import pandas as pd 
from dataclasses import dataclass, field
from typing import Protocol, Callable, Any


@dataclass
class EntityField[T]:
    name: str
    base_type: type[T]
    func: Callable[..., Any] | None = None
    annotations: dict = field(default_factory=dict)


class IEntity(Protocol):

    @classmethod
    def inspect(cls) -> dict[str, EntityField]: ...
    
    @classmethod 
    def get_fields_by_annotation(cls, selection: list = None, exclusion: list = None) -> list[EntityField]: ...

    @classmethod
    def get_primary_key_field(cls) -> EntityField | None: ... 
    
    @classmethod
    def get_primary_time_field(cls) -> EntityField | None: ... 

