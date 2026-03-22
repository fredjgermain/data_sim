import pandas as pd 
from dataclasses import dataclass, field
from typing import Protocol, Callable, Any
from enum import Enum


class Distribution(Enum):
    NORMAL = "normal"
    UNIFORM = "uniform"
    LOGNORMAL = "lognormal"
    POISSON = "poisson"
    EXPONENTIAL = "exponential"


@dataclass(frozen=True)
class Dist:
    min: float | None = None 
    max: float | None = None 
    seed: int | None = None 
    rounding: int | None = None  # None=float, 0=int, n=decimal places
    
    def clip(self, serie: pd.Series) -> pd.Series:
        if self.min is None and self.max is None:
            return serie
        return serie.clip(lower=self.min, upper=self.max)
    
    def apply_rounding(self, serie: pd.Series) -> pd.Series:
        if self.rounding is None:
            return serie
        if self.rounding == 0:
            return serie.round(0).astype(int)
        return serie.round(self.rounding)
    
    def generate(self, N:int) -> pd.Series:
        raise NotImplementedError


@dataclass
class EntityField[T]:
    name: str
    base_type: type[T]
    func: Callable[..., Any] | None = None
    annotations: dict = field(default_factory=dict)
    
    def get[A](self, annotation: type[A]) -> A | None:
        return self.annotations.get(annotation)

    def has(self, *annotations) -> bool:
        return any(a in self.annotations for a in annotations)
    
    def get_dist(self) -> Dist | None:
        return next((v for v in self.annotations.values() if isinstance(v, Dist)), None)
    
    def is_numerical(self) -> bool:
        return any(issubclass(k, Dist) for k in self.annotations)


class IEntity(Protocol):

    @classmethod
    def inspect(cls) -> dict[str, EntityField]: ...
    
    @classmethod 
    def get_fields_by_annotation(cls, selection: list = None, exclusion: list = None) -> list[EntityField]: ...

    @classmethod
    def get_primary_key_field(cls) -> EntityField | None: ... 
    
    @classmethod
    def get_primary_time_field(cls) -> EntityField | None: ... 

