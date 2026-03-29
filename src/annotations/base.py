# ---------------------------------------------------------------------------
# Base Annotations 
# ---------------------------------------------------------------------------

import pandas as pd
from dataclasses import dataclass, field
from src.interface import IAnnotation, IEntity



class IGen(IAnnotation):

    def generate(self, ctx:GenCtx) -> pd.Series:
      raise NotImplementedError


class IStandardGen(IGen):
    ...


class IValid(IAnnotation):

    def validate(self, ctx:ValidCtx) -> ValidationReport:
        raise NotImplementedError


class IFault(IAnnotation):

    def inject(self, ctx:FaultCtx) -> pd.Series:
        raise NotImplementedError



@dataclass
class GenCtx:
    name:str
    N:int
    current_data:pd.DataFrame = field(default_factory=pd.DataFrame)
    foreign_datas:dict[type[IEntity], pd.DataFrame] = field(default_factory=dict)


@dataclass
class FaultCtx:
    name:str
    current_serie: pd.Series = field(default_factory=pd.Series)


@dataclass
class ValidationReport:
    validation_name: str
    field_name:str # Field name 
    success:bool 
    invalid_values: pd.Series


@dataclass
class ValidCtx:
    name:str
    current_serie: pd.Series = field(default_factory=pd.Series)


