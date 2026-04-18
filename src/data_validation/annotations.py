import pandas as pd
from dataclasses import dataclass, field

from _common.interface import IAnnotation 
import data_validation.utils as valid 


@dataclass
class ValidCtx:
  name:str
  data: pd.DataFrame = field(default_factory=pd.DataFrame)


@dataclass
class ValidationReport:
    validation_name: str
    field_name:str # Field name 
    success:bool
    invalid_values: pd.Series



class IValid(IAnnotation):

    def validate(self, ctx:ValidCtx) -> ValidationReport:
        raise NotImplementedError



@dataclass
class Unique(IValid):

    def validate(self, ctx:ValidCtx) -> ValidationReport: 
      invalid_values = ctx.data.duplicated(keep=False) 
      return ValidationReport( 
        validation_name = self.__class__.__name__, 
        field_name = ctx.name, 
        success = not any(invalid_values), 
        invalid_values = invalid_values 
      ) 


@dataclass 
class InRange(IValid): 
  
  def validate(self, ctx:ValidCtx) -> ValidationReport: 
    ... 

