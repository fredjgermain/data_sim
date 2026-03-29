import pandas as pd
from dataclasses import dataclass

from src.annotations.base import ValidCtx, IValid, ValidationReport



@dataclass
class Unique(IValid):

    def validate(self, ctx:ValidCtx) -> ValidationReport: 
      invalid_values = ctx.current_serie.duplicated(keep=False) 
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

