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
    invalid_percent: float 
    invalid_values: pd.Series
    success:bool
    options: dict



class IValid(IAnnotation):

  def validate(self, ctx:ValidCtx) -> ValidationReport:
      raise NotImplementedError


@dataclass
class Uniqueness(IValid):
    tolerance: float = 0.0 # duplicate percentage tolerated 

    def validate(self, ctx:ValidCtx) -> ValidationReport: 
      try:
        serie = ctx.data[ctx.name] 
      except:
        raise Exception(f'Validation exception: Missing column {ctx.name} from dataframe.')

      invalid_percent, invalid_values, success = valid.uniqueness(serie, self.tolerance)
      return ValidationReport( 
        validation_name = self.__class__.__name__, 
        field_name = ctx.name, 
        invalid_percent= invalid_percent, 
        invalid_values = invalid_values, 
        success = success, 
        options=self.__dict__ 
      ) 


@dataclass
class Compleness(IValid): 
  tolerance: float = 0.0 
  count_as_missing: list = field(default_factory=list) 
  
  def validate(self, ctx:ValidCtx) -> ValidationReport: 
    try: 
      serie = ctx.data[ctx.name] 
    except: 
      raise Exception(f'Validation exception: Missing column {ctx.name} from dataframe.') 
    
    invalid_percent, invalid_values, success = valid.uniqueness(serie, self.tolerance)
    return ValidationReport( 
      validation_name = self.__class__.__name__, 
      field_name = ctx.name, 
      invalid_percent= invalid_percent, 
      invalid_values = invalid_values, 
      success = success, 
      options=self.__dict__ 
    ) 


@dataclass
class ValidCategory(IValid):
    categories: list = field(default_factory=list)
    
    def __post_init__(self):
      if not bool(self.categories):
          raise ValueError(
              f"{self.__class__.__name__} attributes 'categories' should not be None or empty"
          )

    def validate(self, ctx: ValidCtx) -> ValidationReport: 
      try: 
        serie = ctx.data[ctx.name] 
      except: 
        raise Exception(f'Validation exception: Missing column {ctx.name} from dataframe.') 
      
      invalid_percent, invalid_values, success = valid.categorical_validity(serie, self.categories) 
      return ValidationReport( 
        validation_name = self.__class__.__name__, 
        field_name = ctx.name, 
        invalid_percent= invalid_percent, 
        invalid_values = invalid_values, 
        success = success, 
        options=self.__dict__ 
      ) 


@dataclass
class ValidRange(IValid):
    min: float = None 
    max: float = None 
    
    def __post_init__(self):
      """Validate that min and max are coherent."""
      if self.min is not None and self.max is not None:
          if self.min > self.max:
              raise ValueError(
                  f"Invalid range: min ({self.min}) cannot be greater than max ({self.max})"
              )
    
    def validate(self, ctx: ValidCtx) -> ValidationReport: 
      try: 
        serie = ctx.data[ctx.name] 
      except: 
        raise Exception(f'Validation exception: Missing column {ctx.name} from dataframe.') 
      
      invalid_percent, invalid_values, success = valid.range_validity(serie, self.min, self.max) 
      return ValidationReport( 
        validation_name = self.__class__.__name__, 
        field_name = ctx.name, 
        invalid_percent= invalid_percent, 
        invalid_values = invalid_values, 
        success = success, 
        options=self.__dict__ 
      ) 


@dataclass
class Outlier(IValid):
    multiplier: float = 1.5,  # Standard is 1.5, use 3.0 for "extreme" outliers
    tolerance: float = 0.05     # Max acceptable % of outliers

    def validate(self, ctx) -> ValidationReport:
      try: 
        serie = ctx.data[ctx.name] 
      except: 
        raise Exception(f'Validation exception: Missing column {ctx.name} from dataframe.') 
      
      invalid_percent, invalid_values, success = valid.outlier_detection_iqr(serie, self.multiplier, self.tolerance) 
      return ValidationReport( 
        validation_name = self.__class__.__name__, 
        field_name = ctx.name, 
        invalid_percent= invalid_percent, 
        invalid_values = invalid_values, 
        success = success, 
        options=self.__dict__ 
      ) 
      
      