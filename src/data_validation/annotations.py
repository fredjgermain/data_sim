import pandas as pd
from dataclasses import dataclass, field
from typing import Callable, Any, Literal

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
  
  def check_missing_column(self, ctx:ValidCtx) -> pd.Series: 
    try:
      return ctx.data[ctx.name] 
    except:
      raise Exception(f'Validation exception: Missing column {ctx.name} from dataframe.')

  
  def make_validation_report(self, ctx:ValidCtx, result:tuple[float, pd.Series, bool]) -> ValidationReport:
    invalid_percent, invalid_values, success = result
    return ValidationReport( 
        validation_name = self.__class__.__name__, 
        field_name = ctx.name, 
        invalid_percent= invalid_percent, 
        invalid_values = invalid_values, 
        success = success, 
        options=self.__dict__ 
      ) 



@dataclass 
class CustomValidation(IValid): 
  func:Callable[[ValidCtx], tuple[float, pd.Series, bool]] 
  
  def validat(self, ctx:ValidCtx) -> ValidationReport: 
    return self.make_validation_report(ctx, self.func(ctx)) 


@dataclass
class Uniqueness(IValid):
    tolerance: float = 0.0 # duplicate percentage tolerated 

    def validate(self, ctx: ValidCtx) -> ValidationReport: 
      serie = self.check_missing_column(ctx)
      result = valid.uniqueness_validity(serie, self.tolerance) 
      return self.make_validation_report(ctx, result)


@dataclass
class Completeness(IValid): 
  tolerance: float = 0.0 
  count_as_missing: list = field(default_factory=list) 
  
  def validate(self, ctx: ValidCtx) -> ValidationReport: 
    serie = self.check_missing_column(ctx)
    result = valid.completeness_validity(serie, self.count_as_missing, self.tolerance) 
    return self.make_validation_report(ctx, result)


@dataclass
class ValidCategory(IValid):
    categories: list = field(default_factory=list)
    
    def __post_init__(self):
      if not bool(self.categories):
          raise ValueError(
              f"{self.__class__.__name__} attributes 'categories' should not be None or empty"
          )

    def validate(self, ctx: ValidCtx) -> ValidationReport: 
      serie = self.check_missing_column(ctx)
      result = valid.categorical_validity(serie, self.categories) 
      return self.make_validation_report(ctx, result)


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
      serie = self.check_missing_column(ctx)
      result = valid.range_validity(serie, self.min, self.max) 
      return self.make_validation_report(ctx, result)


@dataclass
class Outlier(IValid):
    multiplier: float = 1.5,  # Standard is 1.5, use 3.0 for "extreme" outliers
    tolerance: float = 0.05     # Max acceptable % of outliers

    def validate(self, ctx: ValidCtx) -> ValidationReport: 
      serie = self.check_missing_column(ctx)
      result = valid.outlier_validity(serie, self.multiplier, self.tolerance) 
      return self.make_validation_report(ctx, result)
      

@dataclass
class ValidFormat(IValid):
    pattern: str
    
    def validate(self, ctx: ValidCtx) -> ValidationReport: 
      serie = self.check_missing_column(ctx) 
      result = valid.format_validity(serie, self.pattern) 
      return self.make_validation_report(ctx, result) 


@dataclass
class ValidStringLength(IValid):
    min_length: int = None 
    max_length: int = None 
    
    def __post_init__(self):
        """Validate that min_length and max_length are coherent."""
        if self.min_length is not None and self.max_length is not None:
            if self.min_length > self.max_length:
                raise ValueError(
                    f"Invalid length range: min_length ({self.min_length}) cannot be greater than max_length ({self.max_length})"
                )
    
    def validate(self, ctx: ValidCtx) -> ValidationReport: 
      serie = self.check_missing_column(ctx) 
      result = valid.string_length_validity(serie, self.min_length, self.max_length) 
      return self.make_validation_report(ctx, result) 


@dataclass
class ValidDataType(IValid):
    expected_dtype: str  # 'int', 'float', 'datetime', 'bool', 'string'
    
    def __post_init__(self):
        """Validate that expected_dtype is supported."""
        valid_dtypes = ['int', 'float', 'datetime', 'bool', 'string']
        if self.expected_dtype not in valid_dtypes:
            raise ValueError(
                f"Invalid expected_dtype: {self.expected_dtype}. Must be one of {valid_dtypes}"
            )
    
    def validate(self, ctx: ValidCtx) -> ValidationReport: 
      serie = self.check_missing_column(ctx) 
      result = valid.datatype_validity(serie, self.expected_dtype) 
      return self.make_validation_report(ctx, result) 


@dataclass
class ValidTemporal(IValid):
    min_date: str = None
    max_date: str = None
    
    def validate(self, ctx: ValidCtx) -> ValidationReport: 
      serie = self.check_missing_column(ctx) 
      result = valid.temporal_validity(serie, self.min_date, self.max_date) 
      return self.make_validation_report(ctx, result) 


@dataclass
class ValidCardinality(IValid):
    min_unique: int = None
    max_unique: int = None
    
    def __post_init__(self):
        """Validate that min_unique and max_unique are coherent."""
        if self.min_unique is not None and self.max_unique is not None:
            if self.min_unique > self.max_unique:
                raise ValueError(
                    f"Invalid cardinality range: min_unique ({self.min_unique}) cannot be greater than max_unique ({self.max_unique})"
                )
    
    def validate(self, ctx: ValidCtx) -> ValidationReport: 
      serie = self.check_missing_column(ctx) 
      result = valid.cardinality_validity(serie, self.min_unique, self.max_unique) 
      return self.make_validation_report(ctx, result) 


@dataclass
class ValidMonotonic(IValid):
    direction: str  # 'increasing', 'decreasing', 'non-decreasing', 'non-increasing'
    
    def __post_init__(self):
        """Validate that direction is supported."""
        valid_directions = ['increasing', 'decreasing', 'non-decreasing', 'non-increasing']
        if self.direction not in valid_directions:
            raise ValueError(
                f"Invalid direction: {self.direction}. Must be one of {valid_directions}"
            )
    
    def validate(self, ctx: ValidCtx) -> ValidationReport: 
      serie = self.check_missing_column(ctx) 
      result = valid.monotonic_validity(serie, self.direction) 
      return self.make_validation_report(ctx, result) 