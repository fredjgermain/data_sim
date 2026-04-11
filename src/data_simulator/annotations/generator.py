import pandas as pd
from dataclasses import dataclass, field
from typing import Callable
import datetime

from data_simulator.interface import IEntity, IAnnotation, IEntityContext
from data_simulator.annotations.primaries import ForeignKey, PrimaryKey
from data_simulator.utils import generator 


@dataclass
class GenCtx: 
  name:str 
  N:int 
  entity:type[IEntity] 
  current_data:pd.DataFrame = field(default_factory=pd.DataFrame) 
  foreign_datas:dict[type[IEntity], pd.DataFrame] = field(default_factory=dict)   
  
  # ! helper function for CustomGen
  def from_foreign(self, foreignkey:str, foreignfields:list[str]) -> pd.DataFrame: 
    target = self.entity.get(foreignkey).get(ForeignKey).target 
    target_pk = target.get(PrimaryKey) 
    cdata = self.current_data[[foreignkey]] 
    fdata = self.foreign_datas[target][[target_pk.name, *foreignfields]] 
    return pd.merge(cdata, fdata, left_on=foreignkey, right_on=target_pk.name, how='left') 


class IGen(IAnnotation):

    def generate(self, ctx:GenCtx) -> pd.Series:
      raise NotImplementedError



# ! GENERATE Custom ==========================================
@dataclass
class CustomGen(IGen):
    fn: Callable[[GenCtx], pd.Series]
    seed: int | None = None 
    """User-supplied function: (partial_df: DataFrame) -> Series."""
    
    def generate(self, ctx: GenCtx) -> pd.Series:
        return self.fn(ctx)


# ! GENERATE from foreign key ===================================
@dataclass 
class FromForeignKey(IGen): 
  foreignkey:str 
  foreignfield: str 
  
  def generate(self, ctx:GenCtx) -> pd.Series: 
    merged = ctx.from_foreign(self.foreignkey, [self.foreignfield]) 
    return merged[self.foreignfield] 



# ! GENERATE date and time ==========================================
@dataclass 
class GenDate(IGen):
  start: datetime.datetime
  end: datetime.datetime
  seed: int | None = None 
    
  def generate(self, ctx:GenCtx) -> pd.Series: 
      start = pd.Series([self.start] * ctx.N) 
      end = pd.Series([self.end] * ctx.N) 
      return generator.generate_date(self.seed, start, end) 


@dataclass 
class GenTime(IGen):
  start: datetime.datetime
  end: datetime.datetime
  seed: int | None = None 
    
  def generate(self, ctx:GenCtx) -> pd.Series: 
    start = pd.Series([self.start] * ctx.N) 
    end = pd.Series([self.end] * ctx.N) 
    return generator.generate_time(self.seed, start, end) 



# ! GENERATE date and time ==========================================
@dataclass
class GenCategorical(IGen):
    categories: list
    weight: list | None = None 
    seed: int | None = None 
    
    def generate(self, ctx:GenCtx):
        return generator.generate_categorical(ctx.N, self.seed, self.categories, self.weight)



# ! GENERATE Faker ==========================================
@dataclass
class GenFaker(IGen):
    provider: str
    seed: int | None = None 

    def generate(self, ctx: GenCtx) -> pd.Series: 
      return generator.generate_with_faker(ctx.N, self.seed, self.provider) 
        # fkr = faker.Faker()
        # provider_fn = getattr(fkr, self.provider, None)
        # if provider_fn is None:
        #     raise AttributeError(
        #         f"Faker has no provider '{self.provider}'."
        #     )
        # return pd.Series([provider_fn() for _ in range(ctx.N)])


# ! GENERATE Pattern ==========================================
@dataclass
class GenPattern(IGen):
    pattern: str
    seed: int | None = None 

    def generate(self, ctx: GenCtx) -> pd.Series: 
      return generator.generate_pattern(ctx.N, self.seed, self.pattern) 
      #return pd.Series([rstr.xeger(self.pattern) for _ in range(ctx.N)]) 



# ! GENERATE NUMERICAL =========================================
@dataclass
class GenNum(IGen): 
    min: float | None = None 
    max: float | None = None 
    seed: int | None = None 
    rounding: int | None = None  # None=float, 0=int, n=decimal places
    seed: int | None = None 
    
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
    
    def generate(self, ctx:GenCtx) -> pd.Series:
      raise NotImplementedError


@dataclass
class GenUniform(GenNum):
  seed: int | None = None 

  def generate(self, ctx: GenCtx) -> pd.Series: 
      return generator.generate_uniform(
          ctx.N, self.seed, self.min or 0, self.max or 1) 
    

@dataclass
class GenNormal(GenNum):
    mean: float = 0 
    std:  float = 1 
    skewness: float = 0 
    seed: int | None = None 

    def generate(self, ctx: GenCtx) -> pd.Series:
        res = generator.generate_normal(
            ctx.N, 
            self.seed, 
            self.skewness, 
            self.mean, 
            self.std)
        return self.clip(self.apply_rounding(res))



@dataclass
class GenGamma(GenNum):
    skewness: float = 1.0
    scale: float = 1.0
    seed: int | None = None 
    
    def generate(self, ctx: GenCtx) -> pd.Series:
        res = generator.generate_gamma(
            ctx.N, 
            self.seed, 
            self.skewness, 
            self.scale)
        return self.clip(self.apply_rounding(res))


@dataclass
class GenPoisson(GenNum):
    mean: float = 1.0
    seed: int | None = None 
    
    def generate(self, ctx: GenCtx) -> pd.Series:
        res = generator.generate_poisson(ctx.N, self.seed, self.mean)
        return self.clip(self.apply_rounding(res))


@dataclass
class GenExponential(GenNum):
  scale: float = 1.0
  seed: int | None = None 

  def generate(self, ctx: GenCtx) -> pd.Series:
    res = generator.generate_exponential(ctx.N, self.seed, self.scale)
    return self.clip(self.apply_rounding(res))



# ! TRANSFORM ========================================================
@dataclass 
class Transformer(IAnnotation):
    fn: Callable[[pd.Series], pd.Series]
    seed: int | None = None 
    
    def transform(self, series:pd.Series) -> pd.Series: 
        return self.fn(series, self.seed) 

