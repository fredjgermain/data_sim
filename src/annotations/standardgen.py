import pandas as pd
import rstr
import faker
from dataclasses import dataclass
from typing import Callable
import datetime

from src.annotations.base import GenCtx, IStandardGen, IAnnotation
from src.utils import generator



@dataclass
class GenNum(IStandardGen): 
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
    
    def generate(self, ctx:GenCtx) -> pd.Series:
      raise NotImplementedError


@dataclass
class GenUniform(GenNum):

    def generate(self, ctx: GenCtx) -> pd.Series: 
        return generator.generate_uniform(
            ctx.N, self.seed, self.min or 0, self.max or 1) 
    

@dataclass
class GenNormal(GenNum):
    mean: float = 0 
    std:  float = 1 
    skewness: float = 0 

    def generate(self, ctx: GenCtx) -> pd.Series:
        return generator.generate_normal(
            ctx.N, 
            self.seed, 
            self.skewness, 
            self.mean, 
            self.std)



@dataclass
class GenGamma(GenNum):
    skewness: float = 1.0
    scale: float = 1.0
    
    def generate(self, ctx: GenCtx) -> pd.Series:
        return generator.generate_gamma(
            ctx.N, 
            self.seed, 
            self.skewness, 
            self.scale)


@dataclass
class GenPoisson(GenNum):
    mean: float = 1.0
    
    def generate(self, ctx: GenCtx) -> pd.Series:
        return generator.generate_poisson(ctx.N, self.seed, self.mean)


@dataclass
class GenExponential(GenNum):
  scale: float = 1.0

  def generate(self, ctx: GenCtx) -> pd.Series:
    return generator.generate_exponential(ctx.N, self.seed, self.scale)



@dataclass 
class GenDate(IStandardGen):
    start: datetime.datetime
    end: datetime.datetime
    
    def generate(self, ctx:GenCtx) -> pd.Series: 
        start = pd.Series([self.start] * ctx.N) 
        end = pd.Series([self.end] * ctx.N) 
        return generator.generate_date(self.seed, start, end) 


@dataclass 
class GenTime(IStandardGen):
    start: datetime.datetime
    end: datetime.datetime
    
    def generate(self, ctx:GenCtx) -> pd.Series: 
        start = pd.Series([self.start] * ctx.N) 
        end = pd.Series([self.end] * ctx.N) 
        return generator.generate_time(self.seed, start, end)
    

@dataclass
class GenCategorical(IStandardGen):
    categories: list
    weight: list | None = None 
    
    def generate(self, ctx:GenCtx):
        return generator.generate_categorical(
            ctx.N, self.seed, self.categories, self.weight)


@dataclass
class CustomGen(IStandardGen):
    fn: Callable[[GenCtx], pd.Series]
    """User-supplied function: (partial_df: DataFrame) -> Series."""
    
    def generate(self, ctx: GenCtx) -> pd.Series:
        return self.fn(ctx)


@dataclass
class GenFaker(IStandardGen):
    provider: str

    def generate(self, ctx: GenCtx) -> pd.Series:
        fkr = faker.Faker()
        provider_fn = getattr(fkr, self.provider, None)
        if provider_fn is None:
            raise AttributeError(
                f"Faker has no provider '{self.provider}'."
            )
        return pd.Series([provider_fn() for _ in range(ctx.N)])


@dataclass
class GenPattern(IStandardGen):
    pattern: str

    def generate(self, ctx: GenCtx) -> pd.Series:
        return pd.Series([rstr.xeger(self.pattern) for _ in range(ctx.N)])



# ! transforms serie after generation 
@dataclass 
class Transformer(IAnnotation):
    fn: Callable[[pd.Series], pd.Series]
    
    def transform(self, series:pd.Series) -> pd.Series: 
        return self.fn(series)

