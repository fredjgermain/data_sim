"""
data_simulator.annotations
~~~~~~~~~~~~~~~~~~~~~~~~~~
All field-level annotation classes. Each annotation describes how a field's
data should be generated or post-processed during simulation.
"""
import numpy as np
import rstr
import faker
from dataclasses import dataclass
from typing import Callable
import datetime

import pandas as pd
from scipy.stats import skewnorm

from src.interface import IEntity
from src.annotations.base import GenCtx, IStandardGen, IAnnotation



# ! transforms serie after generation 
@dataclass 
class Transformer(IAnnotation):
    fn: Callable[[pd.Series], pd.Series]
    
    def transform(self, series:pd.Series) -> pd.Series: 
        return self.fn(series)



@dataclass 
class GenContextor(IAnnotation): 
    # ! get values from current entity 
    to_agg: dict[type[IEntity], list[str]] 
    
    def aggregate(self, ctx:GenCtx) -> dict[type[IEntity], pd.DataFrame]: 
        res = {} 
        for ent, cols in self.to_agg.items(): 
            res[ent] = ctx.foreign_datas[ent][cols] 
        return res 



@dataclass 
class FromField(IAnnotation): 
    foreignkey: dict # {foreignkey_name:[foreign_fields]} 
    
    # ! Custom Function test 
    def generate(self, ctx:GenCtx) -> pd.Series: 
        cdata = ctx.current_data.copy() 
        fdata = list(ctx.foreign_datas.values())[0] 
        left_on, right_on = '', '' 
        merged = pd.merge(cdata, fdata, left_on=left_on, right_on=right_on, how='left') 
        # make your calculation here
        return pd.Series()
        
        

    # def aggregate(self, current:pd.DataFrame, entity:type[IEntity], entities:dict[type[IEntity], pd.DataFrame]): 
    #     df = current.copy() 
    #     flds = [fld for fld in entity.find([ForeignKey]) if fld.name in self.foreignkey] 
    #     for fld in flds: 
    #         ann = fld.get(ForeignKey) 
    #         target = ann.target 
    #         fdata = entities[target][self.foreignkey[fld.name]] 
            

    # 
    # def aggregate(self, entity:type[IEntity], entities:dict[type[IEntity], pd.DataFrame]): 
    #     flds = [fld for fld in entity.find([ForeignKey]) if fld.name in self.foreignkey] 
    #     datas = {} 
    #     for fld in flds: 
    #         ann = fld.get(ForeignKey) 
    #         target = ann.target 
    #          = entities[target][self.foreignkey[fld.name]] 

        #for fk, cols in self.foreignkey.items():
        
        # fld = current_entity.get[self.foreignkey] 
        # ann = fk_fld.get(ForeignKey) 
        # ent = ann.target 
        # foreign_data 
        # target_fld 
        # merge(current, foreign_data, left_on=, right_on=, how='left') 
        # foreign data on current[self.foreignkey] 
        
        
        # left_on, right_on = self.merge_on if isinstance(self.merge_on, tuple) else [self.merge_on] * 2 
        
        # fdata = ctx.foreign_datas[self.target].copy() # test empty or None 
        # cdata = ctx.current_data.copy() # test empty or None 
        
        # fdata = fdata[[right_on, self.field]] 
        # cdata = cdata[[left_on]] 
        # merged = pd.merge(cdata, fdata, left_on=left_on, right_on=right_on, how='left') 
        # return merged[self.field] 


@dataclass 
class GenDate(IStandardGen):
    start: datetime.datetime
    """Earliest possible timestamp (inclusive)."""

    end: datetime.datetime
    """Latest possible timestamp (inclusive)."""
    
    def generate(self, ctx:GenCtx): 
        start_date = pd.Series([self.start] * ctx.N) 
        end_date = self.end 
        ranges = (end_date - start_date).dt.days.clip(lower=0) 
        random_days = (np.random.rand(len(start_date)) * (ranges + 1)).astype(int) 
        return start_date + pd.to_timedelta(random_days, unit='D') 


@dataclass
class GenCategorical(IStandardGen):
    categories: list
    weight: list | None = None 
    
    def generate(self, ctx:GenCtx):
        categories = self.categories
        weight = self.weight
        
        if weight and len(weight) != len(categories):
            weight = None
            raise Exception('Weights length must match encoding length or weight must be None')
        
        if self.weight:
            weight = weight / sum(weight)  # normalize to sum to 1
        return pd.Series(np.random.choice(categories, size=ctx.N, p=weight))



# ---------------------------------------------------------------------------
# CustomGen
# ---------------------------------------------------------------------------

@dataclass
class CustomGen(IStandardGen):
    fn: Callable[[GenCtx], pd.Series]
    """User-supplied function: (partial_df: DataFrame) -> Series."""
    
    def generate(self, ctx: GenCtx) -> pd.Series:
        return self.fn(ctx)

# ---------------------------------------------------------------------------
# Post process annotations 
# ---------------------------------------------------------------------------

# ! Coalesce(default=0) 
# MapValues ... encoding? 
# Scale? 
# Format(pattern="{:.2f}")



# ---------------------------------------------------------------------------
# GenFaker
# ---------------------------------------------------------------------------

@dataclass
class GenFaker(IStandardGen):
    """Generates values using the Faker library via a named provider.

    Example:
        email: Annotated[str, Unique(), Faker("email")]
    """

    provider: str
    """Name of the Faker provider method to call (e.g. "email", "name")."""

    def generate(self, ctx: GenCtx) -> pd.Series:
        fkr = faker.Faker()
        provider_fn = getattr(fkr, self.provider, None)
        if provider_fn is None:
            raise AttributeError(
                f"Faker has no provider '{self.provider}'."
            )
        return pd.Series([provider_fn() for _ in range(ctx.N)])


# ---------------------------------------------------------------------------
# GenPattern
# ---------------------------------------------------------------------------

@dataclass
class GenPattern(IStandardGen):
    """Generates string values matching a given regular expression pattern.

    Example:
        code: Annotated[str, GenPattern(r'[A-Z]{3}-\\d{4}')]
    """

    pattern: str
    """A regex pattern string compatible with the rstr library."""

    def generate(self, ctx: GenCtx) -> pd.Series:
        return pd.Series([rstr.xeger(self.pattern) for _ in range(ctx.N)])


# ---------------------------------------------------------------------------
# GenNormal
# ---------------------------------------------------------------------------



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
        import numpy as np
        if self.min is None or self.max is None:
            raise ValueError("GenUniform requires both min and max to be set.")
        rng = np.random.default_rng(self.seed)
        serie = pd.Series(rng.uniform(low=self.min, high=self.max, size=ctx.N))
        return self.apply_rounding(serie)


@dataclass
class GenNormal(GenNum):
    mean: float = 0 
    std:  float = 1 
    skewness: float = 0 

    # GenNormal
    def generate(self, ctx: GenCtx) -> pd.Series:
        rng = np.random.default_rng(self.seed)
        serie = pd.Series(skewnorm.rvs(a=self.skewness, loc=self.mean, scale=self.std, size=ctx.N, random_state=rng))
        serie = self.clip(serie)
        return self.apply_rounding(serie)


@dataclass
class GenGamma(GenNum):
    skewness: float = 1.0
    scale: float = 1.0
    
    def generate(self, ctx: GenCtx) -> pd.Series:
        rng = np.random.default_rng(self.seed)
        shape = (2 / self.skewness) ** 2
        res = pd.Series(rng.gamma(shape=shape, scale=self.scale, size=ctx.N))
        return self.clip(self.apply_rounding(res))


@dataclass
class GenPoisson(GenNum):
    mean: float = 1.0
    
    def generate(self, ctx: GenCtx) -> pd.Series:
        rng = np.random.default_rng(self.seed)
        res = pd.Series(rng.poisson(lam=self.mean, size=ctx.N))
        return self.clip(self.apply_rounding(res))


@dataclass
class GenExponential(GenNum):
    scale: float = 1.0

    def generate(self, ctx: GenCtx) -> pd.Series:
        rng = np.random.default_rng(self.seed)
        res = pd.Series(rng.exponential(scale=self.scale, size=ctx.N))
        return self.clip(self.apply_rounding(res))

