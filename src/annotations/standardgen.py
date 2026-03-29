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


import pandas as pd
from scipy.stats import skewnorm

from src.annotations.base import GenCtx, IStandardGen, IGen


@dataclass
class GenCategorical(IStandardGen):
    encoding: dict
    weight: list | None = None 
    
    def generate(self, ctx:GenCtx):
        values = list(self.encoding.keys())
        weight = self.weight
        
        if weight and len(weight) != len(values):
            weight = None
            raise Exception('Weights length must match encoding length or weight must be None')
        
        if self.weight:
            weight = weight / sum(weight)  # normalize to sum to 1
        return pd.Series(np.random.choice(values, size=ctx.N, p=weight))



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
    
    def generate(self, N: int) -> pd.Series:
        rng = np.random.default_rng(self.seed)
        shape = (2 / self.skewness) ** 2
        res = pd.Series(rng.gamma(shape=shape, scale=self.scale, size=N))
        return self.clip(self.apply_rounding(res))


@dataclass
class GenPoisson(GenNum):
    mean: float = 1.0
    
    def generate(self, N: int) -> pd.Series:
        rng = np.random.default_rng(self.seed)
        res = pd.Series(rng.poisson(lam=self.mean, size=N))
        return self.clip(self.apply_rounding(res))


@dataclass
class GenExponential(GenNum):
    scale: float = 1.0

    def generate(self, N: int) -> pd.Series:
        rng = np.random.default_rng(self.seed)
        res = pd.Series(rng.exponential(scale=self.scale, size=N))
        return self.clip(self.apply_rounding(res))


