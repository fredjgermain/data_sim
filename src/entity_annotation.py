import numpy as np
import pandas as pd
from dataclasses import dataclass, field, fields
from scipy.stats import skewnorm
import datetime
from faker import Faker as FakerLib
import rstr


from typing import Callable
from src.entity_common import IEntity, Dist

# ---------------------------------------------------------------------------
# Annotations
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class CreationTime:
    """Marks the single datetime field that records when an entity was created.
    
    Args:
        start: Earliest possible creation datetime (inclusive).
        end:   Latest possible creation datetime (inclusive).
               Defaults to now if omitted.
    """
    start: datetime.datetime = datetime.datetime(2000, 1, 1)
    end: datetime.datetime = field(default_factory=datetime.datetime.now)


@dataclass(frozen=True)
class PrimaryKey:
    pass


@dataclass(frozen=True)
class Unique:
    tries:int = 3


@dataclass(frozen=True)
class ForeignKey:
    entity: type[IEntity]
    

# ! indicates that a field depends on a foreign field.
@dataclass(frozen=True)
class Dependence:
    dependences: dict[type[IEntity], list[str]] = field(default_factory=dict)



@dataclass(frozen=True)
class Faker:
    method: str
    locale: str = "en_US"
    
    def generate(self, N:int):
        faker = FakerLib(locale=self.locale)
        generator = getattr(faker, self.method)
        return [ generator() for _ in range(N) ]



@dataclass(frozen=True)
class Pattern:
    regex: str
    
    def generate(self, N:int) -> Callable:
        return [ rstr.xeger(self.regex) for _ in range(N) ]


@dataclass(frozen=True)
class CustomGenerator:
    func: Callable
    options: dict = field(default_factory=dict)


# ------------------
# Numerical annotation
# ------------------

@dataclass(frozen=True)
class NormalDist(Dist):
    mean: float = 0.0
    std: float = 1.0
    skewness: float = 0.0
    
    def generate(self, N: int) -> pd.Series:
        rng = np.random.default_rng(self.seed)
        res = pd.Series(skewnorm.rvs(a=self.skewness, loc=self.mean, scale=self.std, size=N, random_state=rng))
        print(f"min: {self.min}")
        print(f"max: {self.max}")
        return self.clip(self.apply_rounding(res))


@dataclass(frozen=True)
class UniformDist(Dist):
    
    def generate(self, N: int) -> pd.Series:
        rng = np.random.default_rng(self.seed)
        res = pd.Series(rng.uniform(low=self.min or 0, high=self.max or 1, size=N))
        return self.clip(self.apply_rounding(res))


@dataclass(frozen=True)
class GammaDist(Dist):
    skewness: float = 1.0
    scale: float = 1.0
    
    def generate(self, N: int) -> pd.Series:
        rng = np.random.default_rng(self.seed)
        shape = (2 / self.skewness) ** 2
        res = pd.Series(rng.gamma(shape=shape, scale=self.scale, size=N))
        return self.clip(self.apply_rounding(res))


@dataclass(frozen=True)
class PoissonDist(Dist):
    mean: float = 1.0
    
    def generate(self, N: int) -> pd.Series:
        rng = np.random.default_rng(self.seed)
        res = pd.Series(rng.poisson(lam=self.mean, size=N))
        return self.clip(self.apply_rounding(res))


@dataclass(frozen=True)
class ExponentialDist(Dist):
    scale: float = 1.0

    def generate(self, N: int) -> pd.Series:
        rng = np.random.default_rng(self.seed)
        res = pd.Series(rng.exponential(scale=self.scale, size=N))
        return self.clip(self.apply_rounding(res))