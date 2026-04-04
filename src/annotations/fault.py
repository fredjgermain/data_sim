import pandas as pd
from dataclasses import dataclass
from typing import Callable, Any, Literal

from src.annotations.base import FaultCtx, IFault 
from src.utils import fault



@dataclass
class Corrupt(IFault): 
    func:Callable[[pd.Series, Any,  float], pd.Series] 
    prob: float = 0 
    seed: int | None = None 
    
    def inject(self, ctx: FaultCtx) -> pd.Series:
        return self.func(ctx.current_serie, self.seed, self.prob)


@dataclass
class Nullify(IFault):
    prob: float = 0
    seed: int | None = None

    def inject(self, ctx: FaultCtx) -> pd.Series:
        return fault.inject_missings(ctx.current_serie, self.seed, self.prob) 


@dataclass
class Misspell(IFault):
    prob:0.02
    seed: int | None = None
    
    def inject(self, ctx: FaultCtx) -> pd.Series: 
        return fault.inject_misspellings(ctx.current_serie, self.seed, self.prob) 

@dataclass
class MissingWord(IFault):
    prob:0.02
    seed: int | None = None
    
    def inject(self, ctx:FaultCtx) -> pd.Series: 
        return fault.inject_missings_words(ctx.current_serie, self.seed, self.prob) 


@dataclass
class Duplicate(IFault):
    prob: float = 0.05
    seed: int | None = None

    def inject(self, ctx: FaultCtx) -> pd.Series:
        return fault.inject_duplicates(ctx.current_serie, self.seed, self.prob) 

@dataclass
class Sentinel(IFault):
    sentinels: list
    prob: float = 0.05
    seed: int | None = None

    def inject(self, ctx:FaultCtx) -> pd.Series:
        return fault.inject_sentinel(ctx.current_serie, self.seed, self.sentinels, self.prob)

@dataclass
class Outlier(IFault):
    prob: float = 0.03
    seed: int | None = None
    magnitude: float = 3.0,
    direction: Literal['both', 'up', 'down'] = 'both'
    
    def inject(self, ctx:FaultCtx) -> pd.Series:
        return fault.inject_outliers(
            ctx.current_serie, 
            self.seed, 
            self.prob, 
            self.magnitude, 
            self.direction)

