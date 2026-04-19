import pandas as pd
from dataclasses import dataclass, field
from typing import Callable, Any, Literal

from _common.interface import IAnnotation 
import fault_injection.utils as fault 


@dataclass
class FaultCtx:
    name:str
    data: pd.DataFrame = field(default_factory=pd.DataFrame) 


class IFault(IAnnotation):
    seed: int | None = None 

    def inject(self, ctx:FaultCtx) -> pd.Series:
      raise NotImplementedError



@dataclass
class Corrupt(IFault): 
    func:Callable[[pd.Series, Any,  float], pd.Series] 
    prob: float = 0 
    
    def inject(self, ctx: FaultCtx) -> pd.Series:
      return self.func(ctx.data, self.seed, self.prob)


@dataclass
class Missing(IFault):
    prob: float = 0

    def inject(self, ctx: FaultCtx) -> pd.Series:
      return fault.inject_missings(ctx.data, self.seed, self.prob) 


@dataclass
class Misspell(IFault):
    prob: float = 0.02
    
    def inject(self, ctx: FaultCtx) -> pd.Series: 
      return fault.inject_misspellings(ctx.data, self.seed, self.prob) 

@dataclass
class MissingWord(IFault):
    prob: float = 0.02
    
    def inject(self, ctx:FaultCtx) -> pd.Series: 
      return fault.inject_missings_words(ctx.data, self.seed, self.prob) 


@dataclass
class Duplicate(IFault):
    prob: float = 0.05

    def inject(self, ctx: FaultCtx) -> pd.Series:
        return fault.inject_duplicates(ctx.data, self.seed, self.prob) 

@dataclass
class Sentinel(IFault):
    sentinels: list
    
    prob: float = 0.05

    def inject(self, ctx:FaultCtx) -> pd.Series:
        return fault.inject_sentinel(ctx.data, self.seed, self.sentinels, self.prob)

@dataclass
class Outlier(IFault):
    prob: float = 0.03
    magnitude: float = 3.0
    direction: Literal['both', 'up', 'down'] = 'both'
    
    def inject(self, ctx:FaultCtx) -> pd.Series:
        return fault.inject_outliers(
            ctx.data, 
            self.seed, 
            self.prob, 
            self.magnitude, 
            self.direction)

