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

    def inject(self, ctx:FaultCtx) -> pd.Series:
      raise NotImplementedError


@dataclass
class CustomFault(IFault): 
    func:Callable[[FaultCtx, Any, float], pd.Series] 
    prob: float = 0.05 
    seed: int | None = None 
    
    def inject(self, ctx: FaultCtx) -> pd.Series: 
      return self.func(ctx, self.seed, self.prob) 


@dataclass
class Missing(IFault):
    prob: float = 0.05 
    seed: int | None = None 

    def inject(self, ctx: FaultCtx) -> pd.Series:
      return fault.inject_missings(ctx.data[ctx.name], self.seed, self.prob) 


@dataclass
class Misspell(IFault):
    prob: float = 0.05
    seed: int | None = None 
    
    def inject(self, ctx: FaultCtx) -> pd.Series: 
      return fault.inject_misspellings(ctx.data[ctx.name], self.seed, self.prob) 


@dataclass
class MissingWord(IFault):
    prob: float = 0.05
    seed: int | None = None 
    
    def inject(self, ctx:FaultCtx) -> pd.Series: 
      return fault.inject_missings_words(ctx.data[ctx.name], self.seed, self.prob) 


@dataclass
class Duplicate(IFault):
    prob: float = 0.05
    seed: int | None = None 

    def inject(self, ctx: FaultCtx) -> pd.Series:
        return fault.inject_duplicates(ctx.data[ctx.name], self.seed, self.prob) 


@dataclass
class Insert(IFault):
    insertions: list
    prob: float = 0.05
    seed: int | None = None 

    def inject(self, ctx:FaultCtx) -> pd.Series:
      return fault.inject_insert(ctx.data[ctx.name], self.seed, self.insertions, self.prob)


@dataclass 
class Outlier(IFault): 
    prob: float = 0.05 
    magnitude: float = 3.0 
    direction: Literal['both', 'up', 'down'] = 'both' 
    seed: int | None = None 
    
    def inject(self, ctx:FaultCtx) -> pd.Series: 
      return fault.inject_outliers( 
        ctx.data[ctx.name], 
        self.seed, 
        self.prob, 
        self.magnitude, 
        self.direction) 

