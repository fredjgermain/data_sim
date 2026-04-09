# ---------------------------------------------------------------------------
# PrimaryKey
# ---------------------------------------------------------------------------

import pandas as pd
import numpy as np
import datetime
from dataclasses import dataclass, field
from typing import Callable

from data_simulator.interface import IEntity, IAnnotation, IEntityContext 
from data_simulator.utils import generator 



# PrimaryKey ===========================================================
@dataclass 
class PkCtx:
    name:str 
    N:int 
    entity:type[IEntity] 
    pk_values:pd.Series = field(default_factory=pd.Series) 

@dataclass
class PrimaryKey(IAnnotation):
    fn: Callable[[pd.DataFrame], pd.Series] | None = None
    seed: int | None = None

    def generate(self, ctx:PkCtx) -> pd.Series:
        if self.fn:
            return self.fn(ctx)
        return self._generate_sequential(ctx) # Default primarykey generator. 


    # default generation method 
    def _generate_sequential(self, ctx:PkCtx) -> pd.Series:
        if ctx.pk_values.empty:
            start = 1
        else:
            start = int(ctx.pk_values.max()) + 1
        return pd.Series(range(start, start + ctx.N), dtype='int64')



# ForeignKey ==============================================================
@dataclass
class FkCtx: 
  name:str 
  N:int 
  entity:type[IEntity] 
  fk_values:pd.Series = field(default_factory=pd.Series) 
  

@dataclass
class ForeignKey(IAnnotation):
    target: type[IEntity]
    seed: int | None = None

    def generate(self, ctx: FkCtx) -> pd.Series: 
      if ctx.fk_values is None or ctx.fk_values.empty: 
        raise ValueError(
          f"ForeignKey target '{self.target.__name__}' has no data to sample from."
        )
      rng = np.random.default_rng(self.seed)
      return pd.Series(rng.choice(ctx.fk_values, size=ctx.N)).reset_index(drop=True) 



# CreationTime ==============================================================
@dataclass
class CtCtx: 
  name:str 
  N:int 
  entity:type[IEntity] 
  agg_creation_time:pd.Series = field(default_factory=pd.Series) 


@dataclass
class CreationTime(IAnnotation):
    start: datetime.datetime
    end: datetime.datetime
    seed: int | None = None

    def generate(self, ctx: CtCtx) -> pd.Series:
        if ctx.agg_creation_time.empty:
            start = pd.Series([self.start] * ctx.N)
        else:
            start = ctx.agg_creation_time.clip(lower=self.start)
        
        end = pd.Series([self.end] * ctx.N)
        return generator.generate_date(self.seed, start, end)







