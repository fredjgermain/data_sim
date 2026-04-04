# ---------------------------------------------------------------------------
# PrimaryKey
# ---------------------------------------------------------------------------

import pandas as pd
import numpy as np
import datetime
from dataclasses import dataclass, field
from typing import Callable


#from src.annotations.standardgen import IGen
#from src.annotations.standardgen import GenCtx
from src.interface import IEntity, IAnnotation


@dataclass 
class PkCtx:
    name:str 
    N:int 
    entity:type[IEntity] 
    current_data:pd.DataFrame = field(default_factory=pd.DataFrame) 
    


@dataclass
class PrimaryKey(IAnnotation):
    fn: Callable[[pd.DataFrame], pd.Series] | None = None

    def generate(self, ctx:PkCtx) -> pd.Series:
        if self.fn:
            return self.fn(ctx)
        return self._generate_sequential(ctx) # Default primarykey generator. 


    # default generation method 
    def _generate_sequential(self, ctx:PkCtx) -> pd.Series:
        if ctx.current_data.empty:
            start = 1
        else:
            pk_col = ctx.current_data[ctx.name]
            start = int(pk_col.max()) + 1
        return pd.Series(range(start, start + ctx.N), dtype='int64')
    

# ---------------------------------------------------------------------------
# ForeignKey
# ---------------------------------------------------------------------------


@dataclass
class FkCtx: 
  name:str 
  N:int 
  entity:type[IEntity] 
  foreign_datas:dict[type[IEntity], pd.DataFrame] = field(default_factory=dict) 
  

@dataclass
class ForeignKey(IAnnotation):
    target: type[IEntity]

    def generate(self, ctx: FkCtx) -> pd.Series: 
        target_data = ctx.foreign_datas.get(self.target) 
        target_pk_fld = self.target.get_primary_key_field() 
        if target_data is None or target_data.empty or target_pk_fld is None: 
            raise ValueError( 
                f"ForeignKey target '{self.target.__name__}' has no data to sample from." 
            ) 
        target_pk_values = target_data[target_pk_fld.name] 
        return target_pk_values.sample(n=ctx.N, replace=True).reset_index(drop=True) 


# ---------------------------------------------------------------------------
# CreationTime
# ---------------------------------------------------------------------------

@dataclass
class CtCtx: 
  name:str 
  N:int 
  entity:type[IEntity] 
  current_data:pd.DataFrame = field(default_factory=pd.DataFrame) 
  

@dataclass
class CreationTime(IAnnotation):
    start: datetime.datetime
    """Earliest possible timestamp (inclusive)."""

    end: datetime.datetime
    """Latest possible timestamp (inclusive)."""


    def generate(self, ctx: CtCtx) -> pd.Series: 
        df_start = ctx.current_data.copy() 
        df_start['start'] = [self.start] * ctx.N # ! careful with dimensions. 
        start_date = df_start.max(axis=1) 
        end_date = self.end 
        
        ranges = (end_date - start_date).dt.days.clip(lower=0)
        random_days = (np.random.rand(len(start_date)) * (ranges + 1)).astype(int)
        return start_date + pd.to_timedelta(random_days, unit='D')

