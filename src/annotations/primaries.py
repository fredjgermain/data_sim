# ---------------------------------------------------------------------------
# PrimaryKey
# ---------------------------------------------------------------------------

import pandas as pd
import datetime
from dataclasses import dataclass, field
from typing import Callable


#from src.annotations.standardgen import IGen
#from src.annotations.standardgen import GenCtx
from src.interface import IEntity, IAnnotation, IEntityContext 
from src.utils import generator 






@dataclass 
class PkCtx:
    name:str 
    N:int 
    entity:type[IEntity] 
    pk_values:pd.Series = field(default_factory=pd.Series) 
    
    @classmethod
    def make_ctx(cls, current_ctx:IEntityContext) -> PkCtx: 
        pk_fld = current_ctx.entity.get(PrimaryKey) 
        name = pk_fld.name 
        print(name)
        pk_values = current_ctx.get_serie(name, generated=False) # ! can cause error if no precedent values are set. 
        return PkCtx(name, current_ctx.N, current_ctx.entity, pk_values) 



@dataclass
class PrimaryKey(IAnnotation):
    fn: Callable[[pd.DataFrame], pd.Series] | None = None

    def generate(self, ctx:PkCtx) -> pd.Series:
        if self.fn:
            return self.fn(ctx)
        return self._generate_sequential(ctx) # Default primarykey generator. 


    # default generation method 
    def _generate_sequential(self, ctx:PkCtx) -> pd.Series:
        if ctx.pk_values.empty:
            start = 1
        else:
            pk_col = ctx.pk_values[ctx.name]
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
    
    @classmethod
    def make_ctx(name:str, current_ctx:IEntityContext, entities:dict[type[IEntity], IEntityContext]): 
        fdatas = {} 
        for fld in current_ctx.entity.get([ForeignKey]): 
            target = fld.get(ForeignKey).target 
            pk = target.get(PrimaryKey) 
            fdatas[target] = entities[target][pk.name] 

        return FkCtx(name, current_ctx.N, current_ctx.entity, fdatas) 


    def generate(self, ctx: FkCtx) -> pd.Series: 
        target_data = ctx.foreign_datas.get(self.target) 
        target_pk_fld = self.target.get(PrimaryKey) 
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
  agg_creation_time:pd.Series = field(default_factory=pd.Series) 
  

  #@classmethod
  #def make_ctx(cls, name:str, ctx:IEntityContext, entities:dict[type[IEntity], IEntityContext]): 
    #return CtCtx(name, ctx.N, ctx.entity, agg_creation_time) 
  

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
        # ranges = (end_date - start_date).dt.days.clip(lower=0)
        # random_days = (np.random.rand(len(start_date)) * (ranges + 1)).astype(int)
        # return start_date + pd.to_timedelta(random_days, unit='D')

