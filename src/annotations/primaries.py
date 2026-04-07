# ---------------------------------------------------------------------------
# PrimaryKey
# ---------------------------------------------------------------------------

import pandas as pd
import datetime
from dataclasses import dataclass, field
from typing import Callable

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
        pk_values = current_ctx.get_serie(PrimaryKey, generated=False) 
        #pk_fld = current_ctx.entity.get(PrimaryKey) 
        #name = pk_fld.name 
        #pk_values = current_ctx.get_serie(name, generated=False) # ! can cause error if no precedent values are set. 
        return PkCtx(pk_values.name, current_ctx.N, current_ctx.entity, pk_values) 



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
            start = int(ctx.pk_values.max()) + 1
        return pd.Series(range(start, start + ctx.N), dtype='int64')


# ---------------------------------------------------------------------------
# ForeignKey
# ---------------------------------------------------------------------------


@dataclass
class FkCtx: 
  name:str 
  N:int 
  entity:type[IEntity] 
  fk_values:pd.Series = field(default_factory=pd.Series) 
  

  @classmethod
  def make_ctx(cls, name:str, current_ctx:IEntityContext, entities:dict[type[IEntity], IEntityContext]): 
      entity = current_ctx.entity 
      target = entity.get(name).get(ForeignKey).target 
      fk_values = entities[target].get_serie(PrimaryKey) 
      return FkCtx(name, current_ctx.N, entity, fk_values) 
  

@dataclass
class ForeignKey(IAnnotation):
    target: type[IEntity]

    def generate(self, ctx: FkCtx) -> pd.Series: 
        if ctx.fk_values is None or ctx.fk_values.empty: 
            raise ValueError( 
                f"ForeignKey target '{self.target.__name__}' has no data to sample from." 
            ) 
        return ctx.fk_values.sample(n=ctx.N, replace=True).reset_index(drop=True) 


# ---------------------------------------------------------------------------
# CreationTime
# ---------------------------------------------------------------------------

@dataclass
class CtCtx: 
  name:str 
  N:int 
  entity:type[IEntity] 
  agg_creation_time:pd.Series = field(default_factory=pd.Series) 
  

#   @classmethod
#   def make_ctx(cls, name:str, ctx:IEntityContext, entities:dict[type[IEntity], IEntityContext]): 
#     return CtCtx(name, ctx.N, ctx.entity, agg_creation_time) 
  

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



def from_foreign(
  entity:type[IEntity],
  fk:pd.Series,
  foreign_fields:list[str|type],
  foreign_datas:dict[type[IEntity], pd.DataFrame]
) -> pd.DataFrame:

  fk_name = str(fk.name)
  target = entity.get(fk_name).get(ForeignKey).target
  target_pk = target.get(PrimaryKey)
  target_names = [ f.name for f in target.get(foreign_fields) ]
  fdata = foreign_datas[target][[target_pk.name, *target_names]]

  merged = pd.merge(pd.DataFrame(fk), fdata, left_on=fk_name, right_on=target_pk.name, how='left')
  return merged[[fk_name, *target_names]]



def aggregate_creation_time(
  entity:type[IEntity],
  current_data:pd.DataFrame,
  foreign_datas:dict[type[IEntity], pd.DataFrame]
) -> pd.Series:

  args = {'entity':entity, 'foreign_fields': [CreationTime], 'foreign_datas': foreign_datas }

  dfs:list[pd.DataFrame] = []
  for fld in entity.get([ForeignKey]):

    df = from_foreign(fk=current_data[fld.name], **args).reset_index(drop=True)
    if df.empty:
      continue
    dfs.append(df.drop(columns=fld.name))
  if not dfs:
    return pd.Series()
  # ! no need to rename 
  return pd.concat(dfs, axis=1).max(axis=1).rename('agg_creation_time')
        # ranges = (end_date - start_date).dt.days.clip(lower=0)
        # random_days = (np.random.rand(len(start_date)) * (ranges + 1)).astype(int)
        # return start_date + pd.to_timedelta(random_days, unit='D')

