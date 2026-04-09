import pandas as pd

from data_simulator.annotations.primaries import (
  CreationTime, ForeignKey, PrimaryKey, 
  PkCtx, FkCtx, CtCtx)
from data_simulator.annotations.generator import GenCtx
from data_simulator.annotations.fault import FaultCtx
from data_simulator.annotations.validation import ValidCtx
from data_simulator.interface import IEntity, IEntityContext



class FactoryCtx:
  
  @classmethod
  def make_pkctx(cls, current_ctx:IEntityContext) -> PkCtx: 
    pk_values = current_ctx.get_serie(PrimaryKey, generated=False) 
    return PkCtx(pk_values.name, current_ctx.N, current_ctx.entity, pk_values) 


  @classmethod
  def make_fkctx(cls, 
    name:str, 
    current_ctx:IEntityContext, 
    entities:dict[type[IEntity], IEntityContext]
  ) -> FkCtx: 
    
    entity = current_ctx.entity 
    target = entity.get(name).get(ForeignKey).target 
    fk_values = entities[target].get_serie(PrimaryKey) 
    return FkCtx(name, current_ctx.N, entity, fk_values) 


  @classmethod
  def make_ctctx(cls, 
    name:str, 
    ctx:IEntityContext, 
    entities:dict[type[IEntity], IEntityContext]
  ) -> CtCtx: 
    
    agg_creation_time = aggregate_creation_time(ctx, entities) 
    return CtCtx(name, ctx.N, ctx.entity, agg_creation_time) 
  
  
  @classmethod
  def make_genctx(cls, 
    name:str, 
    ctx:IEntityContext, 
    entities:dict[type[IEntity], IEntityContext]
  ) -> GenCtx: 
    current_data = ctx.get_data(preexisting=False) 
    foreign_data = { e:c.get_data() for e, c in entities.items() } 
    return GenCtx(name=name, N=ctx.N, entity=ctx.entity, current_data=current_data, foreign_datas=foreign_data) 
  
  
  @classmethod
  def make_faultctx(cls, name:str, ctx:IEntityContext) -> FaultCtx:
    current_serie = ctx.get_serie(name, preexisting=False) 
    return FaultCtx(name, current_serie) 
  
  
  @classmethod
  def make_validctx(cls, name, ctx:IEntityContext) -> ValidCtx: 
    current_serie = ctx.get_serie(name, preexisting=False) 
    return ValidCtx(name, current_serie) 


def from_foreign(
  entity:type[IEntity],
  fk:pd.Series,
  foreign_fields:list[str|type],
  foreign_datas:dict[type[IEntity], pd.DataFrame]
) -> pd.DataFrame:

  fk_name = str(fk.name) # ! helps intellisense 
  target = entity.get(fk_name).get(ForeignKey).target
  target_pk = target.get(PrimaryKey)
  target_names = [ f.name for f in target.get(foreign_fields) ]
  fdata = foreign_datas[target][[target_pk.name, *target_names]]

  merged = pd.merge(fk, fdata, left_on=fk_name, right_on=target_pk.name, how='left')
  return merged[[fk_name, *target_names]]


def aggregate_creation_time( ctx:IEntityContext, entities:dict[type[IEntity], IEntityContext] ) -> pd.Series:
  current_data = ctx.get_data(preexisting=False)
  foreign_datas = { e:d.get_data() for e,d in entities.items() }
  args = {'entity':ctx.entity, 'foreign_fields': [CreationTime], 'foreign_datas': foreign_datas }

  dfs:list[pd.DataFrame] = []
  for fld in ctx.entity.get([ForeignKey]):
    df = from_foreign(fk=current_data[fld.name], **args).reset_index(drop=True)
    if df.empty:
      continue
    dfs.append(df.drop(columns=fld.name))
  if not dfs:
    return pd.Series()
  return pd.concat(dfs, axis=1).max(axis=1).rename('agg_creation_time')