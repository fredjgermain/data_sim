import pandas as pd

from data_simulator.annotations.primaries import (
  CreationTime, ForeignKey, PrimaryKey, 
  PkCtx, FkCtx, CtCtx)
from data_simulator.annotations.generator import GenCtx
from data_simulator.entity import Entity
from data_simulator.context import EntityContext



class FactoryCtx:
  
  @classmethod
  def make_pkctx(cls, current_ctx:EntityContext) -> PkCtx: 
    pk_values = current_ctx.get_serie(PrimaryKey, generated=False) 
    return PkCtx(pk_values.name, current_ctx.N, current_ctx.entity, pk_values) 


  @classmethod
  def make_fkctx(cls, 
    name:str, 
    current_ctx:EntityContext, 
    entities:dict[type[Entity], EntityContext], 
  ) -> FkCtx: 
    
    entity = current_ctx.entity 
    target = entity.get(name).get(ForeignKey).target 
    fk_values = entities[target].get_serie(PrimaryKey) 
    
    if fk_values is None or fk_values.empty: 
      raise ValueError(f"ForeignKey target '{target.__name__}' has no data to sample from.") 
    
    return FkCtx(name, current_ctx.N, entity, fk_values) 


  @classmethod
  def make_ctctx(cls, 
    name:str, 
    ctx:EntityContext, 
    entities:dict[type[Entity], EntityContext], 
  ) -> CtCtx: 
    
    agg_creation_time = aggregate_creation_time(ctx, entities) 
    return CtCtx(name, ctx.N, ctx.entity, agg_creation_time) 
  
  
  @classmethod
  def make_genctx(cls, 
    name:str, 
    ctx:EntityContext, 
    entities:dict[type[Entity], EntityContext], 
  ) -> GenCtx: 
    current_data = ctx.get_data(preexisting=False) 
    foreign_datas = { e:c.get_data() for e, c in entities.items() } 
    return GenCtx(name, ctx.N, ctx.entity, current_data, foreign_datas) 
  
  


def from_foreign(
  entity:type[Entity],
  fk:pd.Series,
  foreign_fields:list[str|type],
  foreign_datas:dict[type[Entity], pd.DataFrame]
) -> pd.DataFrame:

  fk_name = str(fk.name) # ! helps intellisense 
  target = entity.get(fk_name).get(ForeignKey).target
  target_pk = target.get(PrimaryKey)
  target_names = [ f.name for f in target.get(foreign_fields) ]
  fdata = foreign_datas[target][[target_pk.name, *target_names]]
  
  if fdata is None or fdata.empty: 
    raise ValueError(f"ForeignKey target '{target.__name__}' has no data to sample from.") 

  merged = pd.merge(fk, fdata, left_on=fk_name, right_on=target_pk.name, how='left')
  return merged[[fk_name, *target_names]]


def aggregate_creation_time( ctx:EntityContext, entities:dict[type[Entity], EntityContext] ) -> pd.Series:
  current_data = ctx.get_data(preexisting=False)
  foreign_datas = { e:d.get_data() for e,d in entities.items() }
  args = {'entity':ctx.entity, 'foreign_fields': [CreationTime], 'foreign_datas': foreign_datas }

  # ! Get each foreignkey field not missing in current_data. 
  # ! if fld exist but cannot be found in current_data it might need to raise an exception. 
  flds = [fld for fld in ctx.entity.get([ForeignKey]) if fld.name in list(current_data.columns)] 
  dfs:list[pd.DataFrame] = []
  for fld in flds:
    df = from_foreign(fk=current_data[fld.name], **args).reset_index(drop=True)
    if df.empty:
      continue
    dfs.append(df.drop(columns=fld.name))
  if not dfs:
    return pd.Series()
  return pd.concat(dfs, axis=1).max(axis=1).rename('agg_creation_time')