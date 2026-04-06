
import pandas as pd 
from src.annotations.primaries import ForeignKey, PrimaryKey, CreationTime

from src.interface import IEntity, IEntityContext
#from src.context import EntityContext



def from_foreign( 
  entity:type[IEntity], 
  fk:pd.DataFrame, 
  foreign_fields:list[str|type], 
  foreign_datas:dict[type[IEntity], pd.DataFrame] 
) -> pd.DataFrame: 
  
  fk_name = list(fk.columns)[0] 
  target = entity.get(fk_name).get(ForeignKey).target 
  target_pk = target.get(PrimaryKey) 
  target_names = [ f.name for f in target.get(foreign_fields) ] 
  fdata = foreign_datas[target][[target_pk.name, *target_names]] 
  merged = pd.merge(fk, fdata, left_on=fk_name, right_on=target_pk.name, how='left') 
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
  # ! no need to rename 
  return pd.concat(dfs, axis=1).max(axis=1).rename('agg_creation_time') 



def aggregate_from_foreign_fields( 
  entity:type[IEntity], 
  current_data:pd.DataFrame, 
  foreign_key_fields:dict[str, list[str|type]], 
  foreign_datas:dict[type[IEntity], pd.DataFrame] 
) -> dict[str, pd.DataFrame]: 
  
  result = {} 
  for fk, ffields in foreign_key_fields.items(): 
    result[fk] = from_foreign(entity, current_data[[fk]], ffields, foreign_datas) 
  return result
