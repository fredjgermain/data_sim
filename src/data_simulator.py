
import pandas as pd
from dataclasses import dataclass, fields
import typing 
from typing import Annotated


#from src.entity import SimEntity
from utils.metaframe import MetaFrame



# @dataclass
# class Transaction:
#     transaction_id: Annotated[int, PrimaryKey(strategy="sequential")] 
#     customer_id: Annotated[int, ForeignKey(Customer.customer_id)]       # ! conditional 
#     amount: Annotated[float, Distribution("skewed", mean=120, std=80, skewness=2.5)]
#     transaction_date: Annotated[datetime, Temporal()] # temporal integrity 



@dataclass
class SimEntity:
  pass



@dataclass
class DataSimContext: 
  preexisting: dict[type[SimEntity], MetaFrame] 
  generated_data = dict[type[SimEntity], MetaFrame] 
  

class EntityContext:
  df:pd.DataFrame



class Generator: 
  func:callable 
  
  def generate(sim_ctx:DataSimContext, ent_ctx:EntityContext): 
    
    field, func, *annotation = ['field_name', lambda x: x, 'Unique'] # get field name and annotation 
    ent_ctx.df[field] 

  


def extract_metadata(hint: type) -> tuple[type, list]:
    """
    Given Annotated[int, PrimaryKey(), Unique()],
    return (int, [PrimaryKey(), Unique()]).
    Given a plain type like int, return (int, []).
    """
    if typing.get_origin(hint) is Annotated:
        base_type = hint.__args__[0]        # the actual type
        metadata  = list(hint.__metadata__) # the annotation markers
        return base_type, metadata
    return hint, []
  
