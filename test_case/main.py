import numpy as np
import pandas as pd
from dataclasses import dataclass, fields, field

import typing
from typing import Annotated

from src.data_simulator import extract_metadata


def inspect_entity(entity: type) -> dict[str, EntityField]:
    result = {}
    hints = typing.get_type_hints(entity, include_extras=True)

    for f in fields(entity):
        hint = hints[f.name]

        if typing.get_origin(hint) is typing.Annotated:
            base_type, *annotations = typing.get_args(hint)
            func = next((a for a in annotations if callable(a)), None)
            meta = [a for a in annotations if not callable(a)]
            result[f.name] = EntityField(name=f.name, base_type=base_type, func=func, annotations=meta)
        else:
            result[f.name] = EntityField(name=f.name, base_type=hint)

    return result



@dataclass
class SimContext: 
  entities: dict[type, EntityContext] = field(default_factory=dict) 


@dataclass
class EntityContext: 
  entity:type 
  preexisting: pd.DataFrame = field(default_factory=pd.DataFrame) 
  generated: pd.DataFrame = field(default_factory=pd.DataFrame) 
  N:int = 0 
  done:bool = False 
  
  def get_primarykey(self) -> EntityField | None:
    entity_fields = inspect_entity(self.entity).values()
    return next((f for f in entity_fields if f.has(PrimaryKey)), None)
  

@dataclass(frozen=True)
class PrimaryKey: 
  pass 

@dataclass(frozen=True) 
class ForeignKey[E]: 
  name:str
  entity:type[E] 
  
# ! indicates that a field depends on a foreign field. 
@dataclass(frozen=True) 
class ForeignField[E]: 
  name:str
  entity:type[E] 


@dataclass
class EntityField[T]: 
  name:str 
  base_type:type[T] 
  func:callable | None = None 
  annotations: list = field(default_factory=list) 
  
  def has[A](self, t:type[A]): 
    return any([isinstance(a, t) for a in self.annotations]) 
  
  def get_foreign(self) -> ForeignKey | ForeignField | None:
    return next((a for a in self.annotations if isinstance(a, (ForeignKey, ForeignField))), None)
  



@dataclass
class Simulator: 
  sim_ctx:SimContext 

  def simulate_all_entities(self): 
    for ent_ctx in self.sim_ctx.entities.values(): 
      self.simulate_entity(ent_ctx) 
  
  
  def __generation_method__(self, f:EntityField): 
    if f.has(ForeignKey): 
      pass # ! pick from foreign 
  
  
  def get_primarykey[T](self, entity:type[T]) -> EntityField:
    entity_fields = inspect_entity(entity).values()
    return next((f for f in entity_fields if f.has(PrimaryKey)), None)
  
  def get_foreign_df[T](self, entity:type[T]) -> pd.DataFrame: 
    foreign_ctx = self.sim_ctx.entities[entity] 
    return pd.concat([foreign_ctx.preexisting, foreign_ctx.generated]) 
  
  def get_foreignkeys[T](self, entity:type[T]) -> pd.Series: 
    foreign_ctx = self.sim_ctx.entities[entity] 
    fkey = foreign_ctx.get_primarykey()
    df = pd.concat([foreign_ctx.preexisting, foreign_ctx.generated])
    if fkey:
      return df[fkey.name] 
    return pd.Series([])
    
    
  
  # ? Assumes foreign Entities have already been defined ? Should not have circular dependencies 
  def simulate_entity(self, ent_ctx:EntityContext): 
    if ent_ctx.done:
      return
    
    ent = ent_ctx.entity 
    
    ent_fields = inspect_entity(ent) 
    for f in ent_fields.values(): 
      if f.has(ForeignKey): 
        # ! collect require foreign field here and pass 
        generate_foreignkey(self.sim_ctx, ent_ctx, f) 
        # ! raise exception if foreign entity has not been generated or is empty. 
        # ent_ctx.generated[f.name] choose from foreign table. 
        continue 
      
      if f.func != None: 
        #foreign_ent = f.get_foreign_entity() 
        ent_ctx.generated[f.name] = f.func(self.sim_ctx, ent_ctx, f) 
        ent_ctx.done = True 
        print(ent_ctx.generated) 

  # def simulate_foreignvalues(self, ent_ctx:EntityContext): 
  #   ent_fields = inspect_entity(ent_ctx.entity) 
  #   foreign_fields = [ v for v in ent_fields.values() if v.has(ForeignKey) or v.has(ForeignField)] 
    
  #   for f in foreign_fields: 
  #     if f.func != None: 
  #       ent_ctx.generated[f.name] = f.func(self.sim_ctx, ent_ctx) 
  #       print(ent_ctx.generated) 


def generate_foreignkey(sim_ctx:SimContext, ent_ctx:EntityContext, ent_field:EntityField) -> pd.Series: 
  foreign = ent_field.get_foreign() 
  foreign_ctx = sim_ctx.entities[foreign.entity] 
  foreignkey_field = foreign_ctx.get_primarykey() # ! find primary key from foreign dataset. 
  foreignkey_field.name 
  print(foreignkey_field.name) 
  
  

# ! template
def generate_foreignvalue(sim_ctx:SimContext, ent_ctx:EntityContext, ent_field:EntityField) -> pd.Series: 
  foreign = ent_field.get_foreign() 
  foreign_ctx = sim_ctx.entities[foreign.entity] 
  
  df_foreign = pd.concat([foreign_ctx.preexisting, foreign_ctx.generated]) 
  foreign_keys = df_foreign[foreign.name] 
  
  return np.random.choice(foreign_keys) # ! add conditional here 



def generate_sequential(sim_ctx:SimContext, ent_ctx:EntityContext, ent_field:EntityField) -> pd.Series: 
  a = ent_ctx.preexisting.shape[0] + 1 
  b = a + ent_ctx.N 
  return pd.Series(range(a, b)) 

@dataclass
class Region:
  region_id: Annotated[int, PrimaryKey()] 

@dataclass
class Customer: 
    customer_id: Annotated[int, generate_sequential, PrimaryKey()] 
    region_id: Annotated[int, ForeignKey('region_id', Region)] 

    # email: Annotated[str, Unique(), Faker("email")] 
    # first_name: Annotated[str, Faker("first_name")] 
    # age: Annotated[int, Distribution("normal", mean=38, std=12, min=18, max=90)]
    # region: Annotated[str, Categorical(["North", "South", "East", "West"])] 
    # created_at: Annotated[datetime, Temporal(before="updated_at")] # temporal integrity 
    # updated_at: Annotated[datetime, Temporal(after="created_at")]

df_region = pd.DataFrame( range(50), columns=['region_id'])


sim = Simulator(SimContext({ 
  Region:EntityContext(Region, preexisting=df_region, done=True), 
  Customer:EntityContext(Customer, N=100) 
  })) 
sim.simulate_all_entities()







#sim_ctx = SimContext([]) 

# preexisting = pd.DataFrame(range(50), columns=['customer_id'])
# ent_ctx = EntityContext(preexisting=preexisting) 


# field, gen_func, *annotations = ['customer_id', generate_sequential, PrimaryKey(), Unique()] 

# # if annotations does not include Foreign key, generate foreign key at the end 
# ent_ctx.generated[field] = gen_func(sim_ctx, ent_ctx) 

# print(ent_ctx.generated) 


