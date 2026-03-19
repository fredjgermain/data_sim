# I think I have a better idea. Instead, 
# I could define an annotation like "PrimaryTime" which would a unique field per entity defining when an 
# observation of that entity has been created. This ought to be accessible in a way similar to getting the PrimaryKey of a foreign entity. 









import numpy as np
import pandas as pd
from dataclasses import dataclass, fields, field
from faker import Faker as FakerLib
import rstr
import datetime 

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


def get_primarytime_field[T](entity:type[T]) -> EntityField | None:
  entity_fields = inspect_entity(entity).values() 
  return next((f for f in entity_fields if f.has(CreationTime)), None) 

def get_primarykey_field[T](entity:type[T]) -> EntityField | None: 
  entity_fields = inspect_entity(entity).values() 
  return next((f for f in entity_fields if f.has(PrimaryKey)), None) 


@dataclass(frozen=True)
class CreationTime:
  start: datetime.datetime = datetime.datetime(2020, 1, 1)
  end: datetime.datetime = field(default_factory=datetime.datetime.now)

@dataclass(frozen=True)
class PrimaryKey: 
  pass 

@dataclass(frozen=True)
class Unique: 
  pass 


@dataclass(frozen=True)
class Faker:
    method: str
    locale: str = "en_US"

@dataclass(frozen=True) 
class ForeignKey[E]: 
  entity:type[E] 
  
# ! indicates that a field depends on a foreign field. 
@dataclass(frozen=True) 
class ForeignFields[E]: 
  columns:list[str] 
  entity:type[E] 

@dataclass(frozen=True)
class Pattern:
  regex: str
    
    

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



@dataclass
class EntityField[T]: 
  name:str 
  base_type:type[T] 
  func:callable | None = None 
  annotations: list = field(default_factory=list) 
  
  def has[A](self, t:type[A]) -> bool: 
    """Returns true if entity field has the specified annotation. """
    return any([isinstance(a, t) for a in self.annotations]) 
  
  def get_annotation[A](self, t:type[A]) -> A | None: 
    return next((a for a in self.annotations if isinstance(a, t)), None) 
  
  def get_foreign(self) -> ForeignKey | ForeignFields | None:
    return next((a for a in self.annotations if isinstance(a, (ForeignKey, ForeignFields))), None)
  
  def get_foreign_columns(self): 
    cols = [] 
    fk = self.get_annotation(ForeignKey) 
    if fk: 
      cols.append(get_primarykey_field(fk.entity).name) 
    ffld = self.get_annotation(ForeignFields) 
    if ffld:
      cols.extend(ffld.columns) 
    return cols 



@dataclass
class Simulator: 
  entities: dict[type, EntityContext] = field(default_factory=dict) 

  def simulate_all_entities(self): 
    for ent_ctx in self.entities.values(): 
      self.simulate_entity(ent_ctx) 

  
  # ? Assumes foreign Entities have already been defined ? Should not have circular dependencies 
  def simulate_entity(self, ent_ctx:EntityContext): 
    if ent_ctx.done:
      return
    
    ent = ent_ctx.entity 
    ent_fields = inspect_entity(ent) 
    for fld in ent_fields.values(): 
      df_foreign = self._get_df_foreign(fld) 
        
      if fld.func is None and fld.has(ForeignKey):
        fld.func = generate_foreign_key
      
      if fld.func is None and fld.has(Faker):
        fld.func = generate_with_faker
      
      if fld.func is None and fld.has(Pattern):
        fld.func = generate_with_pattern
      
      if fld.func: 
        ent_ctx.generated[fld.name] = fld.func(ent_ctx, fld, df_foreign) # ! entity_context, field, foreign values 
      
    ent_ctx.done = True
    result = pd.concat([ent_ctx.preexisting, ent_ctx.generated]).reset_index(drop=True) 
    print(result) 
  
  
  def _get_df_foreign(self, fld:EntityField):
    if fld.has(ForeignKey) or fld.has(ForeignFields): 
      # ! collect required foreign information here and pass the foreign columns later 
      # ! raise exception if foreign entity is not done or is empty. 
      for_ent = fld.get_foreign().entity 
      cols = fld.get_foreign_columns() 
      for_ctx = self.entities[for_ent] 
      return pd.concat([for_ctx.preexisting, for_ctx.generated]).reset_index(drop=True)[cols]
    return pd.DataFrame() 
    


# ! Generator function 
def generate_with_pattern(ent_ctx: EntityContext, ent_field: EntityField, df_foreign: pd.DataFrame) -> pd.Series:
  pattern = ent_field.get_annotation(Pattern).regex
  return pd.Series([rstr.xeger(pattern) for _ in range(ent_ctx.N)])


def generate_with_faker(ent_ctx: EntityContext, ent_field: EntityField, df_foreign: pd.DataFrame) -> pd.Series:
  faker_annotation = ent_field.get_annotation(Faker)
  if faker_annotation is None:
    raise ValueError(f"Field '{ent_field.name}' has no Faker annotation.")

  fake = FakerLib(faker_annotation.locale)
  generator = fake.unique if ent_field.has(Unique) else fake

  method = getattr(generator, faker_annotation.method, None)
  if method is None:
      raise ValueError(f"Faker has no method '{faker_annotation.method}'.")

  return pd.Series([method() for _ in range(ent_ctx.N)])
  

def generate_foreign_key(ent_ctx:EntityContext, ent_field:EntityField, df_foreign:pd.DataFrame) -> pd.Series: 
  fk = df_foreign.iloc[:, 0] 
  return np.random.choice(fk, ent_ctx.N) 

def generate_sequential(ent_ctx:EntityContext, ent_field:EntityField, df_foreign:pd.DataFrame) -> pd.Series: 
  a = ent_ctx.preexisting.shape[0] + 1 
  b = a + ent_ctx.N 
  return pd.Series(range(a, b)) 



# ! Test case entities 
@dataclass
class Region:
  region_id: Annotated[int, PrimaryKey()] 


@dataclass
class Customer: 
    customer_id: Annotated[int, generate_sequential, PrimaryKey()] 
    created_at: Annotated[datetime, CreationTime()] # temporal integrity 
    region_id: Annotated[int, ForeignKey(Region)] 

    email: Annotated[str, Unique(), Faker("email")] 
    code: Annotated[str, Pattern(r'[A-Z]{3}-\d{4}')] 


    # first_name: Annotated[str, Faker("first_name")] 
    # age: Annotated[int, Distribution("normal", mean=38, std=12, min=18, max=90)] 
    # region: Annotated[str, Categorical(["North", "South", "East", "West"])] 
    # created_at: Annotated[datetime, Temporal(before="updated_at")] # temporal integrity 
    # updated_at: Annotated[datetime, Temporal(after="created_at")]

@dataclass
class Transaction:
  transaction_id: Annotated[int, PrimaryKey()]
  created_at: Annotated[datetime, CreationTime()] # temporal integrity 
  customer_id: Annotated[int, ForeignKey(Customer)]
  


# Predefined not simulated data. 
df_region = pd.DataFrame( range(50), columns=['region_id']) 
df_customer = pd.DataFrame( range(100), columns=['customer_id']) 
df_customer['region_id'] = np.random.choice(df_region['region_id']) 
df_customer['email'] = ' ... '
df_customer['code'] = ' ... '

# Simulation 
sim = Simulator({ 
  Region:EntityContext(Region, preexisting=df_region, done=True), 
  Customer:EntityContext(Customer, preexisting=df_customer, N=100), 
  Transaction:EntityContext(Transaction, N=1000) 
  }) 
sim.simulate_all_entities()





