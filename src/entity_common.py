import pandas as pd 
from dataclasses import dataclass, field
from typing import Protocol, Callable, Any


@dataclass
class EntityField[T]:
    name: str
    base_type: type[T]
    func: Callable[..., Any] | None = None
    annotations: dict = field(default_factory=dict)


class IEntity(Protocol):

    @classmethod
    def inspect(cls) -> dict[str, EntityField]: ...
    
    @classmethod 
    def get_fields_with_annotation(cls, *annotations) -> list[EntityField]: ...

    @classmethod
    def primary_key_field(cls) -> EntityField | None: ... 
    
    @classmethod
    def primary_time_field(cls) -> EntityField | None: ... 



@dataclass
class EntityContext:
  entity: type[IEntity] 
  preexisting: pd.DataFrame = field(default_factory=pd.DataFrame)
  generated: pd.DataFrame = field(default_factory=pd.DataFrame)
  N: int = 0
  done: bool = False
  
  def get_columns(self, *annotations) -> list[str]: 
    flds = self.entity.get_fields_with_annotation(*annotations) 
    return [ fld.name for fld in flds ] 

  def get_data(self, *annotations) -> pd.DataFrame: 
    flds = self.entity.get_fields_with_annotation(*annotations) 
    df = pd.concat([self.preexisting, self.generated]) 
    selection = [ fld.name for fld in flds if fld.name in list(df.columns) ] or list(df.columns) 
    return df[selection]

