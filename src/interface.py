import pandas as pd
from dataclasses import dataclass
from typing import Protocol, overload


class IAnnotation:
    ...

# IEntityField  ===================
class IEntityField(Protocol):
    name:        str
    base_type:   type
    annotations: dict[type[IAnnotation], IAnnotation]
    
    def get[A](self, annotation_type: type[A]) -> A | None: ...
    
    def get_many[A](self, annotation_type: type[A]) -> list[A]: ... 

    def has(self, *annotation_types: type) -> bool: ...


# IEntity  ========================
class IEntity(Protocol):
    
    @classmethod
    def inspect(cls) -> dict[str, IEntityField]: ... 

    @classmethod
    @overload
    def get(cls) -> list[IEntityField]: ...
    @classmethod
    @overload
    def get(cls, selection: str | type) -> IEntityField | None: ...
    @classmethod
    @overload
    def get(cls, selection: list[str | type]) -> list[IEntityField]: ...
    
    @classmethod
    def get(cls, selection=None): ... 


# IEntityContext  =================

class IEntityContext(Protocol):
    entity:      type[IEntity]
    preexisting: pd.DataFrame
    N:           int
    generated:   pd.DataFrame 
    
    def get_serie(self, 
        selection: str | type, 
        preexisting: bool = True, 
        generated: bool = True
    ) -> pd.Series: ...
    
    def get_data(
        self,
        selection: list[str | type] | None = None, 
        preexisting: bool =True, 
        generated: bool = True, 
    ) -> pd.DataFrame: ... 


