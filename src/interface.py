import pandas as pd
from typing import Protocol, overload
from typing import Protocol



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

    # @classmethod
    # def get_primary_key_field(cls) -> IEntityField: ...

    # @classmethod
    # def get_creation_time_field(cls) -> IEntityField | None: ...
        


# IEntityContext  =================

class IEntityContext(Protocol):
    entity:      type[IEntity]
    preexisting: pd.DataFrame
    N:           int
    generated:   pd.DataFrame 

    def get_primary_key_values(self) -> pd.Series: ...

    def get_creation_time_values(self) -> pd.Series: ...
    
    def get_serie(self, 
        selection: str | type, 
        preexisting: bool = True, 
        generated: bool = True
    ) -> pd.Series: ...
    
    def get_data(
        self,
        include: list[str | type] | None = None, 
        exclude: list[str | type] | None = None,
        preexisting: bool =True, 
        generated: bool = True, 
    ) -> pd.DataFrame: ... 


