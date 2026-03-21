import pandas as pd
from dataclasses import dataclass, fields, field
import typing
from typing import Protocol, Callable, Any

from src.entity_annotation import PrimaryKey, CreationTime, Faker, ForeignFields, ForeignKey, Pattern, Unique 
from src.entity_common import IEntity, EntityField



class Entity(IEntity):
    """Base class for all simulated entities.

    Provides classmethod introspection and enforces annotation uniqueness
    (at most one PrimaryKey, at most one CreationTime) via __init_subclass__,
    which runs automatically at class definition time.
    """

    # def __init_subclass__(cls, **kwargs):
    #     super().__init_subclass__(**kwargs)
    #     # ! Validate that there's no more than one primarykey and one creation time on the same entity 
    #     for annotation, label in [(PrimaryKey, "PrimaryKey"), (CreationTime, "CreationTime")]:
    #         flagged = [f.name for f in cls.inspect().values() if f.has(annotation)]
    #         if len(flagged) > 1:
    #             raise TypeError(f"'{cls.__name__}' has multiple {label} fields: {flagged}.")

    @classmethod
    def inspect(cls) -> dict[str, EntityField]:
        result = {}
        hints = typing.get_type_hints(cls, include_extras=True)
        for f in fields(cls):
            hint = hints[f.name]
            if typing.get_origin(hint) is typing.Annotated:
                base_type, *annotations = typing.get_args(hint)
                func = next((a for a in annotations if callable(a)), None)
                meta = {type(a): a for a in annotations if not callable(a)}
                result[f.name] = EntityField(name=f.name, base_type=base_type, func=func, annotations=meta)
            else:
                result[f.name] = EntityField(name=f.name, base_type=hint)
        return result

    @classmethod
    def get_fields_by_annotation(cls, selection: list = None, exclusion: list = None) -> list[EntityField]:
        selection, exclusion = selection or [], exclusion or []
        return [
            fld for fld in cls.inspect().values()
            if (not selection or any(a in fld.annotations for a in selection))
            and (not exclusion or not any(a in fld.annotations for a in exclusion))
        ]

    @classmethod
    def get_primary_key_field(cls) -> EntityField | None:
        return next((f for f in cls.inspect().values() if PrimaryKey in f.annotations ), None)

    @classmethod
    def get_primary_time_field(cls) -> EntityField | None:
        return next((f for f in cls.inspect().values() if CreationTime in f.annotations ), None)



@dataclass
class EntityContext:
  entity: type[IEntity] 
  preexisting: pd.DataFrame = field(default_factory=pd.DataFrame)
  generated: pd.DataFrame = field(default_factory=pd.DataFrame)
  N: int = 0
  done: bool = False
  
  def get_primarykey_values(self) -> pd.Series:
    col = self.entity.get_primary_key_field().name
    return pd.concat([self.preexisting, self.generated])[col] 

  def get_creationtime_values(self) -> pd.Series:
    col = self.entity.get_primary_time_field().name
    return pd.concat([self.preexisting, self.generated])[col]

  def get_data(self, selection:list = None, exclusion:list = None ) -> pd.DataFrame: 
    flds = self.entity.get_fields_by_annotation(selection, exclusion) 
    df = pd.concat([self.preexisting, self.generated]) 
    selection = [ fld.name for fld in flds if fld.name in list(df.columns) ] or list(df.columns) 
    return df[selection]

  