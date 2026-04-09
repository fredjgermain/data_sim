"""
data_simulator.entity
~~~~~~~~~~~~~~~~~~~~~
EntityField, IEntity, and Entity — the introspection layer that lets
DataSimulator reason about user-defined entity dataclasses at runtime.
"""


from typing import Annotated, get_args, get_origin, overload

from dataclasses import dataclass
from data_simulator.annotations.primaries import PrimaryKey, CreationTime 
from data_simulator.interface import IEntity, IEntityField, IAnnotation 



# ---------------------------------------------------------------------------
# EntityField
# ---------------------------------------------------------------------------
@dataclass
class EntityField(IEntityField):
    name:        str 
    base_type:   type 
    annotations: dict[type[IAnnotation], IAnnotation] 
    
    def get[A](self, annotation_type: type[A]) -> A | None:
        result = self.annotations.get(annotation_type)
        if result is not None:
            return result 
        return next((a for a in self.annotations.values() if isinstance(a, annotation_type)), None) 

    def get_many[A](self, annotation_type: type[A]) -> list[A]:
        return [ a for a in self.annotations.values() if isinstance(a, annotation_type) ]

    def has(self, *annotation_types: type) -> bool:
        return any(
            isinstance(ann, t)
            for ann in self.annotations.values()
            for t in annotation_types
        )



# ---------------------------------------------------------------------------
# Entity
# ---------------------------------------------------------------------------
class Entity(IEntity):
    
    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.__bases__ != (Entity,):
            raise TypeError(
                f"'{cls.__name__}' cannot subclass '{cls.__bases__[0].__name__}' — "
                "only direct subclasses of Entity are allowed."
            )

    @classmethod
    def inspect(cls) -> dict[str, IEntityField]: 
      fields = {}
      for name, hint in cls.__annotations__.items():
          if get_origin(hint) is not Annotated:
            fields[name] = EntityField(name=name, base_type=hint, annotations={})
            continue
          
          base_type, *anns = get_args(hint) 
          ann_dict = Entity._parse_annotations(anns) 
          fields[name] = EntityField(name=name, base_type=base_type, annotations=ann_dict) 
      return fields 
  
    @classmethod
    def _parse_annotations(cls, args) -> dict[type[IAnnotation], IAnnotation]:
        ann_dict: dict[type[IAnnotation], IAnnotation] = {}
        for ann in args:
            if not isinstance(ann, IAnnotation):
                continue
            ann_type = type(ann)
            if ann_type in ann_dict:
                raise TypeError(
                    f"Duplicate annotation type '{ann_type.__name__}' on the same field."
                )
            ann_dict[ann_type] = ann
        return ann_dict


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
    def get(cls, selection=None):
        if selection is None:
            return list(cls.inspect().values())       # [] → all fields
        if not isinstance(selection, list):
            return next(iter(cls._search([selection])), None)  # single → field or None
        return cls._search(selection)                 # list → list of fields

    @classmethod
    def _search(cls, selection: list) -> list[IEntityField]:
        result = []
        for name, fld in cls.inspect().items():
            ann_sel = [s for s in selection if isinstance(s, type)]
            if name in selection or (ann_sel and fld.has(*ann_sel)):
                result.append(fld)
        return result
