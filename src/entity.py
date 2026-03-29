"""
data_simulator.entity
~~~~~~~~~~~~~~~~~~~~~
EntityField, IEntity, and Entity — the introspection layer that lets
DataSimulator reason about user-defined entity dataclasses at runtime.
"""


from typing import Annotated, get_args, get_origin

from dataclasses import dataclass
from src.annotations.base import IAnnotation
from src.annotations.primaries import PrimaryKey, CreationTime 
from src.interface import IEntity, IEntityField 


# ---------------------------------------------------------------------------
# EntityField
# ---------------------------------------------------------------------------
@dataclass
class EntityField(IEntityField):
    """Holds introspected metadata for a single field of an Entity subclass.

    Attributes:
        name:        The field name as declared in the dataclass.
        base_type:   The unwrapped Python type (e.g. int, str, datetime).
        annotations: A dict mapping each annotation type to its instance.
                     At most one annotation of each type is allowed per field;
                     IEntity.inspect() raises TypeError on duplicates.

    Example (internal representation of ``age: Annotated[int, GenNormal(...)]``):
        EntityField(
            name="age",
            base_type=int,
            annotations={GenNormal: GenNormal(mean=45, std=20, ...)},
        )
    """

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
    """Base class for all user-defined entity dataclasses.

    Users inherit from this class and declare fields using Annotated types:

    Example::

        @dataclass
        class Customer(Entity):
            customer_id: Annotated[int,  PrimaryKey()]
            email:       Annotated[str,  Unique(), Faker("email")]
            age:         Annotated[int,  GenNormal(min=0, mean=45, std=20)]

    No additional attributes are defined here — all behaviour lives in the
    IEntity classmethods and is inherited automatically.
    """
    
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
            ann_dict[type(ann)] = ann
        return ann_dict


    @classmethod
    def find(cls, selection: list[str | type]) -> list[IEntityField]:
        result = []
        for name, fld in cls.inspect().items():
            ann_sel = [s for s in selection if isinstance(s, type)]
            if name in selection or (ann_sel and fld.has(*ann_sel)):
                result.append(fld)
        return result

    @classmethod
    def select( cls, 
        inclusion: list[str | type] | None = None, 
        exclusion: list[str | type] | None = None
    ) -> list[IEntityField]:
        
        flds_in = cls.find(inclusion) if inclusion is not None else list(cls.inspect().values())
        flds_ex = Entity.find(exclusion) if exclusion is not None else []
        name_ex = {fld.name for fld in flds_ex} 
        return [fld for fld in flds_in if fld.name not in name_ex] 

    @classmethod
    def get_primary_key_field(cls) -> IEntityField:
        # there can be no primarykey, that is an acceptable case, but never more than one. 
        # If there were more than one primarykey that should be caught somewhere else. 
        return next(iter(cls.find([PrimaryKey])), None)

    @classmethod
    def get_creation_time_field(cls) -> IEntityField | None:
        # there can be no creationtime, that is an acceptable case, but never more than one. 
        # If there were more than one creationtime that should be caught somewhere else. 
        return next(iter(cls.find([CreationTime])), None)
