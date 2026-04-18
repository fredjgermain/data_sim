from typing import Annotated, get_args, get_origin, overload
from _common.interface import Field, IAnnotation



# ---------------------------------------------------------------------------
# Entity
# ---------------------------------------------------------------------------
class Entity:
    
    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.__bases__ != (Entity,):
            raise TypeError(
                f"'{cls.__name__}' cannot subclass '{cls.__bases__[0].__name__}' — "
                "only direct subclasses of Entity are allowed."
            )

    @classmethod
    def inspect(cls) -> dict[str, Field]: 
      fields = {}
      for name, hint in cls.__annotations__.items():
          if get_origin(hint) is not Annotated:
            fields[name] = Field(name=name, base_type=hint, annotations={})
            continue
          
          base_type, *anns = get_args(hint) 
          ann_dict = cls._parse_annotations(anns) 
          fields[name] = Field(name=name, base_type=base_type, annotations=ann_dict) 
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
    def get(cls) -> list[Field]: ...
    @classmethod
    @overload
    def get(cls, selection: str | type) -> Field | None: ...
    @classmethod
    @overload
    def get(cls, selection: list[str | type]) -> list[Field]: ...
    

    @classmethod
    def get(cls, selection=None):
        if selection is None:
            return list(cls.inspect().values())       # [] → all fields
        if not isinstance(selection, list):
            return next(iter(cls._search([selection])), None)  # single → field or None
        return cls._search(selection)                 # list → list of fields

    @classmethod
    def _search(cls, selection: list) -> list[Field]:
        result = []
        for name, fld in cls.inspect().items():
            ann_sel = [s for s in selection if isinstance(s, type)]
            if name in selection or (ann_sel and fld.has(*ann_sel)):
                result.append(fld)
        return result
