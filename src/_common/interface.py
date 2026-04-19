from dataclasses import dataclass 
from typing import Annotated, get_args, get_origin 


class IAnnotation:
  ...



@dataclass
class Field:
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


class Profile:

    @classmethod
    def inspect(cls) -> dict[str, Field]: 
      fields = {}
      for name, hint in cls.__annotations__.items():
          if get_origin(hint) is not Annotated:
            fields[name] = Field(name=name, base_type=hint, annotations={})
            continue
          
          base_type, *anns = get_args(hint) 
          ann_dict = {type(a):a for a in anns} 
          fields[name] = Field(name=name, base_type=base_type, annotations=ann_dict) 
      return fields 
  