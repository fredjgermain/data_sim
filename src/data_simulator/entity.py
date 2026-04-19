from typing import Annotated, get_args, get_origin, overload
from _common.interface import Field, IAnnotation, Profile



# ---------------------------------------------------------------------------
# Entity
# ---------------------------------------------------------------------------
class Entity(Profile):
    
    @classmethod
    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        if cls.__bases__ != (Entity,):
            raise TypeError(
                f"'{cls.__name__}' cannot subclass '{cls.__bases__[0].__name__}' — "
                "only direct subclasses of Entity are allowed."
            )


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


