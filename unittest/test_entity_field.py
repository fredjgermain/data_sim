"""
tests/test_entity_field.py
~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for EntityField (src/entity.py).

Covered methods:
    - get()
    - get_many()
    - has()
"""

import pytest
from src.entity import EntityField
from src.annotations.primaries import PrimaryKey 
from src.annotations.standardgen import GenNormal, GenUniform, IGen, IGen, Transformer
from src.annotations.validation import Unique
from src.annotations.fault import Nullify, IFault, Misspell


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_field(annotations: dict = None) -> EntityField:
    return EntityField(name="field", base_type=int, annotations=annotations or {})


# Shared annotation instances
_pk         = PrimaryKey()
_gen        = GenNormal(mean=50, std=10)
_uniform    = GenUniform(min=0, max=10)
_nullify    = Nullify() 
_misspell   = Misspell() 
_trf        = Transformer(lambda serie:serie) 




# ---------------------------------------------------------------------------
# Tests: get()
# ---------------------------------------------------------------------------

class TestGet:

    @pytest.mark.parametrize("annotations, query, expected", [
        # Exact type match
        ({PrimaryKey: _pk},                  PrimaryKey,   _pk),
        ({GenNormal:  _gen},                 GenNormal,    _gen),
        # Parent type match (subclass resolution)
        ({GenNormal:  _gen},                 IGen,         _gen),
        ({PrimaryKey: _pk},                  IGen,         None),
        # Multiple annotations — correct one returned
        ({PrimaryKey: _pk, GenNormal: _gen}, PrimaryKey,   _pk),
        ({PrimaryKey: _pk, GenNormal: _gen}, GenNormal,    _gen),
        # Absent annotation
        ({PrimaryKey: _pk},                  Unique,       None),
        # Empty annotations
        ({},                                 PrimaryKey,   None),
    ])
    def test_get(self, annotations, query, expected):
        fld = make_field(annotations)
        assert fld.get(query) is expected 


# ---------------------------------------------------------------------------
# Tests: get_many()
# ---------------------------------------------------------------------------

class TestGetMany:

    @pytest.mark.parametrize("annotations, query, expected_len, expected_items", [
        # Two fault annotations stored under different keys — both returned
        # Mixed annotations — only IStandardGen instances returned
        ({GenNormal: _gen, PrimaryKey: _pk},                         IGen, 1,      [_gen]),
        # Single match
        ({GenNormal: _gen},                                          IGen, 1,      [_gen]),
        # No match
        ({PrimaryKey: _pk},                                          IFault, 0,    []),
        # many Ifault
        ({Nullify: _nullify, Misspell: _misspell},                   IFault, 2,     [_nullify, _misspell]), 
        # Empty annotations
        ({},                                                         PrimaryKey, 0, []),
    ])
    def test_get_many(self, annotations, query, expected_len, expected_items):
        fld = make_field(annotations)
        result = fld.get_many(query)
        assert len(result) == expected_len
        for item in expected_items:
            assert item in result


# ---------------------------------------------------------------------------
# Tests: has()
# ---------------------------------------------------------------------------

class TestHas:

    @pytest.mark.parametrize("annotations, query_types, expected", [
        # Exact type present
        ({PrimaryKey: _pk},                  (PrimaryKey,),        True),
        # Parent type present
        ({GenNormal:  _gen},                 (IGen,),              True),
        # Absent annotation
        ({PrimaryKey: _pk},                  (Unique,),            False),
        # Empty annotations
        ({},                                 (PrimaryKey,),        False),
        # Any-of-multiple — none match
        ({GenNormal:  _gen},                 (PrimaryKey,),        False),
    ])
    def test_has(self, annotations, query_types, expected):
        fld = make_field(annotations)
        assert fld.has(*query_types) is expected
