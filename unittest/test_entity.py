"""
tests/test_entity.py
~~~~~~~~~~~~~~~~~~~~
Unit tests for Entity (src/entity.py).

Covered methods:
    - inspect()
    - _parse_annotations()
    - find()
    - select()
    - get()
    - get_primary_key_field()
    - get_creation_time_field()
"""

import datetime
import pytest
from typing import Annotated
from dataclasses import dataclass

from entity import Entity, EntityField
from annotations.primaries import PrimaryKey, CreationTime
from annotations.generator import GenNormal, GenUniform, GenFaker, IGen, IGen
from annotations.validation import Unique
from annotations.fault import Nullify
from annotations.fault import IFault


# ---------------------------------------------------------------------------
# Entity fixtures
# ---------------------------------------------------------------------------

@dataclass
class FullEntity(Entity):
    id:         Annotated[int,              PrimaryKey()]
    created_at: Annotated[datetime.datetime, CreationTime(
                    start=datetime.datetime(2020, 1, 1),
                    end=datetime.datetime(2024, 1, 1),
                )]
    score:      Annotated[float,            GenNormal(mean=50, std=10)]
    label:      Annotated[str,              GenFaker("name") ]
    amount:     Annotated[float,            GenUniform(min=0, max=100), Nullify(prob=0.05)]


@dataclass
class NoPrimaryKeyEntity(Entity):
    score: Annotated[float, GenNormal(mean=0, std=1)]


@dataclass
class NoCreationTimeEntity(Entity):
    id:    Annotated[int,   PrimaryKey()]
    score: Annotated[float, GenNormal(mean=0, std=1)]


@dataclass
class PlainTypeEntity(Entity):
    """Fields declared without Annotated — plain type hints only."""
    id:    int
    score: float


@dataclass
class EmptyEntity(Entity):
    pass


# ---------------------------------------------------------------------------
# Tests: inspect()
# ---------------------------------------------------------------------------

class TestInspect:

    @pytest.mark.parametrize("entity, expected_keys", [
        (FullEntity,        {"id", "created_at", "score", "label", "amount"}),
        (NoPrimaryKeyEntity, {"score"}),
        (PlainTypeEntity,   {"id", "score"}),
        (EmptyEntity,       set()),
    ])
    def test_inspect_keys(self, entity:type[Entity], expected_keys):
        assert set(entity.inspect().keys()) == expected_keys

    @pytest.mark.parametrize("entity, field_name, expected_base_type", [
        (FullEntity, "id",         int),
        (FullEntity, "score",      float),
        (FullEntity, "created_at", datetime.datetime),
        (PlainTypeEntity, "id",    int),
        (PlainTypeEntity, "score", float),
    ])
    def test_inspect_base_type(self, entity:type[Entity], field_name, expected_base_type):
        assert entity.inspect()[field_name].base_type is expected_base_type

    @pytest.mark.parametrize("entity, field_name, expected_annotation_type", [
        (FullEntity, "id",     PrimaryKey),
        (FullEntity, "score",  GenNormal),
        (FullEntity, "label",  GenFaker),
        (FullEntity, "amount", Nullify),
    ])
    def test_inspect_annotations_present(self, entity:type[Entity], field_name, expected_annotation_type):
        assert expected_annotation_type in entity.inspect()[field_name].annotations

    @pytest.mark.parametrize("entity, field_name", [
        (PlainTypeEntity, "id"),
        (PlainTypeEntity, "score"),
    ])
    def test_inspect_plain_type_has_empty_annotations(self, entity:type[Entity], field_name):
        assert entity.inspect()[field_name].annotations == {}



class TestGet:

    @pytest.mark.parametrize("entity, selection, expected", [
        # By field name string
        (FullEntity,         "id",                "id"),
        (FullEntity,         ["id"],              {"id"}),
        (FullEntity,         ["id", "score"],     {"id", "score"}),
        (FullEntity,         None,                {"id", 'created_at', "score", 'label', 'amount'}),
        (FullEntity,         [],                  set()),
        # By annotation type
        (FullEntity,         [PrimaryKey],        {"id"}),
        (FullEntity,         [CreationTime],      {"created_at"}),
        # By parent annotation type
        (FullEntity,         [IGen],              {"score", "label", "amount"}),
        (FullEntity,         [IFault],            {"amount"}),
        # Mixed name and type
        (FullEntity,         ["id", GenNormal],   {"id", "score"}),
        # Unknown name
        (FullEntity,         ["nonexistent"],     set()),
        # Annotation absent from entity
        (NoPrimaryKeyEntity, [PrimaryKey],        set()),
        # Empty selection
        (FullEntity,         [],                  set()),
    ])
    def test_get(self, entity:type[Entity], selection, expected):
        if isinstance(selection, list) or selection is None: 
            result = entity.get(selection) 
            assert {fld.name for fld in result} == expected 
        elif isinstance(selection, str|type): 
            result = entity.get(selection) 
            assert result.name == expected 
            



