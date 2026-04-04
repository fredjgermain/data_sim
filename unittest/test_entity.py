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

from src.entity import Entity, EntityField
from src.annotations.primaries import PrimaryKey, CreationTime
from src.annotations.standardgen import GenNormal, GenUniform, GenFaker
from src.annotations.validation import Unique
from src.annotations.fault import Nullify
from src.annotations.base import IStandardGen, IFault, IGen


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
    label:      Annotated[str,              GenFaker("name"), Unique()]
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
        (FullEntity, "label",  Unique),
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


# ---------------------------------------------------------------------------
# Tests: _parse_annotations()
# ---------------------------------------------------------------------------

class TestParseAnnotations:

    _pk   = PrimaryKey()
    _gen  = GenNormal(mean=0, std=1)
    _uniq = Unique()

    @pytest.mark.parametrize("args, expected_types", [
        ([_pk],           {PrimaryKey}),
        ([_gen],          {GenNormal}),
        ([_pk, _gen],     {PrimaryKey, GenNormal}),
        ([_pk, _uniq],    {PrimaryKey, Unique}),
        ([],              set()),
        # Non-IAnnotation args are ignored
        (["metadata", 42], set()),
        ([_pk, "ignored"], {PrimaryKey}),
    ])
    def test_parse_annotations_keys(self, args, expected_types):
        result = Entity._parse_annotations(args)
        assert set(result.keys()) == expected_types


# ---------------------------------------------------------------------------
# Tests: find()
# ---------------------------------------------------------------------------

class TestFind:

    @pytest.mark.parametrize("entity, selection, expected_names", [
        # By field name string
        (FullEntity,         ["id"],              {"id"}),
        (FullEntity,         ["id", "score"],     {"id", "score"}),
        # By annotation type
        (FullEntity,         [PrimaryKey],        {"id"}),
        (FullEntity,         [CreationTime],      {"created_at"}),
        # By parent annotation type
        (FullEntity,         [IStandardGen],      {"score", "label", "amount"}),
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
    def test_find(self, entity:type[Entity], selection, expected_names):
        result = entity.find(selection)
        assert {fld.name for fld in result} == expected_names


# ---------------------------------------------------------------------------
# Tests: select()
# ---------------------------------------------------------------------------

class TestSelect:

    @pytest.mark.parametrize("entity, inclusion, exclusion, expected_names", [
        # No filters — all fields returned
        (FullEntity, None,             None,          {"id", "created_at", "score", "label", "amount"}),
        # Inclusion only
        (FullEntity, ["id", "score"],  None,          {"id", "score"}),
        (FullEntity, [IStandardGen],   None,          {"score", "label", "amount"}),
        # Exclusion only
        (FullEntity, None,             ["id"],        {"created_at", "score", "label", "amount"}),
        (FullEntity, None,             [PrimaryKey],  {"created_at", "score", "label", "amount"}),
        # Inclusion + exclusion combined
        (FullEntity, ["id", "score"],  ["score"],     {"id"}),
        (FullEntity, [IStandardGen],   [Unique],      {"score", "amount"}),
        # Exclusion of all
        (PlainTypeEntity, None,        ["id", "score"], set()),
        # Empty entity
        (EmptyEntity,    None,         None,          set()),
    ])
    def test_select(self, entity:type[Entity], inclusion, exclusion, expected_names):
        result = entity.select(inclusion, exclusion)
        assert {fld.name for fld in result} == expected_names


# ---------------------------------------------------------------------------
# Tests: get()
# ---------------------------------------------------------------------------

class TestGet:

    @pytest.mark.parametrize("entity, selection, expected_name", [
        # By field name
        (FullEntity,         "id",          "id"),
        (FullEntity,         "score",       "score"),
        # By annotation type
        (FullEntity,         PrimaryKey,    "id"),
        (FullEntity,         CreationTime,  "created_at"),
        # Unknown
        (FullEntity,         "nonexistent", None),
        (NoPrimaryKeyEntity, PrimaryKey,    None),
    ])
    def test_get(self, entity:type[Entity], selection, expected_name):
        fld = entity.get(selection)
        assert (fld.name if fld else None) == expected_name


# ---------------------------------------------------------------------------
# Tests: get_primary_key_field()
# ---------------------------------------------------------------------------

class TestGetPrimaryKeyField:

    @pytest.mark.parametrize("entity, expected_name", [
        (FullEntity,          "id"),
        (NoCreationTimeEntity, "id"),
        (NoPrimaryKeyEntity,   None),
        (EmptyEntity,          None),
    ])
    def test_get_primary_key_field_name(self, entity:type[Entity], expected_name):
        fld = entity.get_primary_key_field()
        assert (fld.name if fld else None) == expected_name

    @pytest.mark.parametrize("entity", [FullEntity, NoCreationTimeEntity])
    def test_returned_field_has_pk_annotation(self, entity:type[Entity]):
        fld = entity.get_primary_key_field()
        assert fld.get(PrimaryKey) is not None


# ---------------------------------------------------------------------------
# Tests: get_creation_time_field()
# ---------------------------------------------------------------------------

class TestGetCreationTimeField:

    @pytest.mark.parametrize("entity, expected_name", [
        (FullEntity,           "created_at"),
        (NoPrimaryKeyEntity,   None),
        (NoCreationTimeEntity, None),
        (EmptyEntity,          None),
    ])
    def test_get_creation_time_field_name(self, entity:type[Entity], expected_name):
        fld = entity.get_creation_time_field()
        assert (fld.name if fld else None) == expected_name

    def test_returned_field_has_creation_time_annotation(self):
        fld = FullEntity.get_creation_time_field()
        assert fld.get(CreationTime) is not None
