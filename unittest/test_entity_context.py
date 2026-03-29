"""
tests/test_entity_context.py
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
Unit tests for EntityContext (src/context.py).

Covered methods:
    - get_serie()
    - get_primary_key_values()
    - get_creation_time_values()
    - get_data()
"""

import datetime
import pytest
import pandas as pd
from typing import Annotated
from dataclasses import dataclass

from src.context import EntityContext
from src.entity import Entity
from src.annotations.primaries import PrimaryKey, CreationTime, ForeignKey
from src.annotations.standardgen import GenNormal


# ---------------------------------------------------------------------------
# Minimal Entity fixtures
# ---------------------------------------------------------------------------

@dataclass
class SimpleEntity(Entity):
    """Entity with a PrimaryKey, CreationTime, and one generated field."""
    id:         Annotated[int,              PrimaryKey()]
    created_at: Annotated[datetime.datetime, CreationTime(
                    start=datetime.datetime(2020, 1, 1),
                    end=datetime.datetime(2024, 1, 1),
                )]
    score:      Annotated[float,            GenNormal(mean=50, std=10)]


@dataclass
class NoPrimaryKeyEntity(Entity):
    """Entity intentionally missing a PrimaryKey."""
    name:  Annotated[str, GenNormal(mean=0, std=1)]   # GenNormal on str is odd but valid for structure tests
    score: Annotated[float, GenNormal(mean=50, std=10)]


@dataclass
class NoCreationTimeEntity(Entity):
    """Entity intentionally missing a CreationTime."""
    id:    Annotated[int, PrimaryKey()]
    score: Annotated[float, GenNormal(mean=50, std=10)]


# ---------------------------------------------------------------------------
# Shared DataFrames
# ---------------------------------------------------------------------------

def make_preexisting() -> pd.DataFrame:
    return pd.DataFrame({
        "id":         [1, 2, 3],
        "created_at": [
            datetime.datetime(2020, 6, 1),
            datetime.datetime(2021, 3, 15),
            datetime.datetime(2022, 9, 10),
        ],
        "score": [10.0, 20.0, 30.0],
    })


def make_generated() -> pd.DataFrame:
    return pd.DataFrame({
        "id":         [4, 5],
        "created_at": [
            datetime.datetime(2023, 1, 1),
            datetime.datetime(2023, 6, 1),
        ],
        "score": [40.0, 50.0],
    })


# ---------------------------------------------------------------------------
# Tests: initialisation
# ---------------------------------------------------------------------------

class TestEntityContextInit:

    def test_generated_defaults_to_empty_dataframe(self):
        ctx = EntityContext(SimpleEntity, make_preexisting(), N=5)
        assert isinstance(ctx.generated, pd.DataFrame)
        assert ctx.generated.empty

    def test_stores_entity_class(self):
        ctx = EntityContext(SimpleEntity, make_preexisting(), N=5)
        assert ctx.entity is SimpleEntity

    def test_stores_N(self):
        ctx = EntityContext(SimpleEntity, make_preexisting(), N=7)
        assert ctx.N == 7

    def test_accepts_empty_preexisting(self):
        ctx = EntityContext(SimpleEntity, pd.DataFrame(), N=5)
        assert ctx.preexisting.empty


# ---------------------------------------------------------------------------
# Tests: get_serie()
# ---------------------------------------------------------------------------

class TestGetSerie:

    def setup_method(self):
        self.pre = make_preexisting()
        self.gen = make_generated()
        self.ctx = EntityContext(SimpleEntity, self.pre, N=5, generated=self.gen)

    def test_returns_series(self):
        result = self.ctx.get_serie("id")
        assert isinstance(result, pd.Series)

    def test_combines_preexisting_and_generated_by_default(self):
        result = self.ctx.get_serie("id")
        assert len(result) == len(self.pre) + len(self.gen)

    def test_values_match_combined_data(self):
        result = self.ctx.get_serie("id")
        expected = [1, 2, 3, 4, 5]
        assert list(result) == expected

    def test_preexisting_only(self):
        result = self.ctx.get_serie("id", generated=False)
        assert list(result) == [1, 2, 3]

    def test_generated_only(self):
        result = self.ctx.get_serie("id", preexisting=False)
        assert list(result) == [4, 5]

    def test_unknown_field_name_returns_empty_series(self):
        result = self.ctx.get_serie("nonexistent_field")
        assert isinstance(result, pd.Series)
        assert result.empty

    def test_field_not_in_dataframe_columns_returns_empty_series(self):
        # 'score' exists in the entity but not in preexisting or generated
        ctx = EntityContext(SimpleEntity, pd.DataFrame(), N=5)
        result = ctx.get_serie("score")
        assert result.empty

    def test_select_by_annotation_type(self):
        # selecting by PrimaryKey annotation type should resolve to 'id'
        result = self.ctx.get_serie(PrimaryKey)
        assert list(result) == [1, 2, 3, 4, 5]

    def test_both_false_returns_empty_series(self):
        result = self.ctx.get_serie("id", preexisting=False, generated=False)
        assert result.empty

    def test_index_is_reset(self):
        result = self.ctx.get_serie("id")
        assert list(result.index) == list(range(len(result)))


# ---------------------------------------------------------------------------
# Tests: get_primary_key_values()
# ---------------------------------------------------------------------------

class TestGetPrimaryKeyValues:

    def test_returns_pk_values_from_preexisting(self):
        ctx = EntityContext(SimpleEntity, make_preexisting(), N=5)
        result = ctx.get_primary_key_values()
        assert list(result) == [1, 2, 3]

    def test_returns_pk_values_combined(self):
        ctx = EntityContext(SimpleEntity, make_preexisting(), N=5, generated=make_generated())
        result = ctx.get_primary_key_values()
        assert list(result) == [1, 2, 3, 4, 5]

    def test_no_primary_key_field_returns_empty(self):
        ctx = EntityContext(NoPrimaryKeyEntity, pd.DataFrame(), N=5)
        result = ctx.get_primary_key_values()
        assert result.empty

    def test_empty_preexisting_and_no_generated_returns_empty(self):
        ctx = EntityContext(SimpleEntity, pd.DataFrame(), N=5)
        result = ctx.get_primary_key_values()
        assert result.empty


# ---------------------------------------------------------------------------
# Tests: get_creation_time_values()
# ---------------------------------------------------------------------------

class TestGetCreationTimeValues:

    def test_returns_creation_time_from_preexisting(self):
        ctx = EntityContext(SimpleEntity, make_preexisting(), N=5)
        result = ctx.get_creation_time_values()
        assert len(result) == 3
        assert result.iloc[0] == datetime.datetime(2020, 6, 1)

    def test_returns_creation_time_combined(self):
        ctx = EntityContext(SimpleEntity, make_preexisting(), N=5, generated=make_generated())
        result = ctx.get_creation_time_values()
        assert len(result) == 5

    def test_no_creation_time_field_returns_empty(self):
        ctx = EntityContext(NoCreationTimeEntity, pd.DataFrame(), N=5)
        result = ctx.get_creation_time_values()
        assert result.empty


# ---------------------------------------------------------------------------
# Tests: get_data()
# ---------------------------------------------------------------------------

class TestGetData:

    def setup_method(self):
        self.pre = make_preexisting()
        self.gen = make_generated()
        self.ctx = EntityContext(SimpleEntity, self.pre, N=5, generated=self.gen)

    def test_returns_dataframe(self):
        result = self.ctx.get_data()
        assert isinstance(result, pd.DataFrame)

    def test_combines_preexisting_and_generated(self):
        result = self.ctx.get_data()
        assert len(result) == len(self.pre) + len(self.gen)

    def test_preexisting_only(self):
        result = self.ctx.get_data(generated=False)
        assert len(result) == len(self.pre)

    def test_generated_only(self):
        result = self.ctx.get_data(preexisting=False)
        assert len(result) == len(self.gen)

    def test_include_filters_columns(self):
        result = self.ctx.get_data(include=["id"])
        assert list(result.columns) == ["id"]

    def test_include_by_annotation_type(self):
        result = self.ctx.get_data(include=[PrimaryKey])
        assert "id" in result.columns
        assert "score" not in result.columns

    def test_exclude_removes_column(self):
        result = self.ctx.get_data(exclude=["score"])
        assert "score" not in result.columns
        assert "id" in result.columns

    def test_exclude_by_annotation_type(self):
        result = self.ctx.get_data(exclude=[PrimaryKey])
        assert "id" not in result.columns

    def test_include_and_exclude_combined(self):
        # include all, then exclude 'score' — only id and created_at remain
        result = self.ctx.get_data(include=["id", "score"], exclude=["score"])
        assert "score" not in result.columns
        assert "id" in result.columns

    def test_empty_preexisting_and_generated_returns_empty(self):
        ctx = EntityContext(SimpleEntity, pd.DataFrame(), N=5)
        result = ctx.get_data()
        assert result.empty

    def test_index_is_reset(self):
        result = self.ctx.get_data()
        assert list(result.index) == list(range(len(result)))

    def test_column_values_are_correct(self):
        result = self.ctx.get_data(include=["id"], generated=False)
        assert list(result["id"]) == [1, 2, 3]

    def test_no_matching_columns_returns_empty_dataframe(self):
        # Request a field that exists on the entity but isn't in either DataFrame
        ctx = EntityContext(SimpleEntity, pd.DataFrame({"id": [1]}), N=0)
        result = ctx.get_data(include=["score"])  # score not in the df
        assert result.empty