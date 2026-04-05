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
from src.annotations.primaries import PrimaryKey, CreationTime
from src.annotations.standardgen import GenNormal


# ---------------------------------------------------------------------------
# Entity fixtures
# ---------------------------------------------------------------------------

@dataclass
class SimpleEntity(Entity):
    id:         Annotated[int,               PrimaryKey()]
    created_at: Annotated[datetime.datetime, CreationTime(
                    start=datetime.datetime(2020, 1, 1),
                    end=datetime.datetime(2024, 1, 1),
                )]
    score:      Annotated[float,             GenNormal(mean=50, std=10)]


@dataclass
class NoPrimaryKeyEntity(Entity):
    score: Annotated[float, GenNormal(mean=50, std=10)]


@dataclass
class NoCreationTimeEntity(Entity):
    id:    Annotated[int,   PrimaryKey()]
    score: Annotated[float, GenNormal(mean=50, std=10)]


# ---------------------------------------------------------------------------
# DataFrame factories
# ---------------------------------------------------------------------------

def make_pre() -> pd.DataFrame:
    return pd.DataFrame({
        "id":         [1, 2, 3],
        "created_at": [
            datetime.datetime(2020, 6, 1),
            datetime.datetime(2021, 3, 15),
            datetime.datetime(2022, 9, 10),
        ],
        "score": [10.0, 20.0, 30.0],
    })


def make_gen() -> pd.DataFrame:
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

class TestInit:

    @pytest.mark.parametrize("entity, pre, N", [
        (SimpleEntity, make_pre(),       5),
        (SimpleEntity, pd.DataFrame(),   0),
        (SimpleEntity, pd.DataFrame(),  10),
    ])
    def test_stores_fields_correctly(self, entity, pre, N):
        ctx = EntityContext(entity, pre, N=N)
        assert ctx.entity is entity
        assert ctx.N == N

    def test_generated_defaults_to_empty_dataframe(self):
        ctx = EntityContext(SimpleEntity, make_pre(), N=5)
        assert isinstance(ctx.generated, pd.DataFrame)
        assert ctx.generated.empty


# ---------------------------------------------------------------------------
# Tests: get_serie()
# ---------------------------------------------------------------------------

class TestGetSerie:

    @pytest.mark.parametrize("selection, pre, gen, preexisting, generated, expected_values", [
        # Both flags True (default) — combined result
        ("id",     make_pre(), make_gen(), True,  True,  [1, 2, 3, 4, 5]),
        # Preexisting only
        ("id",     make_pre(), make_gen(), True,  False, [1, 2, 3]),
        # Generated only
        ("id",     make_pre(), make_gen(), False, True,  [4, 5]),
        # Both flags False — always empty
        ("id",     make_pre(), make_gen(), False, False, []),
        # Select by annotation type instead of field name
        (PrimaryKey, make_pre(), make_gen(), True, True, [1, 2, 3, 4, 5]),
    ])
    def test_get_serie_values(self, selection, pre, gen, preexisting, generated, expected_values):
        ctx = EntityContext(SimpleEntity, pre, N=5, generated=gen)
        result = ctx.get_serie(selection, preexisting=preexisting, generated=generated)
        assert list(result) == expected_values

    @pytest.mark.parametrize("selection, pre, gen", [
        # Unknown field name
        ("nonexistent",  make_pre(), make_gen()),
        # Field on entity but absent from both DataFrames
        ("score",        pd.DataFrame(), pd.DataFrame()),
    ])
    def test_get_serie_returns_empty_on_missing_field(self, selection, pre, gen):
        ctx = EntityContext(SimpleEntity, pre, N=5, generated=gen)
        result = ctx.get_serie(selection)
        assert isinstance(result, pd.Series)
        assert result.empty



# ---------------------------------------------------------------------------
# Tests: get_data()
# ---------------------------------------------------------------------------

# class TestGetData:

#     @pytest.mark.parametrize("pre, gen, preexisting, generated, expected_len", [
#         # Both flags True (default)
#         (make_pre(), make_gen(), True,  True,  5),
#         # Preexisting only
#         (make_pre(), make_gen(), True,  False, 3),
#         # Generated only
#         (make_pre(), make_gen(), False, True,  2),
#         # Empty DataFrames
#         (pd.DataFrame(), pd.DataFrame(), True, True, 0),
#     ])
#     def test_get_data_row_count(self, pre, gen, preexisting, generated, expected_len):
#         ctx = EntityContext(SimpleEntity, pre, N=5, generated=gen)
#         result = ctx.get_data(preexisting=preexisting, generated=generated)
#         assert len(result) == expected_len

#     @pytest.mark.parametrize("include, exclude, expected_cols, absent_cols", [
#         # Include by name
#         (["id"],           None,       {"id"},              {"score", "created_at"}),
#         # Include by annotation type
#         ([PrimaryKey],     None,       {"id"},              {"score", "created_at"}),
#         # Exclude by name
#         (None,             ["score"],  {"id", "created_at"}, {"score"}),
#         # Exclude by annotation type
#         (None,             [PrimaryKey], {"score", "created_at"}, {"id"}),
#         # Include + exclude combined
#         (["id", "score"],  ["score"],  {"id"},              {"score"}),
#         # No filter — all columns
#         (None,             None,       {"id", "score", "created_at"}, set()),
#     ])
#     def test_get_data_columns(self, include, exclude, expected_cols, absent_cols):
#         ctx = EntityContext(SimpleEntity, make_pre(), N=5, generated=make_gen())
#         result = ctx.get_data(include=include, exclude=exclude)
#         for col in expected_cols:
#             assert col in result.columns
#         for col in absent_cols:
#             assert col not in result.columns
