"""
Unit tests for EntityContext  (src/context.py)

Run from the project root with:
    pytest test_entity_context.py -v
"""

import datetime
import pytest
import pandas as pd
from dataclasses import dataclass
from typing import Annotated

from src.entity import Entity
from src.context import EntityContext
from src.annotations.primaries import PrimaryKey, CreationTime, ForeignKey


# ===========================================================================
# Entity fixtures
# ===========================================================================

START = datetime.datetime(2020, 1, 1)
END   = datetime.datetime(2024, 12, 31)


@dataclass
class WithPK(Entity):
    """Entity with a primary key and one plain field."""
    item_id: Annotated[int, PrimaryKey()]
    label:   str


@dataclass
class WithPKAndCT(Entity):
    """Entity with both a primary key and a creation-time field."""
    item_id:    Annotated[int,               PrimaryKey()]
    created_at: Annotated[datetime.datetime, CreationTime(start=START, end=END)]
    label:      str


@dataclass
class NoPKNoCT(Entity):
    """Entity with neither a primary key nor a creation-time field."""
    value: float


# ---------------------------------------------------------------------------
# Shared sample data
# ---------------------------------------------------------------------------

PRE_IDS  = [10, 20, 30]
PRE_ROWS = pd.DataFrame({"item_id": PRE_IDS, "label": ["a", "b", "c"]})

GEN_IDS  = [40, 50]
GEN_ROWS = pd.DataFrame({"item_id": GEN_IDS, "label": ["d", "e"]})

DT_1 = datetime.datetime(2021, 3, 1)
DT_2 = datetime.datetime(2022, 7, 15)

PRE_TIMED = pd.DataFrame({
    "item_id":    [1, 2],
    "created_at": [DT_1, DT_2],
    "label":      ["x", "y"],
})
GEN_TIMED = pd.DataFrame({
    "item_id":    [3],
    "created_at": [datetime.datetime(2023, 1, 1)],
    "label":      ["z"],
})


# ===========================================================================
# Helpers
# ===========================================================================

def make_ctx(entity=WithPK, preexisting=None, generated=None, N=5):
    pre = PRE_ROWS.copy() if preexisting is None else preexisting
    gen = pd.DataFrame()  if generated  is None else generated
    return EntityContext(entity=entity, preexisting=pre, N=N, generated=gen)


# ===========================================================================
# Construction
# ===========================================================================

class TestConstruction:

    def test_attributes_stored_correctly(self):
        ctx = make_ctx(N=7)
        assert ctx.entity      is WithPK
        assert ctx.N           == 7
        assert len(ctx.preexisting) == 3

    def test_generated_defaults_to_empty_dataframe(self):
        ctx = EntityContext(entity=WithPK, preexisting=PRE_ROWS.copy(), N=3)
        assert isinstance(ctx.generated, pd.DataFrame)
        assert ctx.generated.empty

    def test_empty_preexisting_is_accepted(self):
        ctx = make_ctx(preexisting=pd.DataFrame())
        assert ctx.preexisting.empty


# ===========================================================================
# get_primary_key_values
# ===========================================================================

class TestGetPrimaryKeyValues:

    # --- entity has a PK field ---

    def test_returns_preexisting_pk_values(self):
        ctx  = make_ctx(generated=pd.DataFrame())
        vals = ctx.get_primary_key_values()
        assert set(vals) == set(PRE_IDS)

    def test_returns_generated_pk_values(self):
        ctx  = make_ctx(preexisting=pd.DataFrame(), generated=GEN_ROWS.copy())
        vals = ctx.get_primary_key_values()
        assert set(vals) == set(GEN_IDS)

    def test_returns_combined_pk_values(self):
        ctx  = make_ctx(generated=GEN_ROWS.copy())
        vals = ctx.get_primary_key_values()
        assert set(vals) == set(PRE_IDS) | set(GEN_IDS)

    def test_returns_series(self):
        ctx  = make_ctx()
        vals = ctx.get_primary_key_values()
        assert isinstance(vals, pd.Series)

    def test_index_is_reset(self):
        ctx  = make_ctx(generated=GEN_ROWS.copy())
        vals = ctx.get_primary_key_values()
        assert list(vals.index) == list(range(len(vals)))

    def test_no_nans_in_result(self):
        ctx  = make_ctx(generated=GEN_ROWS.copy())
        vals = ctx.get_primary_key_values()
        assert not vals.isna().any()

    # --- entity has no PK field ---

    def test_returns_empty_series_when_no_pk_field(self):
        ctx  = make_ctx(entity=NoPKNoCT, preexisting=pd.DataFrame({"value": [1.0, 2.0]}))
        vals = ctx.get_primary_key_values()
        assert isinstance(vals, pd.Series)
        assert vals.empty

    def test_returns_empty_series_when_both_dfs_empty(self): 
        ctx  = make_ctx(preexisting=pd.DataFrame(), generated=pd.DataFrame())
        vals = ctx.get_primary_key_values()
        assert vals.empty


# ===========================================================================
# get_creation_time_values
# ===========================================================================

class TestGetCreationTimeValues:

    # --- entity has a CreationTime field ---

    def test_returns_preexisting_ct_values(self):
        ctx  = make_ctx(entity=WithPKAndCT, preexisting=PRE_TIMED.copy(), generated=pd.DataFrame())
        vals = ctx.get_creation_time_values()
        assert set(vals) == {DT_1, DT_2}

    def test_returns_generated_ct_values(self):
        ctx  = make_ctx(entity=WithPKAndCT, preexisting=pd.DataFrame(), generated=GEN_TIMED.copy())
        vals = ctx.get_creation_time_values()
        assert len(vals) == 1
        assert vals.iloc[0] == datetime.datetime(2023, 1, 1)

    def test_returns_combined_ct_values(self):
        ctx  = make_ctx(entity=WithPKAndCT, preexisting=PRE_TIMED.copy(), generated=GEN_TIMED.copy())
        vals = ctx.get_creation_time_values()
        assert len(vals) == 3

    def test_returns_series(self):
        ctx  = make_ctx(entity=WithPKAndCT, preexisting=PRE_TIMED.copy())
        vals = ctx.get_creation_time_values()
        assert isinstance(vals, pd.Series)

    def test_index_is_reset(self):
        ctx  = make_ctx(entity=WithPKAndCT, preexisting=PRE_TIMED.copy(), generated=GEN_TIMED.copy())
        vals = ctx.get_creation_time_values()
        assert list(vals.index) == list(range(len(vals)))

    def test_no_nans_in_result(self):
        ctx  = make_ctx(entity=WithPKAndCT, preexisting=PRE_TIMED.copy(), generated=GEN_TIMED.copy())
        vals = ctx.get_creation_time_values()
        assert not vals.isna().any()

    # --- entity has no CreationTime field ---

    def test_returns_empty_series_when_no_ct_field(self):
        ctx  = make_ctx(entity=WithPK)
        vals = ctx.get_creation_time_values()
        assert isinstance(vals, pd.Series)
        assert vals.empty

    def test_returns_empty_series_when_entity_has_no_pk_or_ct(self):
        ctx  = make_ctx(entity=NoPKNoCT, preexisting=pd.DataFrame({"value": [1.0]}))
        vals = ctx.get_creation_time_values()
        assert vals.empty


# ===========================================================================
# get_data
# ===========================================================================

class TestGetData:

    # --- default (include both preexisting and generated) ---

    def test_returns_all_rows_by_default(self):
        ctx = make_ctx(generated=GEN_ROWS.copy())
        df  = ctx.get_data()
        assert len(df) == len(PRE_ROWS) + len(GEN_ROWS)

    def test_returns_dataframe(self):
        ctx = make_ctx()
        assert isinstance(ctx.get_data(), pd.DataFrame)

    def test_index_is_reset(self):
        ctx = make_ctx(generated=GEN_ROWS.copy())
        df  = ctx.get_data()
        assert list(df.index) == list(range(len(df)))

    def test_columns_match_entity_fields(self):
        ctx  = make_ctx()
        df   = ctx.get_data()
        flds = {f.name for f in WithPK.select()}
        # only columns that exist in the data are expected
        assert set(df.columns).issubset(flds)

    # --- preexisting / generated flags ---

    def test_preexisting_only(self):
        ctx = make_ctx(generated=GEN_ROWS.copy())
        df  = ctx.get_data(preexisting=True, generated=False)
        assert len(df) == len(PRE_ROWS)
        assert set(df["item_id"]) == set(PRE_IDS)

    def test_generated_only(self):
        ctx = make_ctx(generated=GEN_ROWS.copy())
        df  = ctx.get_data(preexisting=False, generated=True)
        assert len(df) == len(GEN_ROWS)
        assert set(df["item_id"]) == set(GEN_IDS)

    def test_both_false_returns_empty(self):
        ctx = make_ctx(generated=GEN_ROWS.copy())
        df  = ctx.get_data(preexisting=False, generated=False)
        assert df.empty

    def test_only_preexisting_no_generated(self):
        ctx = make_ctx(generated=pd.DataFrame())
        df  = ctx.get_data()
        assert len(df) == len(PRE_ROWS)

    def test_only_generated_no_preexisting(self):
        ctx = make_ctx(preexisting=pd.DataFrame(), generated=GEN_ROWS.copy())
        df  = ctx.get_data()
        assert len(df) == len(GEN_ROWS)

    def test_both_empty_returns_empty(self):
        ctx = make_ctx(preexisting=pd.DataFrame(), generated=pd.DataFrame())
        df  = ctx.get_data()
        assert df.empty

    # --- include filter ---

    def test_include_by_field_name(self):
        ctx = make_ctx()
        df  = ctx.get_data(include=["item_id"])
        assert list(df.columns) == ["item_id"]

    def test_include_multiple_field_names(self):
        ctx = make_ctx()
        df  = ctx.get_data(include=["item_id", "label"])
        assert set(df.columns) == {"item_id", "label"}

    def test_include_by_annotation_type(self):
        ctx = make_ctx()
        df  = ctx.get_data(include=[PrimaryKey])
        assert "item_id" in df.columns
        assert "label"   not in df.columns

    def test_include_nonexistent_field_returns_empty_columns(self):
        ctx = make_ctx()
        df  = ctx.get_data(include=["does_not_exist"])
        assert df.empty or len(df.columns) == 0

    # --- exclude filter ---

    def test_exclude_by_field_name(self): # ! FAILED
        ctx = make_ctx()
        df  = ctx.get_data(exclude=["item_id"])
        assert "item_id" not in df.columns
        assert "label"   in     df.columns

    def test_exclude_by_annotation_type(self): # ! FAILED
        ctx = make_ctx()
        df  = ctx.get_data(exclude=[PrimaryKey])
        assert "item_id" not in df.columns
        assert "label"   in     df.columns

    def test_exclude_all_fields_returns_empty_columns(self): # ! FAILED
        ctx = make_ctx()
        df  = ctx.get_data(exclude=["item_id", "label"])
        assert len(df.columns) == 0

    # --- include + exclude combined ---

    def test_include_then_exclude(self): # ! FAILED
        ctx = make_ctx()
        df  = ctx.get_data(include=["item_id", "label"], exclude=["item_id"])
        assert list(df.columns) == ["label"]

    def test_exclude_takes_precedence_over_include(self): # ! FAILED
        ctx = make_ctx()
        df  = ctx.get_data(include=["item_id"], exclude=["item_id"])
        assert "item_id" not in df.columns


# ! FAILED test_exclude_by_field_name - AssertionError: assert 'item_id' not in Index(['item_id', 'label'], dtype='object')
# ! FAILED test_exclude_by_annotation_type - AssertionError: assert 'item_id' not in Index(['item_id', 'label'], dtype='object')
# ! FAILED test_exclude_all_fields_returns_empty_columns - AssertionError: assert 2 == 0
# ! FAILED test_include_then_exclude - AssertionError: assert ['item_id', 'label'] == ['label']
# ! FAILED test_exclude_takes_precedence_over_include - AssertionError: assert 'item_id' not in Index(['item_id'], dtype='object')