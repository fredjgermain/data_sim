# tests/test_faults.py

import pytest
import numpy as np
import pandas as pd

from utils.fault import (
    inject_sentinel,
    inject_outliers,
    inject_duplicates,
    inject_missings,
    inject_misspellings,
    inject_missings_words,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def numeric_series():
    return pd.Series([1.0, 2.0, 3.0, 4.0, 5.0])


@pytest.fixture
def string_series():
    return pd.Series(["alpha", "beta", "gamma", "delta", "epsilon"])


# ---------------------------------------------------------------------------
# inject_sentinel
# ---------------------------------------------------------------------------

class TestInjectSentinel:

    @pytest.mark.parametrize("prob", [0.0, 1.0])
    def test_extreme_probabilities(self, numeric_series, prob):
        result = inject_sentinel(numeric_series, seed=42, sentinels=[-999], prob=prob)

        if prob == 0.0:
            assert result.equals(numeric_series)
        else:
            assert (result == -999).all()

    @pytest.mark.parametrize("sentinels", [[-1], [-1, -2, -3]])
    def test_values_are_from_sentinels(self, numeric_series, sentinels):
        result = inject_sentinel(numeric_series, seed=42, sentinels=sentinels, prob=1.0)
        assert set(result.unique()).issubset(set(sentinels))


# ---------------------------------------------------------------------------
# inject_outliers
# ---------------------------------------------------------------------------

class TestInjectOutliers:

    @pytest.mark.parametrize("direction", ["both", "up", "down"])
    def test_directionality(self, numeric_series, direction):
        result = inject_outliers(numeric_series, seed=42, prob=1.0, magnitude=1.0, direction=direction)
        diff = result - numeric_series

        if direction == "up":
            assert (diff >= 0).all()
        elif direction == "down":
            assert (diff <= 0).all()
        else:
            assert set(np.sign(diff)) <= {-1, 1}

    def test_zero_prob(self, numeric_series):
        result = inject_outliers(numeric_series, seed=42, prob=0.0)
        assert result.equals(numeric_series)


# ---------------------------------------------------------------------------
# inject_duplicates
# ---------------------------------------------------------------------------

class TestInjectDuplicates:

    @pytest.mark.parametrize("prob", [0.0, 1.0])
    def test_prob_behavior(self, numeric_series, prob):
        result = inject_duplicates(numeric_series, seed=42, prob=prob)

        if prob == 0.0:
            assert result.equals(numeric_series)
        else:
            # All values must come from original pool
            assert set(result).issubset(set(numeric_series))

    def test_no_crash_all_masked(self):
        s = pd.Series([1, 1, 1])
        result = inject_duplicates(s, seed=42, prob=1.0)
        assert len(result) == len(s)


# ---------------------------------------------------------------------------
# inject_missings
# ---------------------------------------------------------------------------

class TestInjectMissings:

    @pytest.mark.parametrize("prob, expected_nulls", [
        (0.0, 0),
        (1.0, 5),
    ])
    def test_missing_counts(self, numeric_series, prob, expected_nulls):
        result = inject_missings(numeric_series, seed=42, prob=prob)
        assert result.isna().sum() == expected_nulls


# ---------------------------------------------------------------------------
# inject_misspellings
# ---------------------------------------------------------------------------

class TestInjectMisspellings:

    @pytest.mark.parametrize("prob", [0.0, 0.5])
    def test_output_type_and_length(self, string_series, prob):
        result = inject_misspellings(string_series, seed=42, prob=prob)

        assert isinstance(result, pd.Series)
        assert len(result) == len(string_series)

    def test_prob_zero_identity(self, string_series):
        result = inject_misspellings(string_series, seed=42, prob=0.0)
        # With prob=0, only shuffle logic applies but "keep" mask should preserve original
        assert result.equals(string_series)


# ---------------------------------------------------------------------------
# inject_missings_words
# ---------------------------------------------------------------------------

class TestInjectMissingWords:

    @pytest.mark.parametrize("prob", [0.0, 1.0])
    def test_word_dropping(self, prob):
        s = pd.Series(["one two three", "four five six"])
        result = inject_missings_words(s, seed=42, prob=prob)

        if prob == 0.0:
            assert result.equals(s)
        else:
            # All words dropped → empty strings
            assert (result == "").all()

    def test_partial_drop(self):
        s = pd.Series(["a b c d"])
        result = inject_missings_words(s, seed=42, prob=0.5)

        # Result should have <= original word count
        original_words = len(s.iloc[0].split())
        new_words = len(result.iloc[0].split()) if result.iloc[0] else 0

        assert new_words <= original_words