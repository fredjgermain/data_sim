# tests/test_generator.py

import pytest
import numpy as np
import pandas as pd

from utils.generator import (
    generate_gamma,
    generate_poisson,
    generate_exponential,
    generate_normal,
    generate_uniform,
    generate_categorical,
    generate_date,
    generate_time,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def approx_equal(a, b, tol):
    return abs(a - b) < tol


# ---------------------------------------------------------------------------
# Numerical generators
# ---------------------------------------------------------------------------

class TestNumericalGenerators:

    @pytest.mark.parametrize("func, kwargs", [
        (generate_gamma,       {"skewness": 1.0, "scale": 2.0}),
        (generate_poisson,     {"mean": 5.0}),
        (generate_exponential, {"scale": 2.0}),
        (generate_normal,      {"skewness": 0.0, "mean": 0.0, "std": 1.0}),
        (generate_uniform,     {"min": 0.0, "max": 10.0}),
    ])
    def test_length_and_type(self, func, kwargs):
        N = 100
        result = func(N=N, seed=42, **kwargs)

        assert isinstance(result, pd.Series)
        assert len(result) == N

    @pytest.mark.parametrize("func, kwargs", [
        (generate_gamma,       {"skewness": 1.0, "scale": 2.0}),
        (generate_poisson,     {"mean": 5.0}),
        (generate_exponential, {"scale": 2.0}),
        (generate_normal,      {"skewness": 2.0, "mean": 10.0, "std": 3.0}),
        (generate_uniform,     {"min": -5.0, "max": 5.0}),
    ])
    def test_reproducibility(self, func, kwargs):
        s1 = func(N=100, seed=42, **kwargs)
        s2 = func(N=100, seed=42, **kwargs)

        assert s1.equals(s2)

    @pytest.mark.parametrize("func, kwargs", [
        (generate_gamma,       {"skewness": 1.0, "scale": 2.0}),
        (generate_poisson,     {"mean": 5.0}),
        (generate_exponential, {"scale": 2.0}),
    ])
    def test_non_negative(self, func, kwargs):
        result = func(N=100, seed=42, **kwargs)
        assert (result >= 0).all()


# ---------------------------------------------------------------------------
# Distribution-specific checks
# ---------------------------------------------------------------------------

class TestDistributionProperties:

    def test_uniform_bounds(self):
        result = generate_uniform(N=1000, seed=42, min=1.0, max=3.0)
        assert (result >= 1.0).all()
        assert (result <= 3.0).all()

    def test_poisson_mean(self):
        mean = 5.0
        result = generate_poisson(N=5000, seed=42, mean=mean)
        assert approx_equal(result.mean(), mean, tol=0.5)

    def test_exponential_mean(self):
        scale = 2.0
        result = generate_exponential(N=5000, seed=42, scale=scale)
        assert approx_equal(result.mean(), scale, tol=0.3)

    def test_normal_mean(self):
        result = generate_normal(N=5000, seed=42, skewness=0.0, mean=10.0, std=2.0)
        assert approx_equal(result.mean(), 10.0, tol=0.3)


# ---------------------------------------------------------------------------
# Categorical generator
# ---------------------------------------------------------------------------

class TestCategorical:

    @pytest.mark.parametrize("categories, weights", [
        (["A", "B"], None),
        (["A", "B"], [0.7, 0.3]),
    ])
    def test_basic_properties(self, categories, weights):
        result = generate_categorical(N=100, seed=42, categories=categories, weight=weights)

        assert isinstance(result, pd.Series)
        assert set(result.unique()).issubset(set(categories))

    def test_weight_normalization(self):
        categories = ["A", "B"]
        weights = [7, 3]  # not normalized

        result = generate_categorical(N=5000, seed=42, categories=categories, weight=weights)

        freq = result.value_counts(normalize=True)
        assert approx_equal(freq["A"], 0.7, tol=0.05)

    def test_invalid_weights(self):
        with pytest.raises(ValueError):
            generate_categorical(N=10, seed=42, categories=["A", "B"], weight=[1.0])


# ---------------------------------------------------------------------------
# Date generator
# ---------------------------------------------------------------------------

class TestDateGeneration:

    @pytest.mark.parametrize("start, end", [
        (
            pd.Series(pd.to_datetime(["2020-01-01"] * 100)),
            pd.Series(pd.to_datetime(["2020-01-10"] * 100)),
        ),
        (
            pd.Series(pd.to_datetime(["2020-01-10"] * 100)),
            pd.Series(pd.to_datetime(["2020-01-01"] * 100)),  # reversed
        ),
    ])
    def test_date_bounds(self, start, end):
        result = generate_date(seed=42, start=start, end=end)

        lower = pd.concat([start, end], axis=1).min(axis=1)
        upper = pd.concat([start, end], axis=1).max(axis=1)

        assert ((result >= lower) & (result <= upper)).all()

    def test_reproducibility(self):
        start = pd.Series(pd.to_datetime(["2020-01-01"] * 50))
        end = pd.Series(pd.to_datetime(["2020-01-10"] * 50))

        r1 = generate_date(seed=42, start=start, end=end)
        r2 = generate_date(seed=42, start=start, end=end)

        assert r1.equals(r2)


# ---------------------------------------------------------------------------
# Time generator
# ---------------------------------------------------------------------------

class TestTimeGeneration:

    def test_time_bounds(self):
        start = pd.Series(pd.to_datetime(["2020-01-01 00:00:00"] * 100))
        end = pd.Series(pd.to_datetime(["2020-01-01 01:00:00"] * 100))

        result = generate_time(seed=42, start=start, end=end)

        assert ((result >= start) & (result <= end)).all()

    def test_reproducibility(self):
        start = pd.Series(pd.to_datetime(["2020-01-01"] * 50))
        end = pd.Series(pd.to_datetime(["2020-01-02"] * 50))

        r1 = generate_time(seed=42, start=start, end=end)
        r2 = generate_time(seed=42, start=start, end=end)

        assert r1.equals(r2)