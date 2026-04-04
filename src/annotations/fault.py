import numpy as np
import pandas as pd
from dataclasses import dataclass
from typing import Callable

from src.annotations.base import FaultCtx, IFault 
from src.utils import scramble_letters, missing_elements


@dataclass 
class Nullify(IFault):
    rate: float = 0
    """Marks a field as the primary key of its entity.

    Default strategy: sequential integers starting from max(preexisting) + 1.
    No configuration is needed for the default strategy.
    """

    def inject(self, ctx: FaultCtx) -> pd.Series:
        serie = ctx.current_serie.copy()
        mask = pd.Series(
            np.random.random(size=len(serie)) < self.rate
        )
        serie[mask] = None
        return serie


# ! OutOfRange — inject values outside the declared min/max bounds 

# ! Duplicate — deliberately re-introduce duplicate values after Unique has run

@dataclass
class Corrupt(IFault): 
    func:Callable[[pd.Series, float], pd.Series] 
    rate: float = 0 
    
    def inject(self, ctx: FaultCtx) -> pd.Series:
        return self.func(ctx.current_serie, self.rate)

@dataclass
class Scramble(IFault):
    prob:0.02
    
    def inject(self, ctx: FaultCtx) -> pd.Series: 
        return [ scramble_letters(v) for v in ctx.current_serie ] 

@dataclass
class MissingWord(IFault):
    prob:0.02
    
    def inject(self, ctx:FaultCtx) -> pd.Series: 
        return [ ' '.join(missing_elements(str(v).split(), self.prob)) for v in ctx.current_serie ]


@dataclass
class OutOfRange(IFault):
    """Injects values outside the declared min/max bounds of a field.
    
    Useful for testing ETL pipelines that should reject or flag out-of-range values.
    """
    rate:  float
    min:   float | None = None
    max:   float | None = None
    scale: float = 0.2
    """How far beyond the boundary to sample. 0.2 = up to 20% beyond the bound."""

    def inject(self, ctx: FaultCtx) -> pd.Series:
        serie = ctx.current_serie.copy()
        mask = pd.Series(np.random.random(size=len(serie)) < self.rate)
        n = mask.sum()
        if n == 0:
            return serie

        if self.min is None and self.max is None:
            raise ValueError("OutOfRange requires at least one of min or max to be set.")

        # for each corrupted row randomly pick whether to go below min or above max
        out_values = []
        for _ in range(n):
            side = np.random.choice(
                [s for s in ['low', 'high'] if (s == 'low' and self.min is not None)
                 or (s == 'high' and self.max is not None)]
            )
            if side == 'low':
                span = abs(self.min) * self.scale or self.scale
                out_values.append(self.min - np.random.uniform(0, span))
            else:
                span = abs(self.max) * self.scale or self.scale
                out_values.append(self.max + np.random.uniform(0, span))

        serie[mask] = out_values
        return serie


@dataclass
class Duplicate(IFault):
    """Deliberately re-introduces duplicate values after Unique has run.

    Randomly replaces a fraction of rows with values already present in the
    series, simulating duplicate records that should be caught by deduplication
    logic in downstream pipelines.
    """
    rate: float = 0.05

    def inject(self, ctx: FaultCtx) -> pd.Series:
        serie = ctx.current_serie.copy()
        mask = pd.Series(np.random.random(size=len(serie)) < self.rate)
        n = mask.sum()
        if n == 0:
            return serie
        # sample replacement values from the non-corrupted rows
        pool = serie[~mask]
        if pool.empty:
            return serie
        replacements = pool.sample(n=n, replace=True).values
        serie[mask] = replacements
        return serie