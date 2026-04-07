import datetime
import pandas as pd
from dataclasses import dataclass
from typing import Annotated

from src.annotations.primaries import PrimaryKey
from src.annotations.validation import Unique
from src.entity import Entity
from src.context import EntityContext
from src.simulator import DataSimulator

from src.annotations.standardgen import (
    GenNormal, GenUniform, GenFaker, GenPattern, CustomGen, GenCategorical, 
)
from src.annotations.primaries import (PrimaryKey, CreationTime, ForeignKey, PkCtx)
from src.annotations.fault import Nullify, Duplicate



@dataclass
class Region(Entity):
    region_id:  Annotated[int, PrimaryKey()]
    founded_at: Annotated[datetime.datetime, CreationTime(
                    start=datetime.datetime(1998, 1, 1),
                    end=datetime.datetime(2002, 1, 1),
                )]
    name:       Annotated[str,  GenFaker("city")]
    code:       Annotated[str,  GenPattern(r'[A-Z]{2}-\d{3}'), Unique()]

df_region_pre = pd.DataFrame({
    "region_id":  [1, 2],
    "founded_at": [datetime.datetime(2000, 6, 1), datetime.datetime(2001, 3, 15)],
    "name":       ["North", "South"],
    "code":       ["NA-001", "SA-002"],
})


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------

ctx = EntityContext(Region, N=8, preexisting=df_region_pre)

pk_ctx = PkCtx.make_ctx(ctx) 

print(all(pk_ctx.pk_values == df_region_pre['region_id']) )

df_test = df_region_pre[['region_id']].copy() 
#df_test.columns = ['region_id', 'test1', 'test2'] 

pk_serie = df_region_pre["region_id"] 
serie = pd.Series() 
serie.name = 'test_column' 
print(f"is serie empty ? {serie.name} {serie.empty} ") 


df = pd.DataFrame(columns=['var1', 'var2']) 
print(df.columns) 

