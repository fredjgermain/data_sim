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
from src.annotations.primaries import (PrimaryKey, CreationTime, ForeignKey)
from src.annotations.fault import Nullify, Duplicate


# ---------------------------------------------------------------------------
# Entity definitions
# ---------------------------------------------------------------------------

@dataclass
class Region(Entity):
    region_id:  Annotated[int, PrimaryKey()]
    created_at: Annotated[datetime.datetime, CreationTime(
                    start=datetime.datetime(1998, 1, 1),
                    end=datetime.datetime(2002, 1, 1),
                )]
    name:       Annotated[str,  GenFaker("city")]
    code:       Annotated[str,  GenPattern(r'[A-Z]{2}-\d{3}'), Unique()]


@dataclass
class Customer(Entity):
    customer_id: Annotated[int, PrimaryKey()]
    created_at:  Annotated[datetime.datetime, CreationTime(
                     start=datetime.datetime(2015, 1, 1),
                     end=datetime.datetime(2023, 12, 31),
                 )]
    email:       Annotated[str,  GenFaker("email"), Unique()]
    sexe:        Annotated[int,  GenCategorical(encoding={0:'male', 1:'female'})] 
    age:         Annotated[int,  GenNormal(min=18, max=90, mean=40, std=15, rounding=0)]
    code:        Annotated[str,  GenPattern(r'CUST-[A-Z]{3}-\d{4}')]
    segment:     Annotated[str,  CustomGen(
                     lambda ctx: ctx.current_data["age"].apply(
                         lambda a: "senior" if a >= 65 else "adult" if a >= 30 else "young"
                     )
                 )]


@dataclass
class Transaction(Entity):
    transaction_id: Annotated[int, PrimaryKey()]
    created_at:     Annotated[datetime.datetime, CreationTime(
                        start=datetime.datetime(2015, 1, 1),
                        end=datetime.datetime(2024, 12, 31),
                    )]
    customer_id:    Annotated[int,   ForeignKey(Customer)]
    region_id:      Annotated[int,   ForeignKey(Region), Nullify(rate=0.1)]
    amount:         Annotated[float, GenNormal(min=0, mean=150, std=80, rounding=2)]
    quantity:       Annotated[int,   GenUniform(min=1, max=10, rounding=0)]
    ref:            Annotated[str,   GenPattern(r'TXN-\d{8}'), Unique(), Duplicate(rate=0.1)]
    # fault injections
    amount_nulls:   Annotated[float, GenNormal(min=0, mean=150, std=80, rounding=2), Nullify(rate=0.03)]
    #ref_dupes:      Annotated[str,   Duplicate(rate=0.02)]


# ---------------------------------------------------------------------------
# Preexisting data (optional)
# ---------------------------------------------------------------------------

df_region_pre = pd.DataFrame({
    "region_id":  [1, 2],
    "created_at": [datetime.datetime(2000, 6, 1), datetime.datetime(2001, 3, 15)],
    "name":       ["North", "South"],
    "code":       ["NA-001", "SA-002"],
})


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------

entities = {
    Region:      EntityContext(Region,      df_region_pre,        N=8),
    Customer:    EntityContext(Customer,    pd.DataFrame(),        N=200),
    Transaction: EntityContext(Transaction, pd.DataFrame(),        N=1000),
}

sim = DataSimulator(entities)
results = sim.simulate()

for k, rep in sim.report.items():
    print(f"=== {k} ===") 
    for fld, frep in rep.fld_report.items(): 
        print(fld, [ (a,len(s)) for a, s in frep.results.items()]) 
    print()

# ---------------------------------------------------------------------------
# Inspect results
# ---------------------------------------------------------------------------

print("=== Region ===")
print(results[Region].head())
print(results[Region].shape)


print("=== Customer ===")
print(results[Customer].head())
print(results[Customer].shape)


print("=== Transaction ===")
print(results[Transaction].head())
print(results[Transaction].shape)
dup = results[Transaction].duplicated(subset=['ref'], keep=False)
print(any(dup))
print(results[Transaction][dup].sort_values(by=['ref']))

print(f"Null amounts: {results[Transaction]['amount_nulls'].isna().sum()}")
#print(f"Duplicate refs: {results[Transaction]['ref_dupes'].duplicated().sum()}")


