# import sys
# import os
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

import datetime
import pandas as pd
from dataclasses import dataclass
from typing import Annotated

from data_simulator.annotations.primaries import PrimaryKey, PkCtx
from data_simulator.annotations.validation import Unique
from data_simulator.entity import Entity
from data_simulator.context import EntityContext
from data_simulator.simulator import DataSimulator

from data_simulator.annotations.generator import (
  GenCtx, GenNormal, GenUniform, GenFaker, GenPattern, CustomGen, GenCategorical, 
)
from data_simulator.annotations.primaries import (PrimaryKey, CreationTime, ForeignKey)
from data_simulator.annotations.fault import Nullify, Duplicate
from data_simulator.faultmap import FaultMap
from data_simulator.utils import generator


# ---------------------------------------------------------------------------
# Entity definitions
# ---------------------------------------------------------------------------

@dataclass
class Region(Entity):
    region_id:  Annotated[int, PrimaryKey()]
    founded_at: Annotated[datetime.datetime, CreationTime(
                    start=datetime.datetime(1998, 1, 1),
                    end=datetime.datetime(2002, 1, 1),
                )]
    name:       Annotated[str,  GenFaker("city")]
    code:       Annotated[str,  GenPattern(r'[A-Z]{2}-\d{3}'), Unique()]


# Custom generation function
def customer_id_fn(seed, ctx:PkCtx) -> pd.Series: 
  return generator.generate_ids(seed, ctx.N) 


def age_group(seed, ctx:GenCtx) -> pd.Series: 
  return ctx.current_data['age'].apply( lambda a: "senior" if a >= 65 else "adult" if a >= 30 else "young" ) 


@dataclass
class Customer(Entity):
    customer_id: Annotated[str, PrimaryKey(fn=customer_id_fn)]
    created_at:  Annotated[datetime.datetime, CreationTime(
                     start=datetime.datetime(2015, 1, 1),
                     end=datetime.datetime(2023, 12, 31),
                 )]
    region_id:   Annotated[int,  ForeignKey(Region)]
    region2_id:  Annotated[int,  ForeignKey(Region)]
    email:       Annotated[str,  GenFaker("email"), Unique()]
    sexe:        Annotated[str,  GenCategorical(categories=['male', 'female'])] 
    age:         Annotated[int,  GenNormal(min=18, max=90, mean=40, std=15, rounding=0)]
    code:        Annotated[str,  GenPattern(r'CUST-[A-Z]{3}-\d{4}')]
    age_group:   Annotated[str,  CustomGen(fn=age_group)]


@dataclass
class Transaction(Entity):
    transaction_id: Annotated[int, PrimaryKey()]
    created_at:     Annotated[datetime.datetime, CreationTime(
                        start=datetime.datetime(2015, 1, 1),
                        end=datetime.datetime(2024, 12, 31),
                    )]
    customer_id:    Annotated[str,   ForeignKey(Customer)]
    region_id:      Annotated[int,   ForeignKey(Region)]
    amount:         Annotated[float, GenNormal(min=0, mean=150, std=80, rounding=2)]
    quantity:       Annotated[int,   GenUniform(min=1, max=10, rounding=0)]
    ref:            Annotated[str,   GenPattern(r'TXN-\d{8}'), Unique()]
    # fault injections
    amount_nulls:   Annotated[float, GenNormal(min=0, mean=150, std=80, rounding=2)]
    ref_dupes:      Annotated[str,   GenPattern(r'[A-Z]{3}-\d{4}')]



@dataclass 
class CustomerFaultMap(FaultMap): 
  email: Annotated[str, Nullify(0.05)] 



# ---------------------------------------------------------------------------
# Preexisting data (optional)
# ---------------------------------------------------------------------------

df_region_pre = pd.DataFrame({
    "region_id":  [1, 2],
    "founded_at": [datetime.datetime(2000, 6, 1), datetime.datetime(2001, 3, 15)],
    "name":       ["North", "South"],
    "code":       ["NA-001", "SA-002"],
})


# ---------------------------------------------------------------------------
# Simulation
# ---------------------------------------------------------------------------

entities = {
    #Region:      EntityContext(Region,      preexisting=df_region_pre, N=8),
    Region:      EntityContext(Region,      N=8),
    Customer:    EntityContext(Customer,    N=200),
    Transaction: EntityContext(Transaction, N=1000),
}

fault_maps = { Customer:CustomerFaultMap } 


sim = DataSimulator(entities) 
try:
  sim.simulate() 
  sim.fault_injection(fault_maps) 
  sim._report.failures() 
  print(sim.get_summary()) 
except:
  print(sim.get_failures()) 

gens = sim.get_data(preexisting=False)


for e, data in gens.items():
  print(f'=== {e.__name__} === {data.shape}') 
  print(data.head()) 
