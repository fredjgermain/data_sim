import datetime
import pandas as pd
from dataclasses import dataclass
from typing import Annotated

from data_simulator.annotations.primaries import PrimaryKey
from data_simulator.annotations.validation import Unique
from data_simulator.entity import Entity
from data_simulator.context import EntityContext
from data_simulator.simulator import DataSimulator

from data_simulator.annotations.generator import (
    GenNormal, GenUniform, GenFaker, GenPattern, CustomGen, GenCategorical, 
)
from data_simulator.annotations.primaries import (PrimaryKey, CreationTime, ForeignKey, PkCtx)
from data_simulator.annotations.fault import Nullify, Duplicate


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


entities = {
    #Region:      EntityContext(Region,      preexisting=df_region_pre, N=8),
    Region:      EntityContext(Region,      N=8),
    Customer:    EntityContext(Customer,    N=200),
    Transaction: EntityContext(Transaction, N=1000),
}

sim = DataSimulator(entities) 
try:
  sim.simulate() 
  sim._report.failures() 
  print(sim.get_summary()) 
except:
  print(sim.get_failures()) 

gens = sim.get_data(preexisting=False) # Collect generated data. 



@dataclass 
class CustomerFaultMap(FaultMap): 
  email: Annotated[str, Nullify(0.05)] # Simulate 5% missing values
  sexe: Annotated[str, Nullify(0.05)] # Simulate 5% missing values



# Validation Profile 
class CustomerValidationProfile(ValidationProfile): 
  # id: Must have 0% missing 
  customer_id:           Annotated[int, Unique(), Missing(0.0)] 
  
  # category Tolerates 1% missings 
  sexe:     Annotated[int, ValidCategory(['male', 'female']), Missing(0.1)] 
  
  # num_field should follow a normal distribution min 0 Tolerates 1% missings, 
  wage:    Annotated[float, ValidDistribution(min=0, mean=150, std=50), Missing(0.1)] 
  
  # age must have a min 18 and max 90, and no missing values
  age:          Annotated[int, ValidUniform(min=18, max=90), Missing(0.0)]






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

serie = pd.Series([1,2,3,4], name='id') 
df = pd.DataFrame({'id':[1,2,3,4], 'name':['a','b','c','d']})
df.index = [12,14,10,18] 

df_merged = pd.merge(serie, df, left_on=serie.name, right_on='id', how='left')

print(type(serie.name))

print(serie)
print(df)
print(df_merged)
