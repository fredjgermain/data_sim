import numpy as np
import pandas as pd
from dataclasses import dataclass, fields, field
from faker import Faker as FakerLib
import datetime

from typing import Annotated, Any

#from src.entity_annotation import PrimaryKey, CreationTime, Faker, ForeignFields, ForeignKey, Pattern, Unique 
from src.entity import  EntityContext, Entity 
from src.entity_annotation import  PrimaryKey, CreationTime, Faker, ForeignFields, ForeignKey, Pattern, Unique 
from src.entity_common import EntityField 
from src.gen_funcs import generate_creationtime, generate_sequential, generate_dates, aggregate_foreign_data



# ---------------------------------------------------------------------------
# Test-case entities
# ---------------------------------------------------------------------------

@dataclass
class Item(Entity):
    item_id: Annotated[int, PrimaryKey()]
    created_at:  Annotated[datetime.datetime, CreationTime(
                  start=datetime.datetime(2010, 1, 1),
                  end=datetime.datetime(2025, 12, 31),
              )]


@dataclass
class Region(Entity):
    region_id: Annotated[int, PrimaryKey()]
    created_at:  Annotated[datetime.datetime, CreationTime(
                  start=datetime.datetime(2000, 1, 1),
                  end=datetime.datetime(2020, 12, 31),
              )]


@dataclass
class Customer(Entity):
    customer_id: Annotated[int, generate_sequential, PrimaryKey()]
    created_at:  Annotated[datetime.datetime, CreationTime(
                     start=datetime.datetime(2010, 1, 1),
                     end=datetime.datetime(2025, 12, 31),
                 )]
    region_id:   Annotated[int, ForeignKey(Region)]
    email:       Annotated[str, Unique(), Faker("email")]
    code:        Annotated[str, Pattern(r'[A-Z]{3}-\d{4}')]


@dataclass
class Store(Entity):
  store_id: Annotated[int, generate_sequential, PrimaryKey()] 
  created_at:     Annotated[datetime.datetime, CreationTime(
                        start=datetime.datetime(2010, 1, 1),  # transactions can't precede first customers
                        end=datetime.datetime(2015, 6, 30),
                    )]

@dataclass
class Transaction(Entity):
    transaction_id: Annotated[int, generate_sequential, PrimaryKey()]
    created_at:     Annotated[datetime.datetime, CreationTime(
                        start=datetime.datetime(2010, 1, 1),  # transactions can't precede first customers
                        end=datetime.datetime(2025, 12, 31),
                    )]
    customer_id:    Annotated[int, ForeignKey(Customer)]
    store_id:       Annotated[int, ForeignKey(Store)]
    region_id:       Annotated[int, ForeignKey(Region)]


flds = Transaction.get_fields_by_annotation([PrimaryKey, ForeignKey]) 
#print([ f.name for f in flds]) 


# ---------------------------------------------------------------------------
# Simulation bootstrap
# ---------------------------------------------------------------------------

N = 1000
df_item = pd.DataFrame({ 
    'item_id':range(1,N+1) 
}) 



N = 12 
reg_time:CreationTime = Region.get_primary_time_field().annotations[CreationTime] 
region_date_range = pd.date_range(start=reg_time.start, end=reg_time.end, freq='D') 
df_region = pd.DataFrame({ 
    'region_id': range(1, N+1), 
    'created_at': np.random.choice(region_date_range, N) 
}) 


# Preexisting customers — include a created_at column so temporal FK works. 
N = 100 
cus_time:CreationTime = Customer.get_primary_time_field().annotations[CreationTime] 
customer_date_range = pd.date_range(start=cus_time.start, end=cus_time.end, freq='D') 
df_customer_pre = pd.DataFrame({
    'customer_id': range(1, N+1),
    'created_at':  np.random.choice(customer_date_range, N), 
    'region_id':   np.random.choice(df_region['region_id'], size=N),
    'email':       ['preexisting@example.com'] * N,
    'code':        ['PRE-0000'] * N,
})


N = 10
sto_time:CreationTime = Store.get_primary_time_field().annotations[CreationTime] 
store_date_range = pd.date_range(start=sto_time.start, end=sto_time.end, freq='D')
df_store_pre = pd.DataFrame({
  'store_id': range(1,N+1), 
  'created_at':  np.random.choice(store_date_range, N), 
})


N = 100000
tra_time:CreationTime = Transaction.get_primary_time_field().annotations[CreationTime] 
tra_date_range = pd.date_range(start=tra_time.start, end=tra_time.end, freq='D')
df_transaction_pre = pd.DataFrame({ 
  'transaction_id': range(1,N+1), 
  'customer_id': np.random.choice(df_customer_pre['customer_id'], N), 
  'store_id': np.random.choice(df_store_pre['store_id'], N), 
  'region_id': np.random.choice(df_region['region_id'], N), 
  #'created_at':  pd.date_range(start='2022-01-01', periods=N, freq='3D'),
})
