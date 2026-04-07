
import datetime
import pytest
import pandas as pd 
from typing import Annotated
from dataclasses import dataclass


from src.context import EntityContext
from src.interface import IEntity, IEntityContext
from src.entity import Entity, EntityField
from src.annotations.primaries import PrimaryKey, ForeignKey, CreationTime, PkCtx, FkCtx, CtCtx



@dataclass
class Region(Entity): 
    region_id:         Annotated[int,              PrimaryKey()] 


@dataclass
class Customer(Entity): 
    customer_id:       Annotated[int,              PrimaryKey()] 
    region_id:         Annotated[int,              ForeignKey(Region)] 


class Transaction(Entity):
    transaction_id: Annotated[int, PrimaryKey()]
    created_at:     Annotated[datetime.datetime, CreationTime(
                        start=datetime.datetime(2015, 1, 1),
                        end=datetime.datetime(2024, 12, 31),
                    )]


df_region_pre = pd.DataFrame({
  "region_id":  [1, 2]
})

entities = {
    Region:      EntityContext(Region,      preexisting=df_region_pre, N=8), 
    Customer:    EntityContext(Customer,    N=200), 
    Transaction: EntityContext(Transaction, N=1000), 
} 


class Test_Pk_Make_Ctx: 
  @pytest.mark.parametrize('name, current_ctx, expected', [ 
    ('region_id', entities[Region], entities[Region].preexisting['region_id']), 
    ('customer_id', entities[Customer], pd.Series()), 
  ])
  def test_pk_make_ctx(self, name, current_ctx:IEntityContext, expected): 
    pk_ctx = PkCtx.make_ctx(current_ctx) 
    
    assert pk_ctx.name == name 
    assert pk_ctx.entity == current_ctx.entity 
    assert pk_ctx.N == current_ctx.N 
    assert all(pk_ctx.pk_values == expected) 


class Test_Fk_Make_Ctx: 
  @pytest.mark.parametrize('name, current_ctx, expected', [ 
    #(None, entities[Region], pd.Series()), Region has no foreign keys in the first place it should not be called. 
    ('region_id', entities[Customer], entities[Region].get_serie(PrimaryKey)), 
  ])
  def test_pk_make_ctx(self, name, current_ctx:IEntityContext, expected): 
    fk_ctx = FkCtx.make_ctx(name, current_ctx, entities) 
    
    assert fk_ctx.name == name 
    assert fk_ctx.entity == current_ctx.entity 
    assert fk_ctx.N == current_ctx.N 
    assert all(fk_ctx.fk_values == expected) 
    