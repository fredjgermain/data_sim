
import pytest
import pandas as pd 
from typing import Annotated
from dataclasses import dataclass


from src.context import EntityContext
from src.interface import IEntity, IEntityContext
from src.entity import Entity, EntityField
from src.annotations.primaries import PrimaryKey, CreationTime, PkCtx, FkCtx, CtCtx



@dataclass
class SequentialKey(Entity): 
    id:         Annotated[int,              PrimaryKey()] 

ctx_with_pre = EntityContext(SequentialKey, preexisting=pd.DataFrame({'id':[1,2,3]}), N=10) 
ctx_no_pre = EntityContext(SequentialKey, N=10) 


class TestPk_Make_Ctx:
  @pytest.mark.parametrize('name, current_ctx, expected', [ 
    ('id', ctx_with_pre, ctx_with_pre.preexisting['id']), 
    ('id', ctx_no_pre, pd.Series()), 
  ])
  def test_pk_make_ctx(self, name, current_ctx:IEntityContext, expected): 
    pk_ctx = PkCtx.make_ctx(current_ctx) 
    
    assert pk_ctx.name == name 
    assert pk_ctx.entity == current_ctx.entity 
    assert pk_ctx.N == current_ctx.N 
    assert all(pk_ctx.pk_values == expected) 
    