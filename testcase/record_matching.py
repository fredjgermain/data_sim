import pandas as pd
import numpy as np
import datetime
from dataclasses import dataclass
from typing import Annotated

from src.annotations.primaries import PrimaryKey
from src.annotations.validation import Unique
from src.entity import Entity
from src.context import EntityContext
from src.simulator import DataSimulator

from src.annotations.base import GenCtx
from src.annotations.standardgen import (
    GenNormal, GenUniform, GenFaker, GenPattern, CustomGen, GenCategorical, GenGamma, GenPoisson, Transformer, IStandardGen
) 
from src.annotations.primaries import (PrimaryKey, CreationTime, ForeignKey) 
from src.annotations.fault import Nullify, Duplicate, Scramble, MissingWord
from src.utils import missing_elements



# def teacher_fetch_school_name(ctx:GenCtx) -> pd.Series: 
#   cdata = ctx.current_data[['school_id']] 
#   fdata = ctx.foreign_datas[School][['school_id', 'name']] 
#   return pd.merge(cdata, fdata, left_on='school_id', right_on='school_id', how='left')['name'] 

# def student_fetch_teacher_name(ctx:GenCtx) -> pd.Series: 
#   cdata = ctx.current_data[['teacher_id']] 
#   fdata = ctx.foreign_datas[Teacher][['teacher_id', 'name']] 
#   return pd.merge(cdata, fdata, left_on='teacher_id', right_on='teacher_id', how='left')['name'] 

# def student_fetch_school_name(ctx:GenCtx) -> pd.Series: 
#   cdata = ctx.current_data[['teacher_id']] 
#   fdata = ctx.foreign_datas[Teacher][['teacher_id', 'school_name']] 
#   return pd.merge(cdata, fdata, left_on='teacher_id', right_on='teacher_id', how='left')['school_name'] 



@dataclass 
class FromForeignKey(IStandardGen): 
  foreignkey:str 
  foreignfield: str 
  
  def generate(self, ctx:GenCtx) -> pd.Series: 
    target = ctx.entity.get(self.foreignkey)[ForeignKey].target 
    target_pk = target.get_primary_key_field() 
    cdata = ctx.current_data[[self.foreignkey]] 
    fdata = ctx.foreign_datas[target][[target_pk.name, self.foreignfield]] 
    merged = pd.merge(cdata, fdata, left_on=self.foreignkey, right_on=target_pk.name, how='left') 
    return merged[self.foreignfield] 



@dataclass 
class School(Entity): 
  school_id:      Annotated[int, PrimaryKey()] 
  name:           Annotated[str, GenFaker('name'), Transformer( lambda serie: [ f"{s} school" for s in serie] )] 


@dataclass 
class Teacher(Entity): 
  teacher_id:     Annotated[int, PrimaryKey()] 
  name:           Annotated[str, GenFaker('name')] 
  school_id:      Annotated[int, ForeignKey(School)] 
  school_name:    Annotated[str, FromForeignKey('school_id', 'name') ] 


@dataclass 
class Student(Entity): 
  student_id:     Annotated[int, PrimaryKey()] 
  name:           Annotated[str, GenFaker('name')] 
  teacher_id:     Annotated[int, ForeignKey(Teacher)] 
  teacher_name:   Annotated[str, FromForeignKey('teacher_id', 'name'), Scramble(0.05)] 
  school_name:    Annotated[str, FromForeignKey('teacher_id', 'school_name'), Scramble(0.05), MissingWord(0.05)] 


entities = {
    School:       EntityContext(School,    pd.DataFrame(),   N=12), 
    Teacher:      EntityContext(Teacher,   pd.DataFrame(),   N=200), 
    Student:      EntityContext(Student,   pd.DataFrame(),   N=5000), 
} 


# Simulation ---------------
sim = DataSimulator(entities) 
results = sim.simulate() 

for e in entities.keys(): 
  print(results[e].head()) 


fld = School.find(PrimaryKey) # intelisence says: (variable) fld: IEntityField | None

ann = fld[PrimaryKey]
print(ann)

