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
    GenNormal, GenUniform, GenFaker, GenPattern, CustomGen, GenCategorical, GenGamma, GenPoisson
) 
from src.annotations.primaries import (PrimaryKey, CreationTime, ForeignKey) 
from src.annotations.fault import Nullify, Duplicate 


def generate_school_name(ctx:GenCtx) -> pd.Series: 
  return pd.Series() 


@dataclass 
class School(Entity): 
  name:   Annotated[str, CustomGen()] 

@dataclass 
class Teacher(Entity): 
  name:   Annotated[str, GenFaker('name')] 
  school_name:    Annotated[str, CustomGen('')] 


@dataclass 
class Student(Entity): 
  name:           Annotated[str, GenFaker('name')] 
  school_name:    Annotated[str, CustomGen('')] 
  teacher_name:   Annotated[str, CustomGen('')] 


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

print(results[e][['complaints_N', 'res_time']].sort_values(by=['complaints_N'])) 