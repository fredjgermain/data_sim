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

from src.annotations.generator import GenCtx, IGen
from src.annotations.generator import (
    GenNormal, GenUniform, GenFaker, GenPattern, 
    CustomGen, GenCategorical, GenGamma, GenPoisson, 
    Transformer, FromForeignKey 
) 
from src.annotations.primaries import (PrimaryKey, CreationTime, ForeignKey) 
from src.annotations.fault import Nullify, Duplicate, Misspell, MissingWord 



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
  teacher_name:   Annotated[str, FromForeignKey('teacher_id', 'name'), Misspell(0.03)] 
  school_name:    Annotated[str, FromForeignKey('teacher_id', 'school_name'), MissingWord(0.1), Misspell(0.03)]  # , 


entities = {
    School:       EntityContext(School,   N=12), 
    Teacher:      EntityContext(Teacher,  N=200), 
    Student:      EntityContext(Student,  N=5000), 
} 



# Simulation ---------------
sim = DataSimulator(entities) 
results = sim.simulate() 

for e in entities.keys(): 
  print(results[e].head()) 

