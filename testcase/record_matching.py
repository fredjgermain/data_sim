import pandas as pd
import numpy as np
import datetime
from dataclasses import dataclass
from typing import Annotated

from data_simulator.annotations.primaries import PrimaryKey
from data_simulator.annotations.validation import Unique
from data_simulator.entity import Entity
from data_simulator.context import EntityContext
from data_simulator.simulator import DataSimulator

from data_simulator.annotations.generator import GenCtx, IGen
from data_simulator.annotations.generator import (
    GenFaker, Transformer, FromForeignKey 
) 
from data_simulator.annotations.primaries import (PrimaryKey, CreationTime, ForeignKey) 
from data_simulator.annotations.fault import Nullify, Duplicate, Misspell, MissingWord 



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

