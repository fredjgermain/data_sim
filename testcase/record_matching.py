
import pandas as pd
from dataclasses import dataclass
from typing import Annotated

from data_simulator.annotations.primaries import PrimaryKey, PkCtx
from data_simulator.annotations.validation import Unique
from data_simulator.entity import Entity
from data_simulator.context import EntityContext
from data_simulator.simulator import DataSimulator

from data_simulator.annotations.generator import (
  GenCtx, GenNormal, GenUniform, GenFaker, GenPattern, 
  CustomGen, GenCategorical, Transformer, FromForeignKey
)
from data_simulator.annotations.primaries import (PrimaryKey, CreationTime, ForeignKey)
from data_simulator.annotations.fault import Nullify, Duplicate, Misspell, MissingWord
from data_simulator.faultmap import FaultMap
from data_simulator.utils import generator



@dataclass 
class School(Entity): 
  school_id:      Annotated[int, PrimaryKey()] 
  name:           Annotated[str, GenFaker('name'), Transformer( lambda seed,serie: pd.Series([ f"{s} school" for s in serie]) )] 


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
  teacher_name:   Annotated[str, FromForeignKey('teacher_id', 'name')] 
  school_name:    Annotated[str, FromForeignKey('teacher_id', 'school_name')]  # , 


entities = {
    School:       EntityContext(School,   N=0), 
    Teacher:      EntityContext(Teacher,  N=200), 
    Student:      EntityContext(Student,  N=5000), 
} 


@dataclass 
class StudentFaultMap(FaultMap):
  teacher_name:   Annotated[str, Misspell(0.03)]
  school_name:   Annotated[str, MissingWord(0.1), Misspell(0.03)]



# Simulation ---------------
fault_maps = { Student:StudentFaultMap } 


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

