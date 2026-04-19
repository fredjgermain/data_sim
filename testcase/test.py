import pandas as pd
import datetime
from dataclasses import dataclass
from typing import Annotated

from data_simulator.annotations.primaries import PrimaryKey, PkCtx
from data_simulator.entity import Entity
from data_simulator.context import EntityContext
from data_simulator.simulator import DataSimulator

from data_simulator.annotations.generator import (
  GenCtx, GenNormal, GenUniform, GenFaker, GenPattern, CustomGen, GenCategorical, 
)
from data_simulator.annotations.primaries import ( 
  PrimaryKey, CreationTime, ForeignKey
)
from data_simulator.utils import generator
from fault_injection.annotations import Missing



# Fault injection =========================================
from fault_injection.fault_profile import FaultProfile 

a = None
print(bool(a))
if not a:
  print('is empty')


# a = [1]
# print(bool(a))

# if a:
#   print(a)
  
# if not a:

