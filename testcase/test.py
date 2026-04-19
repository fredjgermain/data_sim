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


@dataclass 
class CustomerFaultProfile(FaultProfile): 
  email: Annotated[str, Missing(0.2)] 

print( CustomerFaultProfile.inspect() ) 

