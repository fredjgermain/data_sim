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



data = pd.DataFrame({ 
  'incomplete':       [1, 2, 3, 4, 5, 6, 7, 8, 9, None], 
  'non_unique':       [1, 2, 1, 2, 1, 2, 1, 2, 1, 2], 
  'empty':            [None] * 10, 
  'low_card':         [1, 2] * 5, 
  'monotonic':        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 
  'outlier':          [-1000, 1, 2, 3, 4, 1, 2, 3, 4, 1000], 
  'out_of_range':     [-10, 2, 3, 4, 5, 6, 7, 8, 9, 40], 
  'str_len':          'ab bc kk u ia uq ja iiij po u'.split(), 
})


t = data['incomplete'][pd.isnull(data['incomplete'])]
t2 = data['incomplete'][pd.isnull(data['incomplete'])]

from data_validation.utils import uniqueness_validity, completeness_validity, cardinality_validity


ex_invalid_values = data['monotonic'][[False]*10]
invalid_percent, invalid_values, success = cardinality_validity(data['monotonic'], min_unique=2)
print(invalid_percent, invalid_values, success)


# a = [1]
# print(bool(a))

# if a:
#   print(a)
  
# if not a:

