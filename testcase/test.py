import pandas as pd
from datetime import datetime
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
  'high_card':        list(range(10)),
  'monotonic':        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 
  'non_monotonic':    [1, 2, 3, 4, 6, 5, 7, 8, 8, 10], # 5, 8
  'no_outlier':       [1, 1, 2, 3, 4, 1, 2, 3, 4, 2], 
  'outlier':          [-1000, 1, 2, 3, 4, 1, 2, 3, 4, 1000], 
  'out_of_range':     [-10, 2, 3, 4, 5, 6, 7, 8, 9, 40], 
  'in_range':         [1, 2, 3, 4, 5, 6, 7, 8, 9, 10],
  'str_len':          'ab bc kk ia uq ja iiij po u u'.split(),
  'category_valid':   ['A', 'B', 'A', 'C', 'B', 'A', 'C', 'B', 'A', 'C'],
  'category_invalid': ['A', 'B', 'A', 'D', 'B', 'A', 'E', 'B', 'A', 'C'], # 3, 6 
  'email_valid':      ['a@b.com', 'test@example.org'] + ['valid@test.com'] * 8,
  'email_invalid':    ['invalid', 'a@b.com', 'test@', '@example.com'] + ['ok@test.com'] * 6,
  'dates_valid':      [datetime(2020, 1, 1), datetime(2021, 6, 15), datetime(2022, 12, 31), 
                       datetime(2023, 5, 20), datetime(2020, 1, 1), datetime(2021, 6, 15), 
                       datetime(2022, 12, 31), datetime(2023, 5, 20), datetime(2022, 12, 31), datetime(2023, 5, 20)], 
})


from data_validation.utils import ( 
  uniqueness_validity, completeness_validity, cardinality_validity, monotonic_validity, temporal_validity, 
  range_validity
)


#pct, invalid, success = monotonic_validity(data['monotonic'])
#print(pct, invalid, success)

serie = data['incomplete'] 

min, max = None, 6
percent, invalid_values, success = range_validity(serie, min, max)
print(percent)
print(invalid_values)


serie = data['dates_valid']


percent, invalid_values, success = temporal_validity(serie, min_date=datetime(2021, 1, 1))
print(percent)
print(invalid_values)

