import pytest
import numpy as np
import pandas as pd
from datetime import datetime

from data_validation.utils import ( 
  completeness_validity, uniqueness_validity, 
  categorical_validity, format_validity, monotonic_validity, 
  outlier_validity, range_validity, string_length_validity, 
  temporal_validity, 
) 

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
  #'dates_invalid':    [pd.Timestamp('2020-01-01')] * 5 + [pd.Timestamp('2019-12-31')] + [pd.Timestamp('2020-01-02')] * 4,
})

N, M = data.shape
EMPTY = [False]*N


class TestValidation:
  
  @pytest.mark.parametrize('func, kwargs, expected', [ 
    # ! Completeness 
    (completeness_validity, {'serie':data['incomplete'] }, 
      (1.0/10.0, data['incomplete'][pd.isnull(data['incomplete'])], False) ), 
    (completeness_validity, {'serie':data['monotonic'] }, 
      (0.0, data['monotonic'][EMPTY], True) ), 
    (completeness_validity, {'serie':data['empty'] }, 
      (1.0, data['empty'], False) ),
    
    # ! Uniqueness 
    (uniqueness_validity, {'serie':data['monotonic'] }, 
      (0.0, data['monotonic'][EMPTY], True) ), 
    (uniqueness_validity, {'serie':data['non_unique'] }, 
      (1.0, data['non_unique'], False) ),
    (uniqueness_validity, {'serie':data['low_card'] }, 
      (1.0, data['low_card'], False) ),
    
    # ! Categorical
    (categorical_validity, {'serie':data['category_valid'], 'categories':['A', 'B', 'C'] }, 
      (0.0, data['category_valid'][EMPTY], True) ),
    (categorical_validity, {'serie':data['category_invalid'], 'categories':['A', 'B', 'C'] }, 
      (0.2, data['category_invalid'].loc[[3,6]], False) ),
    (categorical_validity, {'serie':data['low_card'], 'categories':[1, 2, 3] }, 
      (0.0, data['low_card'][EMPTY], True) ),
    
    # ! Monotonic
    (monotonic_validity, {'serie':data['monotonic'], 'increasing':True }, 
      (0.0, data['monotonic'][EMPTY], True) ), 
    (monotonic_validity, {'serie':data['non_monotonic'], 'increasing':True }, 
      (0.1, data['non_monotonic'].loc[[5]], False) ), 
    (monotonic_validity, {'serie':data['non_monotonic'], 'increasing':True, 'strict':True }, 
      (0.2, data['non_monotonic'].loc[[5, 8]], False) ), 
    
    # ! Outlier 
    (outlier_validity, {'serie':data['no_outlier'] }, 
      (0.0, data['no_outlier'][EMPTY], True) ), 
    (outlier_validity, {'serie':data['outlier'] }, 
      (0.2, data['outlier'][[True, *[False]*8, True]], False) ),
    (outlier_validity, {'serie':data['monotonic'] }, 
      (0.0, data['monotonic'][EMPTY], True) ),
    
    # ! Range
    (range_validity, {'serie':data['in_range'], 'min':None, 'max':15 }, 
      (0.0, data['in_range'][EMPTY], True) ),
    (range_validity, {'serie':data['in_range'], 'min':0, 'max':15 }, 
      (0.0, data['in_range'][EMPTY], True) ),
    (range_validity, {'serie':data['out_of_range'], 'min':0, 'max':15 }, 
      (0.2, data['out_of_range'][[0, 9]], False) ),
    (range_validity, {'serie':data['monotonic'], 'min':1, 'max':10 }, 
      (0.0, data['monotonic'][EMPTY], True) ),
    (range_validity, {'serie':data['monotonic'], 'min':5, 'max':10 }, 
      (0.4, data['monotonic'][[0,1,2,3]], False) ),
  
    # ! String length 
    (string_length_validity, {'serie':data['str_len'], 'min_length':1, 'max_length':4 }, 
      (0.0, data['str_len'][EMPTY], True) ), 
    (string_length_validity, {'serie':data['str_len'], 'min_length':2, 'max_length':4 }, 
      (0.2, data['str_len'][[8, 9]], False) ),
    (string_length_validity, {'serie':data['str_len'], 'min_length':1, 'max_length':2 }, 
      (0.1, data['str_len'][[6]], False) ),
    (string_length_validity, {'serie':data['str_len'], 'min_length':3, 'max_length':10 }, 
      (0.9, data['str_len'][[0, 1, 2, 3, 4, 5, 7, 8, 9]], False) ),
    
    # ! Format (assuming regex pattern validation)
    (format_validity, {'serie':data['email_valid'], 'pattern':r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$' }, 
      (0.0, data['email_valid'][EMPTY], True) ),
    (format_validity, {'serie':data['email_invalid'], 'pattern':r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$' }, 
      (0.3, data['email_invalid'][[0, 2, 3]], False) ),
    
    # ! Temporal validity)
    (temporal_validity, {'serie':data['dates_valid']}, 
      (0.0, data['dates_valid'][EMPTY], True) ), 
    (temporal_validity, {'serie':data['dates_valid'], 'min_date':datetime(2021, 1, 1), 'max_date':datetime(2023, 1, 1)}, 
      (0.5, data['dates_valid'][[0,3,4,7,9]], False) ), 
    (temporal_validity, {'serie':data['dates_valid'], 'min_date':datetime(2021, 1, 1)}, 
      (0.2, data['dates_valid'][[0, 4]], False) ), 
  ]) 
  def test_validation(self, func, kwargs, expected): 
    invalid_percent, invalid_values, success = func(**kwargs) 
    ex_invalid_percent, ex_invalid_values, ex_success = expected 
    invalid_values: pd.Series
    assert invalid_percent == ex_invalid_percent 
    assert success == ex_success
    assert invalid_values.equals(ex_invalid_values) 

   