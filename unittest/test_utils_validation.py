import pytest
import numpy as np
import pandas as pd

#datatype_validity,
from data_validation.utils import ( 
  completeness_validity, uniqueness_validity, cardinality_validity, 
  categorical_validity, format_validity, monotonic_validity, 
  outlier_validity, range_validity, string_length_validity, 
  temporal_validity, 
) 

data = pd.DataFrame({ 
  'incomplete':       [1, 2, 3, 4, 5, 6, 7, 8, 9, None], 
  'non_unique':       [1, 2, 1, 2, 1, 2, 1, 2, 1, 2], 
  'empty':            [None] * 10, 
  'low_card':         [1, 2] * 5, 
  'monotonic':        [1, 2, 3, 4, 5, 6, 7, 8, 9, 10], 
  'non_monotonic':    [1, 2, 3, 4, 6, 5, 7, 8, 9, 10], 
  'no_outlier':       [1, 1, 2, 3, 4, 1, 2, 3, 4, 2], 
  'outlier':          [-1000, 1, 2, 3, 4, 1, 2, 3, 4, 1000], 
  'out_of_range':     [-10, 2, 3, 4, 5, 6, 7, 8, 9, 40], 
  'str_len':          'ab bc kk ia uq ja iiij po u u'.split(), 
})

N, M = data.shape


class TestValidation:
  
  @pytest.mark.parametrize('func, kwargs, expected', [ 
    # ! Completeness 
    (completeness_validity, {'serie':data['incomplete'] }, 
      (1.0/10.0, data['incomplete'][pd.isnull(data['incomplete'])], False) ), 
    (completeness_validity, {'serie':data['monotonic'] }, 
      (0.0, data['monotonic'][[False]*N], True) ), 
    
    # ! Unique ness 
    (uniqueness_validity, {'serie':data['monotonic'] }, 
      (0.0, data['monotonic'][[False]*N], True) ), 
    (uniqueness_validity, {'serie':data['non_unique'] }, 
      (1, data['non_unique'], False) ), 
    
    # ! Outlier 
    (outlier_validity, {'serie':data['no_outlier'] }, 
      (0.0, data['no_outlier'][[False]*N], True) ), 
    (outlier_validity, {'serie':data['outlier'] }, 
      (0.2, data['outlier'][[True, *[False]*8, True] ], False) ), 
    
    # ! Str len 
    (string_length_validity, {'serie':data['str_len'], 'min_length':1, 'max_length':4 }, 
      (0.0, data['str_len'][[False]*N], True) ), 
    (string_length_validity, {'serie':data['str_len'], 'min_length':2, 'max_length':4 }, 
      (0.2, data['str_len'][[*[False]*8, True, True]], False) ), 
  ]) 
  def test_validation(self, func, kwargs, expected): 
    invalid_percent, invalid_values, success = func(**kwargs) 
    ex_invalid_percent, ex_invalid_values, ex_success = expected 
    invalid_values:pd.Series
    assert invalid_percent == ex_invalid_percent 
    assert invalid_values.equals(ex_invalid_values) 
    assert success == ex_success 
    
