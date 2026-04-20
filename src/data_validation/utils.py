
import pandas as pd
import re
from datetime import datetime, timedelta



def uniqueness_validity(serie: pd.Series, tolerance: float = 0.0) -> tuple[float, pd.Series, bool]:
    # Find all duplicate values (keep=False marks all occurrences as duplicates)
    duplicates_mask = serie.duplicated(keep=False) 
    duplicates = serie[duplicates_mask] 
    
    # Percent of non-unique values 
    percent = 0.0 if len(serie) == 0 else len(duplicates) / len(serie) 
    
    # Check if within tolerance
    passes = percent <= tolerance
    
    return percent, duplicates, passes



def completeness_validity(serie: pd.Series, count_as_missing: list = [], tolerance: float = 0.0) -> tuple[float, pd.Series, bool]:
    # Start with standard missing values (NaN, None, NaT)
    missing_mask = serie.isna() | serie.isin(count_as_missing) 
    missing_values = serie[missing_mask] 
    
    # Calculate percentage of missing values
    percent = 0.0 if len(serie) == 0 else len(missing_values) / len(serie) 
    
    # Check if within tolerance (strictly less than)
    passes = percent <= tolerance
   
    return percent, missing_values, passes



def range_validity(serie: pd.Series, min: float = None, max: float = None) -> tuple[float, pd.Series, bool]:
  
  valid = ((min == None) | (serie >= min)) & ((max == None) | (serie <= max)) 
  out_of_range = serie[~valid] 
  
  percent = 0.0 if len(serie) == 0 else len(out_of_range) / len(serie)
  passes = percent == 0
  return percent, out_of_range, passes 



def temporal_validity(serie: pd.Series, min_date: datetime = None, max_date: datetime = None) -> tuple[float, pd.Series, bool]:
  
  serie_clean = serie_clean = serie.dropna().astype('int64') 
  min = pd.Timestamp(min_date).value if min_date else None 
  max = pd.Timestamp(max_date).value if max_date else None 
  
  percent, invalid, passes = range_validity(serie_clean, min, max) 
  invalid = serie[invalid.index] 
  return percent, invalid, passes  



def string_length_validity(serie: pd.Series, min_length: int = 0, max_length: int = None) -> tuple[float, pd.Series, bool]:
    # Get non-null values
    serie_clean = serie.dropna() 
    
    # Convert to string and get lengths
    lengths = serie_clean.astype(str).str.len() 
    percent, invalid, passes = range_validity(lengths, min_length, max_length) 
    invalid = serie[invalid.index] 
    return percent, invalid, passes  



def categorical_validity(serie: pd.Series, categories: list) -> tuple[float, pd.Series, bool]:
    # Find values not in the category list (excluding NaN)
    out_of_category_mask = ~serie.isin(categories) & serie.notna()
    out_of_category = serie[out_of_category_mask]
    
    # Calculate percentage of out-of-category values
    valid_count = serie.notna().sum()
    percent = 0.0 if valid_count == 0 else len(out_of_category) / valid_count
    
    # Check if valid (no out-of-category values)
    passes = percent == 0.0
    
    return percent, out_of_category, passes



def monotonic_validity(serie: pd.Series, increasing:bool = True, strict:bool = False) -> tuple[float, pd.Series, bool]: 
  serie_clean = serie.dropna()
  diff = serie_clean.diff().dropna()
  
  is_mono = (diff >= 0) | pd.isnull(diff) if increasing else (diff <= 0) | pd.isnull(diff)
  is_strict = (diff != 0) | pd.isnull(diff) if strict else pd.Series() 
  invalid = serie.loc[diff[~is_mono | ~is_strict].index] 
  
  percent = len(invalid) / len(serie) 
  passes = percent == 0 
  
  return percent, invalid, passes 



def outlier_validity(
    serie: pd.Series, 
    multiplier: float = 1.5,  # Standard is 1.5, use 3.0 for "extreme" outliers
    tolerance: float = 0.05     # Max acceptable % of outliers
) -> tuple[float, pd.Series, bool]:
    # Remove NaN values for calculation
    serie_clean = serie.dropna()
    
    if len(serie_clean) == 0:
        return 0.0, pd.Series(dtype=serie.dtype), True
    
    # Calculate Q1, Q3, and IQR
    Q1 = serie_clean.quantile(0.25)
    Q3 = serie_clean.quantile(0.75)
    IQR = Q3 - Q1
    
    # Define outlier bounds
    lower_bound = Q1 - multiplier * IQR
    upper_bound = Q3 + multiplier * IQR
    
    # Find outliers (values outside bounds)
    outlier_mask = (serie < lower_bound) | (serie > upper_bound)
    outliers = serie[outlier_mask]
    
    # Calculate percentage of outliers
    percent = len(outliers) / len(serie_clean)
    
    # Check if within tolerance
    passes = percent <= tolerance
    
    return percent, outliers, passes



def format_validity(serie: pd.Series, pattern: str) -> tuple[float, pd.Series, bool]:
    # Compile regex pattern
    regex = re.compile(pattern)
    
    # Get non-null values
    serie_clean = serie.dropna()
    
    if len(serie_clean) == 0:
        return 0.0, pd.Series(dtype=serie.dtype), True
    
    # Convert to string and check pattern
    invalid_mask = ~serie_clean.astype(str).apply(lambda x: bool(regex.match(x)))
    invalid_values = serie_clean[invalid_mask]
    
    # Calculate percentage
    percent = len(invalid_values) / len(serie_clean)
    
    # Check if valid
    passes = percent == 0
    
    return percent, invalid_values, passes



def datatype_validity(serie: pd.Series, expected_dtype: str) -> tuple[float, pd.Series, bool]:
    # Get non-null values
    serie_clean = serie.dropna()
    
    if len(serie_clean) == 0:
        return 0.0, pd.Series(dtype=serie.dtype), True
    
    invalid_indices = []
    
    # Try to convert each value to the expected type
    for idx, value in serie_clean.items():
        try:
            if expected_dtype == 'int':
                int(value)
            elif expected_dtype == 'float':
                float(value)
            elif expected_dtype == 'datetime':
                pd.to_datetime(value)
            elif expected_dtype == 'bool':
                if str(value).lower() not in ['true', 'false', '1', '0', 'yes', 'no', 't', 'f']:
                    raise ValueError()
            elif expected_dtype == 'string':
                str(value)
            else:
                raise ValueError(f"Unsupported dtype: {expected_dtype}")
        except (ValueError, TypeError):
            invalid_indices.append(idx)
    
    # Get invalid values
    invalid_values = serie_clean.loc[invalid_indices]
    
    # Calculate percentage
    percent = len(invalid_values) / len(serie_clean)
    
    # Check if valid
    passes = percent == 0
    
    return percent, invalid_values, passes



def cardinality_validity(serie: pd.Series, min_unique: int = None, max_unique: int = None) -> tuple[float, pd.Series, bool]:
    # Get non-null values
    serie_clean = serie.dropna()
    
    if len(serie_clean) == 0:
        return 0.0, pd.Series(dtype=serie.dtype), True
    
    # Count unique values
    unique_values = serie_clean.unique()
    n_unique = len(unique_values)
    
    # Check bounds
    min_bound = min_unique if min_unique is not None else 0
    max_bound = max_unique if max_unique is not None else float('inf')
    
    passes = min_bound <= n_unique <= max_bound
    
    # Calculate deviation percentage
    if not passes:
        if n_unique < min_bound:
            pct_deviation = (min_bound - n_unique) / min_bound
        else:  # n_unique > max_bound
            pct_deviation = (n_unique - max_bound) / max_bound
    else:
        pct_deviation = 0.0
    
    # Return unique values as a series for inspection
    unique_series = pd.Series(unique_values)
    
    return pct_deviation, unique_series, passes


  
  

