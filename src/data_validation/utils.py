
import pandas as pd
import re
from datetime import datetime, timedelta



def uniqueness_validity(serie: pd.Series, tolerance: float = 0.0) -> tuple[float, pd.Series, bool]:
    # Find all duplicate values (keep=False marks all occurrences as duplicates)
    duplicates_mask = serie.duplicated(keep=False) 
    duplicates = serie[duplicates_mask] 
    
    # Calculate percentage of non-unique values 
    if len(serie) == 0: 
        pct_non_unique = 0.0 
    else: 
        pct_non_unique = len(duplicates) / len(serie) 
    
    # Check if within tolerance
    passes = pct_non_unique <= tolerance
    
    return pct_non_unique, duplicates, passes



def completeness_validity(serie: pd.Series, count_as_missing: list = [], tolerance: float = 0.0) -> tuple[float, pd.Series, bool]:
    # Start with standard missing values (NaN, None, NaT)
    missing_mask = serie.isna()
    
    # Add custom missing values
    if count_as_missing:
        for missing_value in count_as_missing:
            missing_mask = missing_mask | (serie == missing_value)
    
    missing_values = serie[missing_mask]
    
    # Calculate percentage of missing values
    if len(serie) == 0:
        pct_missing = 0.0
    else:
        pct_missing = len(missing_values) / len(serie) 
    
    # Check if within tolerance (strictly less than)
    passes = pct_missing <= tolerance
   
    return pct_missing, missing_values, passes



def range_validity(serie: pd.Series, min: float = None, max: float = None) -> tuple[float, pd.Series, bool]:

    # Use -inf and +inf as defaults when bounds are None
    min_bound = min if min is not None else float('-inf')
    max_bound = max if max is not None else float('inf')
    
    # Find values outside the range (excluding NaN)
    out_of_range_mask = (serie < min_bound) | (serie > max_bound)
    out_of_range = serie[out_of_range_mask]
    
    # Calculate percentage of out-of-range values
    # Note: NaN values are automatically excluded from comparison
    valid_count = serie.notna().sum()
    if valid_count == 0:
        pct_out_of_range = 0.0
    else:
        pct_out_of_range = len(out_of_range) / valid_count 
    
    # Check if valid (no out-of-range values)
    passes = len(out_of_range) == 0
    
    return pct_out_of_range, out_of_range, passes


def categorical_validity(serie: pd.Series, categories: list) -> tuple[float, pd.Series, bool]:
    # Find values not in the category list (excluding NaN)
    out_of_category_mask = ~serie.isin(categories) & serie.notna()
    out_of_category = serie[out_of_category_mask]
    
    # Calculate percentage of out-of-category values
    valid_count = serie.notna().sum()
    if valid_count == 0:
        pct_out_of_category = 0.0
    else:
        pct_out_of_category = len(out_of_category) / valid_count 
    
    # Check if valid (no out-of-category values)
    passes = len(out_of_category) == 0
    
    return pct_out_of_category, out_of_category, passes
  
  
  
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
    valid_count = len(serie_clean)
    pct_outliers = len(outliers) / valid_count 
    
    # Check if within tolerance
    passes = pct_outliers <= tolerance
    
    return pct_outliers, outliers, passes
  

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
    pct_invalid = len(invalid_values) / len(serie_clean)
    
    # Check if valid
    passes = len(invalid_values) == 0
    
    return pct_invalid, invalid_values, passes


def string_length_validity(serie: pd.Series, min_length: int = None, max_length: int = None) -> tuple[float, pd.Series, bool]:
    # Get non-null values
    serie_clean = serie.dropna()
    
    if len(serie_clean) == 0:
        return 0.0, pd.Series(dtype=serie.dtype), True
    
    # Convert to string and get lengths
    lengths = serie_clean.astype(str).str.len()
    
    # Use -inf and +inf as defaults when bounds are None
    min_bound = min_length if min_length is not None else 0
    max_bound = max_length if max_length is not None else float('inf')
    
    # Find values with invalid lengths
    invalid_mask = (lengths < min_bound) | (lengths > max_bound)
    invalid_values = serie_clean[invalid_mask]
    
    # Calculate percentage
    pct_invalid = len(invalid_values) / len(serie_clean)
    
    # Check if valid
    passes = len(invalid_values) == 0
    
    return pct_invalid, invalid_values, passes


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
    pct_invalid = len(invalid_values) / len(serie_clean)
    
    # Check if valid
    passes = len(invalid_values) == 0
    
    return pct_invalid, invalid_values, passes


def temporal_validity(serie: pd.Series, min_date: str = None, max_date: str = None) -> tuple[float, pd.Series, bool]:
    # Get non-null values
    serie_clean = serie.dropna()
    
    if len(serie_clean) == 0:
        return 0.0, pd.Series(dtype=serie.dtype), True
    
    # Convert to datetime
    try:
        dates = pd.to_datetime(serie_clean)
    except:
        # If conversion fails, all values are invalid
        return 1.0, serie_clean, False
    
    # Parse date bounds
    def parse_date(date_str):
        if date_str is None:
            return None
        
        # Handle relative dates
        if date_str.startswith('today'):
            base = pd.Timestamp.now().normalize()
            if len(date_str) > 5:
                # Parse offset like 'today-30d' or 'today+1y'
                offset_str = date_str[5:]  # Get the offset part
                sign = 1 if offset_str[0] == '+' else -1
                value = int(offset_str[1:-1])
                unit = offset_str[-1]
                
                if unit == 'd':
                    return base + sign * timedelta(days=value)
                elif unit == 'w':
                    return base + sign * timedelta(weeks=value)
                elif unit == 'm':
                    return base + sign * pd.DateOffset(months=value)
                elif unit == 'y':
                    return base + sign * pd.DateOffset(years=value)
            return base
        
        # Parse ISO date
        return pd.to_datetime(date_str)
    
    min_bound = parse_date(min_date) if min_date else pd.Timestamp.min
    max_bound = parse_date(max_date) if max_date else pd.Timestamp.max
    
    # Find dates outside range
    invalid_mask = (dates < min_bound) | (dates > max_bound)
    invalid_values = serie_clean[invalid_mask]
    
    # Calculate percentage
    pct_invalid = len(invalid_values) / len(serie_clean)
    
    # Check if valid
    passes = len(invalid_values) == 0
    
    return pct_invalid, invalid_values, passes



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



def monotonic_validity(serie: pd.Series, direction: str) -> tuple[float, pd.Series, bool]:
    # Get non-null values
    serie_clean = serie.dropna()
    
    if len(serie_clean) <= 1:
        return 0.0, pd.Series(dtype=serie.dtype), True
    
    # Calculate differences
    diffs = serie_clean.diff()
    
    # Check based on direction
    if direction == 'increasing':
        violations_mask = diffs <= 0
    elif direction == 'decreasing':
        violations_mask = diffs >= 0
    elif direction == 'non-decreasing':
        violations_mask = diffs < 0
    elif direction == 'non-increasing':
        violations_mask = diffs > 0
    else:
        raise ValueError(f"Invalid direction: {direction}. Must be 'increasing', 'decreasing', 'non-decreasing', or 'non-increasing'")
    
    # Skip first value (diff is NaN)
    violations_mask.iloc[0] = False
    
    # Get violating values
    violations = serie_clean[violations_mask]
    
    # Calculate percentage
    pct_violations = len(violations) / (len(serie_clean) - 1)
    
    # Check if valid
    passes = len(violations) == 0
    
    return pct_violations, violations, passes
