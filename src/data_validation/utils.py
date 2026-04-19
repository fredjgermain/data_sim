
import pandas as pd



def uniqueness(serie: pd.Series, tolerance: float = 0.0) -> tuple[float, pd.Series, bool]:
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



def completeness(serie: pd.Series, count_as_missing: list = [], tolerance: float = 0.0) -> tuple[float, pd.Series, bool]:
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
  
  
  
def outlier_detection_iqr(
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