import pandas as pd
import numpy as np

def sample_from_dict(d: dict, n: int) -> pd.Series:
    """
    Returns a pd.Series of N keys randomly chosen from a dictionary 
    where values represent probabilities.
    
    Example:
        d = {0: 0.2, 1: 0.4, 2: 0.3, 3: 0.1}
        sample_from_dict(d, 10) -> pd.Series([1, 2, 0, 1, 1, 3, 2, 0, 2, 1])
    """
    if not d:
        return pd.Series(dtype='object')
        
    keys = list(d.keys())
    weights = list(d.values())
    
    # Normalize weights to ensure they sum to 1.0
    total_weight = sum(weights)
    if total_weight == 0:
        # If all weights are zero, sample uniformly
        weights = [1.0 / len(keys)] * len(keys)
    else:
        weights = [w / total_weight for w in weights]
        
    samples = np.random.choice(keys, size=n, p=weights)
    return pd.Series(samples)
