import random
import string 
import numpy as np 
import pandas as pd
from scipy.stats import skewnorm
from typing import Literal




def inject_sentinel(
    serie: pd.Series,
    seed,
    sentinels: list,
    prob: float = 0.03,
) -> pd.Series:
    rng = np.random.default_rng(seed)
    serie = serie.copy()

    mask = rng.random(size=len(serie)) < prob
    replacements = rng.choice(sentinels, size=mask.sum())
    serie[mask] = replacements

    return serie


def inject_outliers(
    serie: pd.Series,
    seed,
    prob: float = 0.03,
    magnitude: float = 3.0,
    direction: Literal['both', 'up', 'down'] = 'both'
) -> pd.Series:
  
    rng = np.random.default_rng(seed)
    serie = serie.copy()
    std = serie.std()
    mask = rng.random(size=len(serie)) < prob
    
    if direction == 'both':
        signs = rng.choice([-1, 1], size=mask.sum())
    elif direction == 'up':
        signs = np.ones(mask.sum())
    else:
        signs = -np.ones(mask.sum())

    serie[mask] += signs * magnitude * std
    return serie



def inject_duplicates(
  serie:pd.Series, 
  seed, 
  prob:float = 0.03
) -> pd.Series:
  
  serie = serie.copy()
  rng = np.random.default_rng(seed)
  mask = pd.Series(rng.random(size=len(serie)) < prob)
  n = mask.sum()
  if n == 0:
      return serie
  pool = serie[~mask]
  if pool.empty:
      return serie
  replacements = pool.sample(n=n, replace=True, random_state=seed).values
  serie[mask] = replacements
  return serie



def inject_missings(
  serie:pd.Series, 
  seed, 
  prob:float = 0.03
) -> pd.Series:
  
  serie = serie.copy()
  rng = np.random.default_rng(seed)
  mask = pd.Series(rng.random(size=len(serie)) < prob)
  serie[mask] = None
  return serie



def inject_misspellings(
  serie: pd.Series, 
  seed, 
  prob: float = 0.03
) -> pd.Series:
  rng = np.random.default_rng(seed)
  
  def scramble_letters(text: str, rng: np.random.Generator, prob: float = 0.03) -> str:
    shuffled = list(text)
    rng.shuffle(shuffled)

    scrambling_prob = rng.uniform(0, 1, size=len(text)) >= prob
    zipped = zip(list(text), shuffled, scrambling_prob)
    scrambled = [a if keep else b for a, b, keep in zipped]

    res = []
    for char in scrambled:
      p = rng.uniform(0, 1)
      if p >= prob:
        res.append(char)           # keep original
      elif p >= prob / 2:
        res.append(rng.choice(list(string.ascii_lowercase)))  # replace with random letter
        
    return ''.join(res)
  
  return pd.Series([scramble_letters(v, rng, prob) for v in serie])



def inject_missings_words(
  serie: pd.Series, 
  seed, 
  prob: float = 0.03
) -> pd.Series:
  
  rng = np.random.default_rng(seed)

  def drop_from_string(text: str) -> str:
      words = text.split()
      kept = [w for w in words if rng.uniform(0, 1) >= prob]
      return ' '.join(kept)

  return pd.Series([drop_from_string(v) for v in serie])

