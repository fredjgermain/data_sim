import random
import string 
import numpy as np 
import pandas as pd
from scipy.stats import skewnorm


# ! GENERATE numerical values 
def generate_gamma(N:int, seed, skewness:float, scale:float) -> pd.Series:
  rng = np.random.default_rng(seed)
  shape = (2 / skewness) ** 2
  return pd.Series(rng.gamma(shape=shape, scale=scale, size=N))


def generate_poisson(N:int, seed, mean:float) -> pd.Series:
  rng = np.random.default_rng(seed)
  return pd.Series(rng.poisson(lam=mean, size=N))


def generate_exponential(N:int, seed, scale:float) -> pd.Series:
  rng = np.random.default_rng(seed)
  return pd.Series(rng.exponential(scale=scale, size=N))


def generate_normal(N:int, seed, skewness:float, mean:float=0, std:float=1) -> pd.Series:
  rng = np.random.default_rng(seed)
  return pd.Series(skewnorm.rvs(a=skewness, loc=mean, scale=std, size=N, random_state=rng))


def generate_uniform(N:int, seed, min:float, max:float) -> pd.Series:
  rng = np.random.default_rng(seed)
  return pd.Series(rng.uniform(low=min, high=max, size=N))


def generate_categorical(N: int, seed, categories: list, weight: list | None = None) -> pd.Series:
  if weight is not None:
    if len(weight) != len(categories):
      raise ValueError('Weights length must match categories length')
    weight = np.array(weight, dtype=float)
    weight = weight / weight.sum()  # normalize

  rng = np.random.default_rng(seed)
  return pd.Series(rng.choice(categories, size=N, p=weight))



# ! GENERATE basic date values 
def generate_date(seed, start: pd.Series, end: pd.Series) -> pd.Series:
  rng = np.random.default_rng(seed)
  ranges = (end - start).dt.days.clip(lower=0)
  random_days = (rng.random(len(start)) * (ranges + 1)).astype(int)  # fix: was np.random.rand
  return start + pd.to_timedelta(random_days, unit='D')


def generate_time(seed, start: pd.Series, end: pd.Series) -> pd.Series:
  rng = np.random.default_rng(seed)
  ranges_s = (end - start).dt.total_seconds().clip(lower=0)
  random_s = (rng.random(len(start)) * (ranges_s + 1)).astype(int)
  return start + pd.to_timedelta(random_s, unit='s')






def scramble_letters(text:str, prob:float=0.03): 
  shuffled = list(text) 
  random.shuffle(shuffled) 
  
  scrambling_prob = [random.uniform(0, 1) >= prob for _ in range(len(text))] 
  zipped = zip(list(text), shuffled, scrambling_prob) 
  scrambled = [ a if c else b for a,b,c in zipped ] 
  
  res = []
  for a in scrambled: 
    p = random.uniform(0, 1) 
    if p >= prob: 
      res.append(a) 
    elif p >= prob/2: # ! Add a random letter 
      res.append(random.choice(string.ascii_lowercase)) 
  
  return ''.join(res) 


def missing_elements(elements:list, prob:float=0.03): 
  return [c for c in elements if random.uniform(0,1) >= prob ] 

