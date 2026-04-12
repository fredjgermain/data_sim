
import numpy as np 
import pandas as pd
import uuid
import random  
import rstr
import faker

from scipy.stats import skewnorm 



# ! GERATE WITH FAKER
def generate_with_faker(N:int, seed, provider: str) -> pd.Series:
    fkr = faker.Faker()
    provider_fn = getattr(fkr, provider, None)
    if provider_fn is None:
      raise AttributeError(f"Faker has no provider '{provider}'.")

    if seed is not None:
      state = random.getstate()
      faker.Faker.seed(seed)

    result = pd.Series([provider_fn() for _ in range(N)])

    if seed is not None:
      random.setstate(state)

    return result


# ! GENERATE PATTERN
def generate_pattern(N:int, seed, pattern:str) -> pd.Series:
    if seed is not None:
      state = random.getstate()
      random.seed(seed)
    result = pd.Series([rstr.xeger(pattern) for _ in range(N)])
    
    if seed is not None:
        random.setstate(state)
    return result


# ! GENERATE IDS
def generate_ids(seed: int, N: int) -> pd.Series:
    rng = random.Random(seed)
    
    ids = set()
    while len(ids) < N:
        fake_uuid = uuid.UUID(int=rng.getrandbits(128), version=4)
        ids.add(str(fake_uuid))
    
    return pd.Series(list(ids))

# def generate_ids(N: int, seed: int) -> pd.Series:
#     rng = np.random.default_rng(seed)
#     high = rng.integers(0, 2**64, size=N, dtype=np.uint64)
#     low  = rng.integers(0, 2**64, size=N, dtype=np.uint64)
#     uuids = [uuid.UUID(int=(int(h) << 64) | int(l)) for h, l in zip(high, low)]
#     return pd.Series([str(u) for u in uuids], dtype="string")
  
# def generate_ids(N: int, seed: int) -> pd.Series:
#     rng = np.random.default_rng(seed)
#     uuids = [uuid.UUID(int=rng.integers(0, 2**128).item()) for _ in range(N)]
#     return pd.Series([str(u) for u in uuids], dtype="string")


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
  if start.empty or end.empty:
    return pd.Series()
  rng = np.random.default_rng(seed)
  ranges = (end - start).dt.days.clip(lower=0)
  random_days = (rng.random(len(start)) * (ranges + 1)).astype(int)  # fix: was np.random.rand
  return start + pd.to_timedelta(random_days, unit='D')


def generate_time(seed, start: pd.Series, end: pd.Series) -> pd.Series:
  rng = np.random.default_rng(seed)
  ranges_s = (end - start).dt.total_seconds().clip(lower=0)
  random_s = (rng.random(len(start)) * (ranges_s + 1)).astype(int)
  return start + pd.to_timedelta(random_s, unit='s')


def generate_date_offset(
    seed: int,
    reference: pd.Series,
    offset_years: float,
    spread_years: float,
) -> pd.Series:
    rng = np.random.default_rng(seed)
    offset_days = int(offset_years * 365.25)
    spread_days = int(spread_years * 365.25)
    random_days = rng.integers(-spread_days, spread_days + 1, size=len(reference))
    return reference + pd.to_timedelta(offset_days + random_days, unit="D")