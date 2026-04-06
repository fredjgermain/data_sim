import pandas as pd
import numpy as np
import datetime
from dataclasses import dataclass
from typing import Annotated

from src.annotations.primaries import PrimaryKey
from src.annotations.validation import Unique
from src.entity import Entity
from src.context import EntityContext
from src.simulator import DataSimulator


from src.annotations.standardgen import GenCtx
from src.annotations.standardgen import (
    GenNormal, GenUniform, GenFaker, GenPattern, CustomGen, GenCategorical, GenGamma, GenPoisson
)
from src.annotations.primaries import (PrimaryKey, CreationTime, ForeignKey)
from src.annotations.fault import Nullify, Duplicate


# ! CustomGenerators function  
from faker import Faker
fake = Faker()

def fake_movie_title(ctx:GenCtx):
  res = []
  for _ in range(ctx.N):
    words = fake.words(nb=3)  # Generate 3 random words
    capitalized_words = [w.capitalize() for w in words]
    res.append(' '.join(capitalized_words))
  return res

def res_time_func(ctx:GenCtx): 
  complaints_N = ctx.current_data['complaints_N'] 
  res_time_per_complain = np.random.uniform(2,5, size=ctx.N) 
  return complaints_N * res_time_per_complain 



GENRES = [
    'Action',
    'Adventure',
    'Animation',
    'Comedy',
    'Crime',
    'Documentary',
    'Drama',
    'Family',
    'Fantasy',
    'History',
    'Horror',
    'Music',
    'Mystery',
    'Romance',
    'Science Fiction',
    'TV Movie',
    'Thriller',
    'War',
    'Western'
    ]


@dataclass
class Region(Entity):
    region_id:  Annotated[int, PrimaryKey()]
    created_at: Annotated[datetime.datetime, CreationTime(
                start=datetime.datetime(1998, 1, 1),
                end=datetime.datetime(2002, 1, 1),
            )]
    name:       Annotated[str,  GenFaker("city")]
    code:       Annotated[str,  GenPattern(r'[A-Z]{2}-\d{3}'), Unique()]


@dataclass
class Customer(Entity):
    customer_id: Annotated[int, PrimaryKey()]
    created_at:  Annotated[datetime.datetime, CreationTime(
                     start=datetime.datetime(2015, 1, 1),
                     end=datetime.datetime(2023, 12, 31),
                 )]
    region_id:   Annotated[int,   ForeignKey(Region)]
    email:       Annotated[str,  GenFaker("email"), Unique()]
    sexe:        Annotated[int,  GenCategorical(categories=['male', 'female'])] 
    age:         Annotated[int,  GenNormal(min=18, max=90, mean=40, std=15, rounding=0)]
    code:        Annotated[str,  GenPattern(r'CUST-[A-Z]{3}-\d{4}')]


@dataclass
class Movie(Entity): 
  movie_id:      Annotated[int, PrimaryKey()] 
  title:         Annotated[str, CustomGen(fake_movie_title), Unique()] 
  genre1:         Annotated[int, GenCategorical(categories=GENRES)] 
  genre2:         Annotated[int, GenCategorical(categories=GENRES)] 
  genre3:         Annotated[int, GenCategorical(categories=GENRES)] 


@dataclass
class Subscription(Entity):
  sub_id:       Annotated[int, PrimaryKey()]
  created_at:   Annotated[datetime.datetime, CreationTime(
                     start=datetime.datetime(2015, 1, 1),
                     end=datetime.datetime(2023, 12, 31),
                 )]
  customer_id:    Annotated[int, ForeignKey(Customer)]
  complaints_N:   Annotated[int, GenPoisson(mean=0.4)]
  res_time:       Annotated[int, CustomGen(res_time_func)]
  

entities = {
    Region:       EntityContext(Region,     pd.DataFrame(),    N=12), 
    Customer:     EntityContext(Customer,   pd.DataFrame(),   N=200), 
    Movie:        EntityContext(Movie,      pd.DataFrame(),   N=1000), 
    Subscription: EntityContext(Subscription, pd.DataFrame(), N=200) 
} 


# Simulation ---------------

sim = DataSimulator(entities) 
results = sim.simulate() 

for e in entities.keys(): 
  print(results[e].head()) 

#print(results[e][['complaints_N', 'res_time']].sort_values(by=['complaints_N'])) 