from dataclasses import dataclass 
import datetime 
from typing import Annotated


#from src.entity import SimEntity
from src.entity import Distribution, PrimaryKey, Faker, Unique, Categorical, Temporal


@dataclass
class Customer:
    customer_id: Annotated[int, PrimaryKey(strategy="sequential")]
    email: Annotated[str, Unique(), Faker("email")]
    first_name: Annotated[str, Faker("first_name")]
    age: Annotated[int, Distribution("normal", mean=38, std=12, min=18, max=90)]
    region: Annotated[str, Categorical(["North", "South", "East", "West"])] 
    created_at: Annotated[datetime, Temporal(before="updated_at")] # temporal integrity 
    updated_at: Annotated[datetime, Temporal(after="created_at")]


