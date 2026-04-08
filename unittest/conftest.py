# conftest.py  (place at project root, next to pytest.ini)

# Prevent pytest from treating the simulation runner as a test file
collect_ignore_glob = ["testcase/*"]  # adjust path if it's elsewhere


import datetime
import pandas as pd
import pytest
from dataclasses import dataclass
from typing import Annotated

from src.entity import Entity
from src.context import EntityContext
from src.annotations.primaries import PrimaryKey, CreationTime
from src.annotations.generator import GenNormal


@dataclass
class SimpleEntity(Entity):
    id:         Annotated[int, PrimaryKey()]
    created_at: Annotated[datetime.datetime, CreationTime(
                    start=datetime.datetime(2020, 1, 1),
                    end=datetime.datetime(2024, 1, 1),
                )]
    score:      Annotated[float, GenNormal(mean=50, std=10)]


@pytest.fixture
def preexisting_df():
    return pd.DataFrame({
        "id":         [1, 2, 3],
        "created_at": [datetime.datetime(2020, 6, 1),
                       datetime.datetime(2021, 3, 15),
                       datetime.datetime(2022, 9, 10)],
        "score":      [10.0, 20.0, 30.0],
    })


@pytest.fixture
def simple_ctx(preexisting_df):
    return EntityContext(SimpleEntity, preexisting_df, N=5)