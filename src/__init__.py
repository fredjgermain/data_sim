# """
# data_simulator
# ~~~~~~~~~~~~~~
# A library for simulating realistic, relational tabular data as pandas DataFrames.

# Quickstart::

#     from dataclasses import dataclass
#     from typing import Annotated
#     import datetime

#     from data_simulator import (
#         Entity, EntityContext, DataSimulator,
#         PrimaryKey, ForeignKey, CreationTime,
#         Unique, Faker, GenPattern, GenNormal, CustomGen,
#     )

#     @dataclass
#     class Region(Entity):
#         region_id:  Annotated[int,               PrimaryKey()]
#         created_at: Annotated[datetime.datetime, CreationTime(
#                         start=datetime.datetime(2000, 1, 1),
#                         end=datetime.datetime(2020, 12, 31),
#                     )]

#     @dataclass
#     class Customer(Entity):
#         customer_id: Annotated[int,               PrimaryKey()]
#         created_at:  Annotated[datetime.datetime, CreationTime(
#                          start=datetime.datetime(2010, 1, 1),
#                          end=datetime.datetime(2025, 12, 31),
#                      )]
#         region_id:   Annotated[int,  ForeignKey(Region)]
#         email:       Annotated[str,  Unique(), Faker("email")]
#         code:        Annotated[str,  GenPattern(r'[A-Z]{3}-\\d{4}')]
#         age:         Annotated[int,  GenNormal(min=0, mean=45, std=20, rounding=0)]
#         label:       Annotated[str,  CustomGen(
#                          lambda df: df["age"].apply(lambda a: "senior" if a >= 65 else "adult")
#                      )]

#     entities = {
#         Region:   EntityContext(Region,   df_region_pre,   N=10),
#         Customer: EntityContext(Customer, df_customer_pre, N=1000),
#     }

#     sim = DataSimulator(entities)
#     results = sim.simulate()
#     # results: {Region: pd.DataFrame, Customer: pd.DataFrame}

# Design constraints:
#     - The ``entities`` dict must be ordered so that parent entities appear
#       before their dependents. This is required for correct CreationTime
#       causal ordering (child timestamps >= parent timestamps). Foreign key
#       resolution is always safe regardless of order (all PKs are generated
#       in pass 1 before any FK resolution occurs in pass 2).
#     - Each field may carry at most one annotation of each type. Declaring
#       two annotations of the same type on the same field raises TypeError
#       at class-introspection time.
#     - CustomGen fields are executed in dataclass field declaration order.
#       If one CustomGen field depends on another, declare it later.
# """

# from data_simulator.annotations import (
#     Annotation,
#     CreationTime,
#     CustomGen,
#     Faker,
#     ForeignKey,
#     GenNormal,
#     GenPattern,
#     PrimaryKey,
#     Unique,
# )
# from data_simulator.context import EntityContext
# from data_simulator.entity import Entity, EntityField, IEntity
# from data_simulator.simulator import DataSimulator, SimulationContext

# __all__ = [
#     # Core simulation
#     "DataSimulator",
#     "EntityContext",
#     "SimulationContext",
#     # Entity introspection
#     "Entity",
#     "IEntity",
#     "EntityField",
#     # Annotations
#     "Annotation",
#     "PrimaryKey",
#     "ForeignKey",
#     "CreationTime",
#     "Unique",
#     "Faker",
#     "GenPattern",
#     "GenNormal",
#     "CustomGen",
# ]
