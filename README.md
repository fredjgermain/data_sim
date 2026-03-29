# data_simulator — Project Plan

## Overview

`data_simulator` is a Python library for generating realistic, relational tabular data as pandas DataFrames. Users define entities as dataclasses inheriting from `Entity`, annotate fields with generation descriptors, declare relationships via `ForeignKey`, and hand everything off to `DataSimulator` to produce a dict of DataFrames.

---

## User-facing API (reference)

```python
@dataclass
class Region(Entity):
    region_id:  Annotated[int,              PrimaryKey()]
    created_at: Annotated[datetime.datetime, CreationTime(
                    start=datetime.datetime(2000, 1, 1),
                    end=datetime.datetime(2020, 12, 31),
                )]

@dataclass
class Customer(Entity):
    customer_id: Annotated[int,              PrimaryKey()]
    created_at:  Annotated[datetime.datetime, CreationTime(
                     start=datetime.datetime(2010, 1, 1),
                     end=datetime.datetime(2025, 12, 31),
                 )]
    region_id:   Annotated[int,  ForeignKey(Region)]
    email:       Annotated[str,  Unique(), Faker("email")]
    code:        Annotated[str,  GenPattern(r'[A-Z]{3}-\d{4}')]
    age:         Annotated[int,  GenNormal(min=0, mean=45, std=20, rounding=0)]
    label:       Annotated[str,  CustomGen(lambda df: df["age"].apply(lambda a: "senior" if a >= 65 else "adult"))]

entities = {
    Region:   EntityContext(Region,   df_region_pre,   N=10),
    Customer: EntityContext(Customer, df_customer_pre, N=1000),
}

sim = DataSimulator(entities)
results: dict[type[Entity], pd.DataFrame] = sim.simulate()
```

---

## Module structure

```
data_simulator/
├── __init__.py
├── simulator.py       # DataSimulator
├── context.py         # EntityContext
├── entity.py          # Entity, IEntity, EntityField
└── annotations.py     # All annotation classes
```

---

## `annotations.py`

### Base class

```python
class Annotation:
    pass
```
> Marker base class for all annotation types. Provides a common `__repr__` and is used for `isinstance` checks throughout the library.

---

### `PrimaryKey`

```python
@dataclass
class PrimaryKey(Annotation):
    # No configuration fields needed for the default sequential strategy.

    def generate(self, ctx: "EntityContext", N: int) -> pd.Series:
        # Determine the max primary key value already present in ctx.preexisting
        # (or 0 if empty), then produce N sequential integers starting from max+1.
        # Return as a pd.Series.
        ...
```

---

### `ForeignKey`

```python
@dataclass
class ForeignKey(Annotation):
    target: type  # The Entity class this field references, e.g. ForeignKey(Region)

    def generate(self, target_ctx: "EntityContext", N: int) -> pd.Series:
        # Collect all primary-key values from target_ctx (both .preexisting and
        # .generated). Randomly sample N values with replacement from that pool.
        # Return as a pd.Series.
        ...
```

---

### `CreationTime`

```python
@dataclass
class CreationTime(Annotation):
    start: datetime.datetime
    end:   datetime.datetime

    def generate(
        self,
        N: int,
        lower_bound: pd.Series | None = None,
    ) -> pd.Series:
        # Generate N random datetimes uniformly distributed between self.start
        # and self.end.
        # If lower_bound is provided (a Series of datetimes — the parent entity's
        # creation times looked up via the already-generated ForeignKey column),
        # clamp each generated value so it is >= its corresponding lower_bound.
        # Return as a pd.Series of datetime values.
        ...
```

> **Causal ordering contract:** `DataSimulator` is responsible for passing the correct `lower_bound` series. In pass 3 it resolves the entity's `ForeignKey` fields, looks up the matched parent rows' `CreationTime` values from the parent's `EntityContext`, and forwards them as `lower_bound`. If the parent entity has no `CreationTime` field, `lower_bound` is `None` and the entity falls back to generating timestamps freely within its own `start`/`end` range.
>
> **Entity ordering:** Causal correctness depends on the user ordering the `entities` dict so that parent entities appear before their dependents. This is a documented user responsibility — no automatic topological sort is performed.

---

### `Unique`

```python
@dataclass
class Unique(Annotation):
    max_attempts: int = 3

    def enforce(
        self,
        series: pd.Series,
        generate_fn: Callable[[int], pd.Series],
        existing: pd.Series | None,
        logger: logging.Logger,
    ) -> pd.Series:
        # Identify duplicate values in `series` (and optionally against `existing`
        # preexisting values). For each duplicate, call generate_fn(n_dupes) to
        # regenerate replacements. Repeat up to self.max_attempts times.
        # If duplicates remain after all attempts, log a warning via `logger`
        # and return the series as-is.
        ...
```

> `Unique` is a post-processing step, not a generator. `DataSimulator` calls `enforce()` after the field's primary generator has run.

---

### `Faker`

```python
@dataclass
class Faker(Annotation):
    provider: str  # e.g. "email", "name", "address"

    def generate(self, N: int) -> pd.Series:
        # Instantiate a faker.Faker() instance. Call the method named
        # self.provider on it N times. Return results as a pd.Series.
        ...
```

---

### `GenPattern`

```python
@dataclass
class GenPattern(Annotation):
    pattern: str  # A regex pattern, e.g. r'[A-Z]{3}-\d{4}'

    def generate(self, N: int) -> pd.Series:
        # Use the rstr library to generate N strings matching self.pattern.
        # Return as a pd.Series.
        ...
```

---

### `GenNormal`

```python
@dataclass
class GenNormal(Annotation):
    mean:     float
    std:      float
    min:      float | None = None
    max:      float | None = None
    rounding: int   | None = None  # decimal places; 0 means cast to int

    def generate(self, N: int) -> pd.Series:
        # Draw N samples from a normal distribution with self.mean and self.std.
        # Clip values to [self.min, self.max] if either bound is set.
        # Apply rounding if self.rounding is not None (rounding=0 → integer).
        # Return as a pd.Series.
        ...
```

---

### `CustomGen`

```python
@dataclass
class CustomGen(Annotation):
    fn: Callable[[pd.DataFrame], pd.Series]
    # fn receives the partially-built DataFrame for the current entity
    # (all fields generated in passes 1–4 are already present as columns)
    # and must return a pd.Series of length N.

    def generate(self, partial_df: pd.DataFrame) -> pd.Series:
        # Call self.fn(partial_df) and return the resulting pd.Series.
        # Validation: assert the returned series has the same length as partial_df.
        ...
```

---

## `entity.py`

### `EntityField`

```python
@dataclass
class EntityField:
    name:        str
    base_type:   type
    annotations: dict[type[Annotation], Annotation]
    # Keyed by annotation type — enforces at most one annotation of each type per field.
    # e.g. {Unique: Unique(), Faker: Faker("email")}
    # Populated by IEntity.inspect() at introspection time; raises TypeError if a
    # duplicate annotation type is detected on the same field.

    def get(self, annotation_type: type[A]) -> A | None:
        # Return self.annotations.get(annotation_type), cast to A.
        # Returns None if the annotation type is not present on this field.
        ...

    def has(self, *annotation_types: type) -> bool:
        # Return True if ANY of the provided annotation_types are keys in
        # self.annotations. Used for field filtering in DataSimulator passes.
        ...
```

---

### `IEntity` (interface / mixin)

```python
class IEntity:

    @classmethod
    def inspect(cls) -> dict[str, EntityField]:
        # Introspect cls.__dataclass_fields__ and cls.__annotations__ to build
        # one EntityField per field. For each field, collect all Annotation instances
        # from the Annotated[...] metadata into a dict keyed by annotation type.
        # Raise TypeError if two annotations of the same type appear on the same field.
        # Return as an ordered dict {field_name: EntityField}.
        ...

    @classmethod
    def get_fields(
        cls,
        include: list[type] | None = None,
        exclude: list[type] | None = None,
    ) -> list[EntityField]:
        # Call cls.inspect(), then filter the resulting EntityFields.
        # If `include` is given, keep only fields that have ANY of those annotation types.
        # If `exclude` is given, drop fields that have ANY of those annotation types.
        # Both can be combined.
        ...

    @classmethod
    def get_primary_key_field(cls) -> EntityField:
        # Return the single EntityField annotated with PrimaryKey.
        # Raise ValueError if none or more than one is found.
        ...

    @classmethod
    def get_creation_time_field(cls) -> EntityField | None:
        # Return the EntityField annotated with CreationTime, or None if absent.
        ...
```

---

### `Entity`

```python
@dataclass
class Entity(IEntity):
    # Concrete base class that users inherit from.
    # No additional attributes — all behaviour lives in IEntity classmethods.
    ...
```

---

## `context.py`

### `EntityContext`

```python
@dataclass
class EntityContext:
    entity:      type[Entity]           # The Entity subclass type (not an instance)
    preexisting: pd.DataFrame           # Rows that exist before simulation; may be empty
    N:           int                    # Number of new rows to generate
    generated:   pd.DataFrame = field(default_factory=pd.DataFrame)  # Populated during simulation

    def get_primary_key_values(self) -> pd.Series:
        # Identify the primary key field name via self.entity.get_primary_key_field().
        # Concatenate that column from both self.preexisting and self.generated,
        # dropping NaNs. Return the combined pd.Series.
        ...

    def get_creation_time_values(self) -> pd.Series:
        # Identify the creation time field name via self.entity.get_creation_time_field().
        # Concatenate that column from both self.preexisting and self.generated,
        # dropping NaNs. Return the combined pd.Series.
        ...

    def get_data(
        self,
        include: list[str] | None = None,
        exclude: list[str] | None = None,
    ) -> pd.DataFrame:
        # Concatenate self.preexisting and self.generated into a single DataFrame.
        # If `include` is provided, return only those columns.
        # If `exclude` is provided, drop those columns.
        # Reset the index before returning.
        ...
```

---

## `simulator.py`

### `SimulationContext` *(optional internal dataclass)*

```python
@dataclass
class SimulationContext:
    pass_number: int
    entity:      type[Entity]
    ctx:         EntityContext
    # Lightweight carrier passed to internal helpers so they don't need
    # to re-derive entity/ctx from the entities dict on every call.
```

---

### `DataSimulator`

```python
@dataclass
class DataSimulator:
    entities: dict[type[Entity], EntityContext]
    logger:   logging.Logger = field(default_factory=lambda: logging.getLogger("data_simulator"))

    # ------------------------------------------------------------------ #
    # Public API                                                           #
    # ------------------------------------------------------------------ #

    def simulate(self) -> dict[type[Entity], pd.DataFrame]:
        # Orchestrate the five passes in order. After all passes complete,
        # return {entity_type: ctx.get_data() for entity_type, ctx in self.entities.items()}.
        ...

    # ------------------------------------------------------------------ #
    # Pass 1 — Primary keys                                               #
    # ------------------------------------------------------------------ #

    def _pass_primary_keys(self) -> None:
        # For each entity, locate its PrimaryKey field via Entity.get_primary_key_field().
        # Call PrimaryKey.generate(ctx, N) to produce N sequential values.
        # Write the result as a column in ctx.generated.
        ...

    # ------------------------------------------------------------------ #
    # Pass 2 — Foreign keys                                               #
    # ------------------------------------------------------------------ #

    def _pass_foreign_keys(self) -> None:
        # For each entity, find all fields annotated with ForeignKey.
        # For each such field, resolve its target EntityContext from self.entities.
        # Call ForeignKey.generate(target_ctx, N) to randomly sample N primary-key
        # values from the target's combined preexisting+generated pool.
        # Write the result as a column in ctx.generated.
        ...

    # ------------------------------------------------------------------ #
    # Pass 3 — Creation times                                             #
    # ------------------------------------------------------------------ #

    def _pass_creation_times(self) -> None:
        # For each entity, find its CreationTime field (skip if absent).
        # Determine if the entity has any ForeignKey field whose target entity
        # also has a CreationTime field.
        # If so, build the lower_bound Series: for each generated row, look up
        # the matched parent row's creation time using the already-generated
        # foreign key column and the parent ctx's combined data.
        # Call CreationTime.generate(N, lower_bound=...) and write to ctx.generated.
        ...

    # ------------------------------------------------------------------ #
    # Pass 4 — Standard fields                                            #
    # ------------------------------------------------------------------ #

    def _pass_standard_fields(self) -> None:
        # For each entity, collect fields NOT annotated with PrimaryKey,
        # ForeignKey, CreationTime, or CustomGen.
        # For each such field, dispatch to the appropriate annotation's
        # .generate(N) method (GenNormal, GenPattern, Faker, etc.).
        # After generation, if the field also carries a Unique annotation,
        # call Unique.enforce() with a lambda that re-invokes the generator.
        # Write each result column to ctx.generated.
        ...

    # ------------------------------------------------------------------ #
    # Pass 5 — CustomGen fields                                           #
    # ------------------------------------------------------------------ #

    def _pass_custom_gen(self) -> None:
        # For each entity, collect fields annotated with CustomGen.
        # Build the partial DataFrame for this entity from ctx.generated
        # (all columns populated so far in passes 1–4).
        # Call CustomGen.generate(partial_df) and write the result column
        # to ctx.generated.
        ...

    # ------------------------------------------------------------------ #
    # Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _resolve_ctx(self, entity: type[Entity]) -> EntityContext:
        # Look up entity in self.entities. Raise KeyError with a descriptive
        # message if the entity has not been registered.
        ...

    def _log_pass(self, pass_number: int, description: str) -> None:
        # Emit a structured INFO log indicating which pass is running and
        # a short description. Useful for progress visibility on large simulations.
        ...
```

---

## Simulation pass order — summary

| Pass | What it generates | Annotation types handled |
|------|-------------------|--------------------------|
| 1 | Primary keys | `PrimaryKey` |
| 2 | Foreign keys | `ForeignKey` |
| 3 | Creation timestamps | `CreationTime` |
| 4 | All remaining standard fields | `GenNormal`, `GenPattern`, `Faker`, `Unique` |
| 5 | Derived / computed fields | `CustomGen` |

---

## Design decisions & constraints

- **Entity ordering (user responsibility):** Pass 1 generates all primary keys independently, so foreign key resolution in pass 2 is always safe regardless of dict order. However, causal `CreationTime` ordering in pass 3 requires that a parent entity's creation times already be written before its dependents are processed. The user is responsible for ordering the `entities` dict so that parent entities appear before their dependents. This must be clearly documented.

- **Circular foreign keys:** Foreign key resolution (pass 2) is safe with circular FK references because all primary keys are guaranteed to exist after pass 1. For `CreationTime` causal ordering in pass 3, circularly dependent entities will be assigned the same creation time. The `lower_bound` for each entity in the cycle is derived from whichever parent was processed first; the cycle resolves naturally without special handling.

- **One annotation type per field:** `EntityField.annotations` is `dict[type[Annotation], Annotation]`. `IEntity.inspect()` raises `TypeError` at class-introspection time if a field declares two annotations of the same type. `CustomGen` fields follow this same rule — if a field needs to reference another `CustomGen`-derived column, it must be declared as a separate field later in the dataclass definition (field declaration order is the execution order).

- **dtype coercion:** After each pass, each newly written column in `ctx.generated` is cast to the `base_type` declared in its `EntityField`. This keeps DataFrames correctly typed throughout simulation and prevents silent type drift across passes.
