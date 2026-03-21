import numpy as np
import pandas as pd
from dataclasses import dataclass, fields, field
from faker import Faker as FakerLib
import rstr
import datetime

from typing import Annotated

#from src.entity_annotation import PrimaryKey, CreationTime, Faker, ForeignFields, ForeignKey, Pattern, Unique 
from src.entity import Entity, EntityField, PrimaryKey, CreationTime, Faker, ForeignFields, ForeignKey, Pattern, Unique 



@dataclass
class EntityContext:
  entity: type
  preexisting: pd.DataFrame = field(default_factory=pd.DataFrame)
  generated: pd.DataFrame = field(default_factory=pd.DataFrame)
  N: int = 0
  done: bool = False


# ---------------------------------------------------------------------------
# Generator functions
# ---------------------------------------------------------------------------


# ! if creation depends on an other table it must generate Creation time at the end 
# ! If creation time is generated last then it can be generated to accommodate all other constraints 
def generate_fkey(entities: dict[type, EntityContext], ent_ctx:EntityContext, fld:EntityField) -> pd.Series: 
  fkey:ForeignKey = fld.annotations[ForeignKey] 
  foreign_entity = fkey.entity 
  forekeyfld = foreign_entity.get_primary_key_field() 
  
  foreign_ctx = entities[foreign_entity] 
  df_foreign = pd.concat([foreign_ctx.preexisting, foreign_ctx.generated]) 
  fk = df_foreign[forekeyfld.name] 
  return np.random.choice(fk) 


def get_entity_primaries(ent_ctx:EntityContext) -> pd.DataFrame: 
  ent:Entity = ent_ctx.entity 
  primarykey = ent.get_primary_key_field() 
  creationtime = ent.primary_time_field() 
  
  selection = [ n for n in [primarykey.name, creationtime.name] if n is not None ] 
  return pd.concat([ent_ctx.preexisting, ent_ctx.generated])[selection] 

def get_entity_data(ent_ctx:EntityContext) -> pd.DataFrame: 
  return pd.concat([ent_ctx.preexisting, ent_ctx.generated]) 


# ! if creation depends on an other table it must generate Creation time at the end 
# ! If creation time is generated last then it can be generated to accommodate all other constraints 
def generate_creationtime(entities: dict[type, EntityContext], ent_ctx:EntityContext, fld:EntityField) -> pd.Series: 
  ent:Entity = ent_ctx.entity 
  primarykey = ent.get_primary_key_field() 
  
  flds = ent.inspect() 
  deps_flds = { k:fld for k,fld in flds.items() if ForeignKey in fld.annotations } 
  
  ## Accumulate all foreign creation times into a single dataframe. 
  df_creationtimes = get_entity_data(ent_ctx)[[primarykey.name]] 
  for fld in deps_flds: 
    fk:ForeignKey = fld.annotations[ForeignKey] 
    df = get_entity_data(ent_ctx)[[primarykey.name, fld.name]] 
    df_fore = get_entity_primaries(entities[fk.entity]) 
    df = pd.merge(df, df_fore, on=fld.name, how='left').drop(columns=fld.name) 
    df_creationtimes = pd.merge(df_creationtimes, df, on=primarykey.name, how='left') 



def generate_with_pattern(ent_ctx: EntityContext, ent_field: EntityField, df_foreign: pd.DataFrame) -> pd.Series:
  ptrn:Pattern = ent_field.annotations[Pattern]
  ptrn.regex
  return pd.Series([rstr.xeger(ptrn) for _ in range(ent_ctx.N)])


def generate_with_faker(ent_ctx: EntityContext, ent_field: EntityField, df_foreign: pd.DataFrame) -> pd.Series:
    fkr:Faker = ent_field.annotations[Faker]
    fake = FakerLib(fkr.locale)
    generator = fake.unique if ent_field.has(Unique) else fake

    method = getattr(generator, fkr.method, None)
    if method is None:
        raise ValueError(f"Faker has no method '{fkr.method}'.")

    return pd.Series([method() for _ in range(ent_ctx.N)])


def generate_sequential(ent_ctx: EntityContext, ent_field: EntityField, df_foreign: pd.DataFrame) -> pd.Series:
    a = ent_ctx.preexisting.shape[0] + 1
    b = a + ent_ctx.N
    return pd.Series(range(a, b))


def generate_creation_time(ent_ctx: EntityContext, ent_field: EntityField, df_foreign: pd.DataFrame) -> pd.Series:
    """Generate N random datetimes uniformly distributed in [start, end].
    
    The annotation is guaranteed unique per entity (enforced by Simulator).
    Timestamps are sorted ascending so that sequential rows feel natural,
    though callers must not rely on this for correctness.
    """
    ann: CreationTime = ent_field.annotations[CreationTime]
    
    start_ts = ann.start.timestamp()
    end_ts   = ann.end.timestamp()

    # ! this kind of validation should be done upfront in Entity method. 
    if end_ts < start_ts:
        raise ValueError(f"CreationTime.end ({ann.end}) must be >= CreationTime.start ({ann.start}).")

    random_ts = np.random.uniform(start_ts, end_ts, size=ent_ctx.N)
    random_ts.sort()  # ascending — not required for correctness, but makes output readable
    return pd.Series([datetime.datetime.fromtimestamp(ts) for ts in random_ts])


def generate_foreign_key(ent_ctx: EntityContext, ent_field: EntityField, df_foreign: pd.DataFrame) -> pd.Series:
    """Randomly assign a foreign primary key to each generated row.

    Temporal integrity — if both the child entity and the foreign entity have a
    CreationTime field, each child row only draws from foreign rows whose
    creation time is *earlier than or equal to* the child row's creation time.

    df_foreign columns (injected by Simulator._get_df_foreign):
        col 0 : foreign primary key
        col 1 : foreign CreationTime (optional — present only when the foreign
                 entity declares a CreationTime field)

    The child entity's own CreationTime values are read from
    ent_ctx.generated[child_time_col] which must already be populated before
    this function is called (Simulator ensures CreationTime fields are
    generated first).
    """
    fk_col = df_foreign.columns[0]
    has_foreign_time = len(df_foreign.columns) > 1
    foreign_time_col = df_foreign.columns[1] if has_foreign_time else None

    fk = ent_field.get_annotation(ForeignKey)
    child_time_field = get_primarytime_field(ent_ctx.entity)

    use_temporal = (
        has_foreign_time
        and child_time_field is not None
        and child_time_field.name in ent_ctx.generated.columns
        and foreign_time_col is not None
        # Only apply temporal filter when the foreign df actually has real timestamps
        and not df_foreign[foreign_time_col].isna().all()
    )

    if not use_temporal:
        # Simple random choice — no temporal constraint.
        return pd.Series(np.random.choice(df_foreign[fk_col], size=ent_ctx.N))

    child_times = ent_ctx.generated[child_time_field.name].reset_index(drop=True)
    result = []

    for child_ts in child_times:
        # Eligible foreign rows: those created at or before this child row.
        eligible = df_foreign.loc[df_foreign[foreign_time_col] <= child_ts, fk_col]

        if eligible.empty:
            raise ValueError(
                f"No eligible '{ent_ctx.entity.__name__}' foreign keys exist "
                f"at or before {child_ts}. "
                f"Extend CreationTime.start on the foreign entity or adjust the child's range."
            )

        result.append(np.random.choice(eligible))

    return pd.Series(result)



# ---------------------------------------------------------------------------
# Simulator
# ---------------------------------------------------------------------------

@dataclass
class Simulator:
    entities: dict[type, EntityContext] = field(default_factory=dict)

    def simulate_all_entities(self):
        for ent_ctx in self.entities.values():
            self.simulate_entity(ent_ctx)

    def simulate_entity(self, ent_ctx: EntityContext):
        if ent_ctx.done:
            return

        ent = ent_ctx.entity
        ent_fields = ent.inspect()


        # --- Pass 1: generate CreationTime first so FK generators can use it. ---
        for fld in ent_fields.values(): 
            if CreationTime not in fld.annotations: 
                continue 
            if fld.func is None:
                fld.func = generate_creation_time
            df_foreign = pd.DataFrame()  # CreationTime never depends on a foreign table
            ent_ctx.generated[fld.name] = fld.func(ent_ctx, fld, df_foreign)

        # --- Pass 2: generate all other fields. ---
        for fld in ent_fields.values():
            if CreationTime in fld.annotations:
                continue  # already done in pass 1

            df_foreign = self._get_df_foreign(fld)

            if fld.func is None and ForeignKey in fld.annotations:
                fld.func = generate_foreign_key

            if fld.func is None and Faker in fld.annotations:
                fld.func = generate_with_faker

            if fld.func is None and Pattern in fld.annotations: 
                fld.func = generate_with_pattern

            if fld.func:
                ent_ctx.generated[fld.name] = fld.func(ent_ctx, fld, df_foreign)

        ent_ctx.done = True
        result = pd.concat([ent_ctx.preexisting, ent_ctx.generated]).reset_index(drop=True)
        #print(result)

    def _get_df_foreign(self, fld: EntityField) -> pd.DataFrame:
        """Return the foreign DataFrame slice needed by a field's generator.

        For a ForeignKey field the slice always includes:
          col 0 — the foreign entity's primary key
          col 1 — the foreign entity's CreationTime column (if it exists)

        The CreationTime column is included so that generate_foreign_key can
        enforce temporal integrity without needing a separate lookup.
        """
        if ForeignKey in fld.annotations or ForeignFields in fld.annotations:
            for_ann = fld.get_foreign()
            for_ent = for_ann.entity
            for_ctx = self.entities[for_ent]

            if not for_ctx.done:
                raise RuntimeError(
                    f"Foreign entity '{for_ent.__name__}' has not been simulated yet. "
                    f"Simulate it before '{fld.name}'."
                )

            all_foreign = pd.concat(
                [for_ctx.preexisting, for_ctx.generated]
            ).reset_index(drop=True)

            pk_cols = fld.get_foreign_columns()   # [pk_field_name, ...]

            # Append the foreign CreationTime column if available and not already included.
            time_field = get_primarytime_field(for_ent)
            extra_cols = []
            if (
                time_field is not None
                and time_field.name not in pk_cols
                and time_field.name in all_foreign.columns
            ):
                extra_cols.append(time_field.name)

            return all_foreign[pk_cols + extra_cols]

        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Test-case entities
# ---------------------------------------------------------------------------

@dataclass
class Region(Entity):
    region_id: Annotated[int, PrimaryKey()]


@dataclass
class Customer(Entity):
    customer_id: Annotated[int, generate_sequential, PrimaryKey()]
    created_at:  Annotated[datetime.datetime, CreationTime(
                     start=datetime.datetime(2022, 1, 1),
                     end=datetime.datetime(2023, 12, 31),
                 )]
    region_id:   Annotated[int, ForeignKey(Region)]
    email:       Annotated[str, Unique(), Faker("email")]
    code:        Annotated[str, Pattern(r'[A-Z]{3}-\d{4}')]


@dataclass
class Transaction(Entity):
    transaction_id: Annotated[int, generate_sequential, PrimaryKey()]
    created_at:     Annotated[datetime.datetime, CreationTime(
                        start=datetime.datetime(2022, 6, 1),  # transactions can't precede first customers
                        end=datetime.datetime(2024, 6, 30),
                    )]
    customer_id:    Annotated[int, ForeignKey(Customer)]


# ---------------------------------------------------------------------------
# Simulation bootstrap
# ---------------------------------------------------------------------------

df_region = pd.DataFrame(range(1, 51), columns=['region_id'])

# Preexisting customers — include a created_at column so temporal FK works.
df_customer_pre = pd.DataFrame({
    'customer_id': range(1, 101),
    'created_at':  pd.date_range(start='2022-01-01', periods=100, freq='3D'),
    'region_id':   np.random.choice(df_region['region_id'], size=100),
    'email':       ['preexisting@example.com'] * 100,
    'code':        ['PRE-0000'] * 100,
})

sim = Simulator({
    Region:      EntityContext(Region,      preexisting=df_region,       done=True),
    Customer:    EntityContext(Customer,    preexisting=df_customer_pre, N=100),
    Transaction: EntityContext(Transaction, N=1000),
})
sim.simulate_all_entities()

customer = sim.entities[Customer] 
transac = sim.entities[Transaction] 
df_cust = pd.concat([customer.preexisting, customer.generated])
df_tran = pd.concat([transac.preexisting, transac.generated])


# Find referential temporal violations 
merged = pd.merge(df_cust, df_tran, on='customer_id', how='left')
violations = merged[merged['created_at_y'] < merged['created_at_x']]

print(f"Violations: {len(violations)} / {len(merged[merged['created_at_y'].notna()])}")
print(violations[['customer_id', 'created_at_x', 'created_at_y']].rename(columns={
    'created_at_x': 'customer_created_at',
    'created_at_y': 'transaction_created_at',
}))

