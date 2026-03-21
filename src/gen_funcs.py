import numpy as np
import pandas as pd

#from src.entity_annotation import PrimaryKey, CreationTime, Faker, ForeignFields, ForeignKey, Pattern, Unique 
from src.entity import Entity, EntityField, PrimaryKey, CreationTime, Faker, ForeignFields, ForeignKey, Pattern, Unique 
from src.entity_common import EntityField, EntityContext




def aggregate_foreign_data(
    entities: dict[type, EntityContext],
    ent_ctx: EntityContext,
    foreign_annotations: list[type],
) -> tuple[pd.DataFrame, dict[str, list[str]]]:
    """
    Starting from ent_ctx's data (filtered by current_annotations),
    left-joins each foreign entity's data (filtered by foreign_annotations)
    via the ForeignKey fields found on the entity.

    Returns the merged dataframe with FK columns dropped after joining.
    """
    df_agg = ent_ctx.get_data(PrimaryKey, ForeignKey) 
    fk_fields = ent_ctx.entity.get_fields_with_annotation(ForeignKey)
    agg_cols = {}
    for fld in fk_fields:
        fk: ForeignKey = fld.annotations[ForeignKey] 
        foreign_ctx = entities[fk.entity] 
        foreign_pk = fk.entity.primary_key_field() 
        df_foreign = foreign_ctx.get_data(PrimaryKey, *foreign_annotations) 
        
        # ! add suffix to avoid name collision 
        # ! if creation time not defined it is now empty, but it should be set to its default 
        
        rename_map = { c:f"{fld.name}_{c}" for c in df_foreign.columns if c != foreign_pk.name } 
        df_foreign = df_foreign.rename(columns=rename_map) 
        agg_cols[fld.name] = list(rename_map.values()) 
        
        df_agg = pd.merge(df_agg, df_foreign, left_on=fld.name, right_on=foreign_pk.name, how="left") 
        if fld.name != foreign_pk.name:
            df_agg = df_agg.drop(columns=[foreign_pk.name]) 

    return df_agg, agg_cols



def generate_dates(start_date: pd.Series, end_date: pd.Series) -> pd.Series:
    ranges = (end_date - start_date).dt.days.clip(lower=0)
    random_days = (np.random.rand(len(start_date)) * (ranges + 1)).astype(int)
    return start_date + pd.to_timedelta(random_days, unit='D')


def generate_creationtime(entities: dict[type, EntityContext], ent_ctx: EntityContext) -> pd.Series:
    #pk = ent_ctx.entity.primary_key_field()
    ptime = ent_ctx.entity.primary_time_field()
    time_annotation: CreationTime = ptime.annotations[CreationTime]
    
    df, agg_cols = aggregate_foreign_data(entities, ent_ctx, foreign_annotations=[CreationTime]) 

    # Find the most recent foreign created_at across all FK fields
    time_cols = [item for sublist in agg_cols.values() for item in sublist]
    df['start_date'] = time_annotation.start
    df['start_date'] = df[['start_date', *time_cols]].max(axis=1).clip(lower=time_annotation.start)
    df['end_date'] = time_annotation.end
    return generate_dates(df['start_date'], df['end_date'])



def generate_sequential(ent_ctx: EntityContext) -> pd.Series:
    a = ent_ctx.preexisting.shape[0] + 1
    b = a + ent_ctx.N
    return pd.Series(range(a, b))
  
  
def generate_fk_fields(entities: dict[type, EntityContext], ent_ctx: EntityContext) -> dict[str, pd.Series]:
    generated = {}
    for fld in ent_ctx.entity.get_fields_with_annotation(ForeignKey):
        fk: ForeignKey = fld.annotations[ForeignKey]
        foreign_ctx = entities[fk.entity]
        foreign_pk = fk.entity.primary_key_field()
        foreign_data = foreign_ctx.get_data(PrimaryKey)
        generated[fld.name] = pd.Series(
            np.random.choice(foreign_data[foreign_pk.name], size=ent_ctx.N)
        )
    return generated