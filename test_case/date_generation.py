# import numpy as np
# import pandas as pd
# from dataclasses import dataclass, fields, field
# from faker import Faker as FakerLib
# import rstr
# import datetime

# import typing
# from typing import Annotated

# #from src.entity_annotation import PrimaryKey, CreationTime, Faker, ForeignFields, ForeignKey, Pattern, Unique 
# from src.entity import Entity, EntityField, PrimaryKey, CreationTime, Faker, ForeignFields, ForeignKey, Pattern, Unique 



# @dataclass
# class EntityContext:
#   entity: type[Entity] 
#   preexisting: pd.DataFrame = field(default_factory=pd.DataFrame)
#   generated: pd.DataFrame = field(default_factory=pd.DataFrame)
#   N: int = 0
#   done: bool = False

#   def get_data(self, selection: list = None, exclusion: list = None) -> pd.DataFrame: 
#     flds = self.entity.get_fields_by_annotation(selection, exclusion) 
#     df = pd.concat([self.preexisting, self.generated]) 
#     selection = [ fld.name for fld in flds ] or list(df.columns) 
#     return df[selection]



# # ! if creation depends on an other table it must generate Creation time at the end 
# # ! If creation time is generated last then it can be generated to accommodate all other constraints 
# def generate_creationtime(entities: dict[type, EntityContext], ent_ctx:EntityContext, fld:EntityField=None) -> pd.Series: 
#   pk = ent_ctx.entity.get_primary_key_field() 
#   ptime = ent_ctx.entity.primary_time_field() 
#   flds = ent_ctx.entity.get_fields_by_annotation([ForeignKey]) 
#   df_current = ent_ctx.get_data(PrimaryKey, ForeignKey) 
  
#   ## Accumulate all foreign creation times into a single dataframe. 
#   for f in flds: 
#     fk:ForeignKey = f.annotations[ForeignKey] 
#     fpk = fk.entity.get_primary_key_field() 
#     df_foreign = entities[fk.entity].get_data(PrimaryKey, CreationTime) 
    
#     df_current = pd.merge(df_current, df_foreign, left_on=f.name, right_on=fpk.name, how='left') 
#     df_current = df_current.drop(columns=[f.name]) 
  
#   # keep only creation_dates and current primarykey and finds most recent creation date 
#   time_annotation:CreationTime = ptime.annotations[CreationTime] 
#   df_current[f'{pk.name}_start'] = time_annotation.start 
#   cols = [ c for c in df_current.columns if c is not pk.name] 
#   df_current[f'{pk.name}_start'] = df_current[cols].max(axis=1) 
#   df_current[f'{pk.name}_end'] = time_annotation.end
  
#   df = df_current.copy()
#   df = df.rename( columns={f'{pk.name}_start':'start_date', f'{pk.name}_end':'end_date'}) 
  
#   # ! Generate dates ... Reduce duplicates to generate faster. 
#   df_date = df[['start_date', 'end_date']].sort_values(by='start_date').drop_duplicates() 
#   df_date['random_date'] = df_date.apply(
#       lambda row: row['start_date'] + pd.Timedelta(
#           days=np.random.randint(0, (row['end_date'] - row['start_date']).days + 1)
#       ),
#       axis=1
#   )
#   df = pd.merge(df, df_date, on='start_date', how='left')
#   return df['random_date']
#   #return df_current.sort_values(by=['most_recent'])
  



# def generate_sequential(ent_ctx: EntityContext, ent_field: EntityField, df_foreign: pd.DataFrame) -> pd.Series:
#     a = ent_ctx.preexisting.shape[0] + 1
#     b = a + ent_ctx.N
#     return pd.Series(range(a, b))


# # ---------------------------------------------------------------------------
# # Test-case entities
# # ---------------------------------------------------------------------------

# @dataclass
# class Region(Entity):
#     region_id: Annotated[int, PrimaryKey()]
#     created_at:  Annotated[datetime.datetime, CreationTime(
#                   start=datetime.datetime(2000, 1, 1),
#                   end=datetime.datetime(2020, 12, 31),
#               )]


# @dataclass
# class Customer(Entity):
#     customer_id: Annotated[int, generate_sequential, PrimaryKey()]
#     created_at:  Annotated[datetime.datetime, CreationTime(
#                      start=datetime.datetime(2010, 1, 1),
#                      end=datetime.datetime(2025, 12, 31),
#                  )]
#     region_id:   Annotated[int, ForeignKey(Region)]
#     email:       Annotated[str, Unique(), Faker("email")]
#     code:        Annotated[str, Pattern(r'[A-Z]{3}-\d{4}')]


# @dataclass
# class Store(Entity):
#   store_id: Annotated[int, generate_sequential, PrimaryKey()] 
#   created_at:     Annotated[datetime.datetime, CreationTime(
#                         start=datetime.datetime(2010, 1, 1),  # transactions can't precede first customers
#                         end=datetime.datetime(2024, 6, 30),
#                     )]

# @dataclass
# class Transaction(Entity):
#     transaction_id: Annotated[int, generate_sequential, PrimaryKey()]
#     created_at:     Annotated[datetime.datetime, CreationTime(
#                         start=datetime.datetime(2010, 1, 1),  # transactions can't precede first customers
#                         end=datetime.datetime(2025, 12, 31),
#                     )]
#     customer_id:    Annotated[int, ForeignKey(Customer)]
#     store_id:       Annotated[int, ForeignKey(Store)]
#     region_id:       Annotated[int, ForeignKey(Region)]


# flds = Transaction.get_fields_by_annotation([PrimaryKey, ForeignKey]) 
# print([ f.name for f in flds]) 


# # ---------------------------------------------------------------------------
# # Simulation bootstrap
# # ---------------------------------------------------------------------------

# N = 12 
# reg_time:CreationTime = Region.primary_time_field().annotations[CreationTime] 
# region_date_range = pd.date_range(start=reg_time.start, end=reg_time.end, freq='D') 
# df_region = pd.DataFrame({ 
#     'region_id': range(1, N+1), 
#     'created_at': np.random.choice(region_date_range, N) 
# }) 

# # Preexisting customers — include a created_at column so temporal FK works. 
# N = 100 
# cus_time:CreationTime = Customer.primary_time_field().annotations[CreationTime] 
# customer_date_range = pd.date_range(start=cus_time.start, end=cus_time.end, freq='D') 
# df_customer_pre = pd.DataFrame({
#     'customer_id': range(1, N+1),
#     'created_at':  np.random.choice(customer_date_range, N), 
#     'region_id':   np.random.choice(df_region['region_id'], size=N),
#     'email':       ['preexisting@example.com'] * N,
#     'code':        ['PRE-0000'] * N,
# })


# N = 10
# store_date_range = customer_date_range
# df_store_pre = pd.DataFrame({
#   'store_id': range(1,N+1), 
#   'created_at':  np.random.choice(store_date_range, N), 
# })


# N = 100000
# transaction_date_range = customer_date_range
# df_transaction_pre = pd.DataFrame({ 
#   'transaction_id': range(1,N+1), 
#   'customer_id': np.random.choice(df_customer_pre['customer_id'], N), 
#   'store_id': np.random.choice(df_store_pre['store_id'], N), 
#   'region_id': np.random.choice(df_region['region_id'], N), 
#   #'created_at':  pd.date_range(start='2022-01-01', periods=N, freq='3D'),
# })


# reg_ctx = EntityContext(Region, df_region) 
# cus_ctx = EntityContext(Customer, df_customer_pre) 
# tra_ctx = EntityContext(Transaction, df_transaction_pre) 
# sto_ctx = EntityContext(Store, df_store_pre) 
# #print(cus_ctx.get_data()) 

# entities = {Region:reg_ctx, Customer:cus_ctx, Store:sto_ctx, Transaction:tra_ctx} 

# df:pd.DataFrame = generate_creationtime(entities, tra_ctx) 
# # df = df[['transaction_id', 'most_recent', 'transaction_id_end']] 
# # df = df.rename( columns={'most_recent':'start_date', 'transaction_id_end':'end_date'}) 

# # df_date = df[['start_date', 'end_date']].sort_values(by='start_date').drop_duplicates() 
# # df_date 

# # df_date['random_date'] = df_date.apply(
# #     lambda row: row['start_date'] + pd.Timedelta(
# #         days=np.random.randint(0, (row['end_date'] - row['start_date']).days + 1)
# #     ),
# #     axis=1
# # )
# # df = pd.merge(df, df_date, on='start_date', how='left') 
# print(df) # .sort_values(by='random_date') 


# # cus_key = Customer.primary_key_field().name 
# # cus_time = Customer.primary_time_field().name 
# # tra_key = Transaction.primary_key_field().name 
# # sto_key = 'store_id' 
# # sto_time = 'created_at' 

# # merged = df_transaction_pre[[tra_key, cus_key, sto_key]].copy() 
# # merged = pd.merge(merged, df_customer_pre[[cus_key, cus_time]], on=cus_key, how='left') 
# # #merged = merged.drop(columns=[cus_key]) 
# # merged = pd.merge(merged, df_store_pre[[sto_key, sto_time]], on=sto_key, how='left') 
# # #merged = merged.drop(columns=[sto_key]) 
# # merged['most_recent'] = merged[['created_at_x', 'created_at_y']].max(axis=1)
# # #print(merged) 




