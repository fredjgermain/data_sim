import pandas as pd 
from dataclasses import dataclass, field

from src.annotations.primaries import PrimaryKey, CreationTime
from src.interface import IEntity, IEntityContext, IEntityField 



@dataclass
class EntityContext(IEntityContext):
    """Container for one entity's data during simulation.

    An EntityContext is created by the user for each entity and passed to
    DataSimulator. It stores both preexisting rows (supplied upfront) and
    generated rows (populated pass by pass during simulation).

    Example::

        EntityContext(Region, df_region_pre, N=10)
        # entity      = Region  (the class itself, not an instance)
        # preexisting = df_region_pre
        # generated   = empty DataFrame  (filled during simulation)
        # N           = 10 new rows to generate

    Attributes:
        entity:      The Entity subclass type (not an instance).
        preexisting: DataFrame of rows that exist before simulation. May be
                     an empty DataFrame if there are no preexisting rows.
        N:           Number of new rows to generate during simulation.
        generated:   DataFrame populated incrementally during the simulation
                     passes. Starts empty; each pass adds columns.
    """

    entity:      type[IEntity]
    preexisting: pd.DataFrame
    N:           int
    generated:   pd.DataFrame = field(default_factory=pd.DataFrame)

    def get_primary_key_values(self) -> pd.Series:
        return self.get_serie(PrimaryKey)

    def get_creation_time_values(self) -> pd.Series: 
        return self.get_serie(CreationTime)

    def get_serie(self, 
        selection: str | type, 
        preexisting: bool = True, 
        generated: bool = True
    ) -> pd.Series:
        pre = self.preexisting if preexisting else pd.DataFrame() 
        gen = self.generated if generated else pd.DataFrame() 
        
        df = pd.concat([pre, gen]).reset_index(drop=True) 
        fld = self.entity.get(selection) 
        if fld is None or fld.name not in list(df.columns): 
            return pd.Series(dtype=object) 
        return df[fld.name] 



    def get_data( self,
        include: list[str | type] | None = None, 
        exclude: list[str | type] | None = None, 
        preexisting: bool = True, 
        generated: bool = True, 
    ) -> pd.DataFrame:
      pre = self.preexisting if preexisting else pd.DataFrame()
      gen = self.generated if generated else pd.DataFrame()
      
      df = pd.concat([pre, gen], axis=0).reset_index(drop=True)
      flds = self.entity.select(include, exclude)
      selection = [ f.name for f in flds if f.name in list(df.columns)]
      if selection:
        return df[selection]
      return pd.DataFrame()
  

