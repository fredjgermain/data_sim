import pandas as pd 
from dataclasses import dataclass, field 

from src.annotations.primaries import PrimaryKey, CreationTime 
from src.interface import IEntity, IEntityContext, IEntityField 



@dataclass
class EntityContext(IEntityContext):

    entity:      type[IEntity]
    preexisting: pd.DataFrame
    N:           int
    generated:   pd.DataFrame = field(default_factory=pd.DataFrame)


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
        selection: list[str | type] = None, 
        preexisting: bool = True, 
        generated: bool = True, 
    ) -> pd.DataFrame:
      pre = self.preexisting if preexisting else pd.DataFrame()
      gen = self.generated if generated else pd.DataFrame()
      
      df = pd.concat([pre, gen], axis=0).reset_index(drop=True)
      flds = self.entity.get(selection)
      
      selection = [ f.name for f in flds if f.name in list(df.columns)]
      if selection:
        return df[selection]
      return pd.DataFrame()
  

