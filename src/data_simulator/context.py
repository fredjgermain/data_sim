import pandas as pd 
from dataclasses import dataclass, field 

from data_simulator.interface import IEntity, IEntityContext



@dataclass
class EntityContext(IEntityContext):

    entity:      type[IEntity]
    N:           int
    preexisting: pd.DataFrame = field(default_factory=pd.DataFrame)
    generated:   pd.DataFrame = field(default_factory=pd.DataFrame)


    def get_serie(self, 
        selection: str | type, 
        preexisting: bool = True, 
        generated: bool = True
    ) -> pd.Series:
        '''
        Receives a selection (str or annotation type), 
        Return a single corresponding pd.Serie. 
            - empty serie if there's no corresponding field. 
            - empty serie with name if there no data but a corresponding field. 
            - serie with data and name if there's data and corresponding field. 
        '''
        
        pre = self.preexisting if preexisting else pd.DataFrame() 
        gen = self.generated if generated else pd.DataFrame() 
        
        df = pd.concat([pre, gen]).reset_index(drop=True) 
        fld = self.entity.get(selection) 
        
        if fld is None: 
            return pd.Series(dtype=object) 
        if fld.name not in list(df.columns): 
            return pd.Series(name=fld.name) 
        # if fld is None or fld.name not in list(df.columns): 
        #     return pd.Series(dtype=object) 
        return df[fld.name] 


    def get_data( self,
        selection: list[str | type] = None, 
        preexisting: bool = True, 
        generated: bool = True, 
    ) -> pd.DataFrame:
      pre = self.preexisting if preexisting else pd.DataFrame()
      gen = self.generated if generated else pd.DataFrame()
      
      df = pd.concat([pre, gen], axis=0).reset_index(drop=True)
      
      if selection is None:
        return df 
      
      flds = self.entity.get(selection)
      selection = [ f.name for f in flds if f.name in list(df.columns)]
      if not selection: 
        return pd.DataFrame() 
        #raise KeyError(f"None of the selected fields were found in the DataFrame.")
      return df[selection]

