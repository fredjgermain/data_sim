
import pandas as pd
from dataclasses import dataclass 


# ! TEMPORARY STRUCTURE 
@dataclass 
class MetaFrame: 
  data:pd.DataFrame 
  encoding:dict # ? temporary 
