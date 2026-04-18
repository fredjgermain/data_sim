import pandas as pd 
from dataclasses import dataclass, field 
from typing import Any 

from _common.interface import Profile, Field
from fault_injection.annotations import IFault, FaultCtx



@dataclass
class FaultReport:
    _reports: dict[tuple, Any] = field(default_factory=dict)

    def update(self, fld: Field, annotation: IFault, result: Any) -> None:
      self._reports[(fld.name, annotation.__class__.__name__)] = result

    def failures(self) -> list[tuple]:
      return [
        (fieldname, ann, res) 
        for (fieldname, ann), res in self._reports.items() 
        if isinstance(res, Exception) 
      ]
    
    def summary(self) -> list[tuple]:
      return [
        (fieldname, ann, res.shapes[0] if isinstance(res, pd.Series) else res ) 
        for (fieldname, ann), res in self._reports.items() 
      ] 
      

class ValidationProfile(Profile):
  
  
    @classmethod 
    def inject(cls, data:pd.DataFrame) -> FaultReport: 
      report = FaultReport()
      for name, fld in cls.inspect().items(): 
        for ann in fld.get_many(IFault): 
          res = ann.inject(FaultCtx(name, data)) 
          report.update(fld, ann, res) 
      return report