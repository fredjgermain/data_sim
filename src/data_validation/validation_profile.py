import pandas as pd 
from dataclasses import dataclass, field 
from typing import Any 

from _common.interface import Profile, Field
from data_validation.annotations import IValid, ValidCtx



@dataclass
class ValidationReport:
    _reports: dict[tuple, Any] = field(default_factory=dict)

    def update(self, fld: Field, annotation: IValid, result: Any) -> None:
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
    def validate(cls, data:pd.DataFrame) -> ValidationReport: 
      report = ValidationReport()
      for name, fld in cls.inspect().items(): 
        for ann in fld.get_many(IValid): 
          res = ann.validate(ValidCtx(name, data)) 
          report.update(fld, ann, res) 
      return report