import pandas as pd 
from dataclasses import dataclass, field
from typing import Any

from data_simulator.interface import IAnnotation, IEntityField




@dataclass
class DataSimulationReport:
    _reports: dict[tuple, Any] = field(default_factory=dict)

    def update(self, entity, fld: IEntityField, annotation: IAnnotation, result: Any) -> None:
      self._reports[(entity.__name__, fld.name, annotation.__class__.__name__)] = result

    def get_field(self, entity, fieldname: str) -> dict:
      return {k: v for k, v in self._reports.items() if k[0] == entity and k[1] == fieldname}

    def get_entity(self, entity) -> dict:
      return {k: v for k, v in self._reports.items() if k[0] == entity}

    def failures(self) -> list[tuple]:
      return [
        (entity, fieldname, ann, res) 
        for (entity, fieldname, ann), res in self._reports.items() 
        if isinstance(res, Exception) 
      ]
    
    def summary(self) -> list[tuple]:
      return [
        (entity, fieldname, ann, res.shapes[0] if isinstance(res, pd.Series) else res ) 
        for (entity, fieldname, ann), res in self._reports.items() 
      ]



class DataSimulatorException(Exception):
    def __init__(self, failures: list[tuple]):
      df_failures = pd.DataFrame(failures, columns=['entity', 'field', 'annotation', 'failure'])
      self.failures = df_failures
      super().__init__(f"Data simulation failed with {len(df_failures)} failure(s):\n{df_failures.to_string()}")
