import pandas as pd 
from dataclasses import dataclass, field
import datetime
from typing import Any

from data_simulator.interface import IAnnotation, IEntity, IEntityField, IEntityContext
from data_simulator.annotations.primaries import ( PrimaryKey, ForeignKey, CreationTime ) 
from data_simulator.annotations.generator import IGen, Transformer 
from data_simulator.annotations.fault import IFault 
from data_simulator.annotations.validation import IValid
from data_simulator.annotations.factory_ctx import FactoryCtx

from data_simulator.context import EntityContext 
from data_simulator.entity import Entity, EntityField 
from data_simulator.faultmap import FaultMap

from dataclasses import dataclass, field



@dataclass
class DataSimulationReport:
    reports: dict[type[Entity], dict] = field(default_factory=dict)

    def update(self, entity, fld: IEntityField, annotation: IAnnotation, success: Any) -> None:
        self.reports.setdefault(entity, {}).setdefault(fld.name, {})[annotation.__class__.__name__] = success

    def get_field(self, entity, fieldname: str) -> dict:
        """Get all annotations for a given entity + field."""
        return self.reports.get(entity, {}).get(fieldname, {})

    def get_entity(self, entity) -> dict:
        """Get all fields and annotations for a given entity."""
        return self.reports.get(entity, {})
      
    def __str__(self) -> str:
      lines = []
      for entity, fields in self.reports.items():
          lines.append(f"\n\n=== {entity.__name__} ===") 
          for fieldname, annotations in fields.items():
              lines.append(f"\n  {fieldname}: ")
              for annotation_name, success in annotations.items():
                  status = "✓" if success else "✗"
                  lines.append(f"    {status} {annotation_name}")
      return "".join(lines)


# DataSimulator =====================================================
@dataclass 
class DataSimulator: 
    entities: dict[type[Entity], EntityContext] 
    _report: DataSimulationReport = field(default_factory=DataSimulationReport) 


    def get_data(self, preexisting=True, generated=True) -> dict[type[Entity], pd.DataFrame]:
      return {entity: ctx.get_data(preexisting=preexisting, generated=generated) for entity, ctx in self.entities.items()}


    # ! Simulation ----------------------------------------
    def simulate(self) -> None:
        self._pass_primary_keys() # ! pass 1 
        self._pass_foreign_keys() # ! pass 2 
        self._pass_creation_times() # ! pass 3 
        self._pass_standard_generation() # ! pass 4 


    def _pass_primary_keys(self) -> None: 
        for entity, ctx in self.entities.items(): 
          pk_fld = entity.get(PrimaryKey) 
          if pk_fld is None: 
            continue 
          ann = pk_fld.get(PrimaryKey) 
          pk_ctx = FactoryCtx.make_pkctx(ctx) 
          try:
            serie = ann.generate(pk_ctx) 
            ctx.generated[pk_fld.name] = self._coerce_column(serie, pk_fld.base_type) 
            self._report.update(entity, pk_fld, ann, True)
          except Exception as e:
            self._report.update(entity, pk_fld, ann, e)

    def _pass_foreign_keys(self) -> None: 
        for entity, ctx in self.entities.items(): 
          for fld in entity.get([ForeignKey]): 
            ann = fld.get(ForeignKey) 
            fk_ctx = FactoryCtx.make_fkctx(fld.name, ctx, self.entities)
            try:
              serie = ann.generate(fk_ctx) 
              ctx.generated[fld.name] = self._coerce_column(serie, fld.base_type) 
              self._report.update(entity, fld, ann, True) 
            except Exception as e: 
              self._report.update(entity, fld, ann, e) 

    def _pass_creation_times(self) -> None:
        for entity, ctx in self.entities.items(): 
          ct_fld = entity.get(CreationTime) 
          if ct_fld is None:
            continue
          ann = ct_fld.get(CreationTime) 
          ct_ctx = FactoryCtx.make_ctctx(ct_fld.name, ctx, self.entities) 
          try:
            serie = ann.generate(ct_ctx) 
            ctx.generated[ct_fld.name] = self._coerce_column(serie, ct_fld.base_type) 
            self._report.update(entity, ct_fld, ann, True) 
          except Exception as e: 
            self._report.update(entity, ct_fld, ann, e) 

    def _pass_standard_generation(self) -> None: 
        for entity, ctx in self.entities.items(): 
          for fld in entity.get([IGen]): 
            ann_gen = fld.get(IGen) 
            ann_trf = fld.get(Transformer) 
            gen_ctx = FactoryCtx.make_genctx(fld.name, ctx, self.entities) 
            try:
              serie = ann_gen.generate(gen_ctx) 
              if ann_trf:
                serie = ann_trf.transform(serie) 
              ctx.generated[fld.name] = self._coerce_column(serie, fld.base_type) 
              self._report.update(entity, fld, ann_gen, True) 
            except Exception as e:
              self._report.update(entity, fld, ann_gen, e) 


    # ! Fault Injection -----------------------------------
    def fault_injection(self, fault_maps:dict[type[Entity], type[FaultMap]]) -> None: 
      for entity, fault_map in fault_maps.items(): 
        ctx = self.entities[entity] 
        for fld in fault_map.get([IFault]): 
          for ann in fld.get_many(IFault): 
            fault_ctx = FactoryCtx.make_faultctx(fld.name, ctx) 
            try:
              serie = ann.inject(fault_ctx) 
              ctx.generated[fld.name] = serie 
              self._report.update(entity, fld, ann, True)
            except Exception as e: 
              self._report.update(entity, fld, ann, e)
  
  
    # ! Validation ----------------------------------------
    def validation(self) -> None: 
      for entity, ctx in self.entities.items(): 
        for fld in entity.get([IValid]): 
          valid_ctx = FactoryCtx.make_validctx(fld.name, ctx)
          for ann in fld.get_many(IValid): 
            try:
              res = ann.validate(valid_ctx) 
              self._report.update(entity, fld, ann, True) 
            except Exception as e: 
              self._report.update(entity, fld, ann, e)


    def _coerce_column(self, series: pd.Series, base_type: type) -> pd.Series:
        type_map = {
            int:                'int64',
            float:              'float64',
            str:                'object',
            bool:               'bool',
            datetime.datetime:  'datetime64[ns]',
        }
        dtype = type_map.get(base_type)
        if dtype is None:
            return series
        try:
            return series.astype(dtype)
        except Exception:
            return series


