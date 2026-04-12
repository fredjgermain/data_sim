import pandas as pd 
from dataclasses import dataclass, field
import datetime

from data_simulator.interface import IAnnotation, IEntity, IEntityField, IEntityContext
from data_simulator.annotations.primaries import ( PrimaryKey, ForeignKey, CreationTime ) 
from data_simulator.annotations.generator import IGen, Transformer 
from data_simulator.annotations.fault import IFault 
from data_simulator.annotations.validation import IValid
from data_simulator.annotations.factory_ctx import FactoryCtx

from data_simulator.context import EntityContext 
from data_simulator.entity import Entity, EntityField 
from data_simulator.faultmap import FaultMap 
from data_simulator.report_exception import DataSimulationReport, DataSimulatorException 



# DataSimulator =====================================================
@dataclass 
class DataSimulator: 
    entities: dict[type[Entity], EntityContext] 
    _report: DataSimulationReport = field(default_factory=DataSimulationReport) 


    def get_data(self, preexisting=True, generated=True) -> dict[type[Entity], pd.DataFrame]:
      return {entity: ctx.get_data(preexisting=preexisting, generated=generated) for entity, ctx in self.entities.items()}
    
    def get_failures(self) -> pd.DataFrame:
      return pd.DataFrame(self._report.failures(), columns=['entity', 'field', 'annotation', 'failure'])
    
    def get_summary(self) -> pd.DataFrame:
      return pd.DataFrame(self._report.summary(), columns=['entity', 'field', 'annotation', 'result'])


    # ! Simulation ----------------------------------------
    def simulate(self) -> None:
        self._pass_primary_keys() # ! pass 1 
        self._pass_foreign_keys() # ! pass 2 
        self._pass_creation_times() # ! pass 3 
        self._pass_standard_generation() # ! pass 4 
        
        # If any simulation failure is reported, then it raises DataSimulatorException
        failures = self._report.failures()
        if failures:
          raise DataSimulatorException(failures)
    

    def _pass_primary_keys(self) -> None: 
        for entity, ctx in self.entities.items(): 
          pk_fld = entity.get(PrimaryKey) 
          if pk_fld is None: 
            continue 
          ann = pk_fld.get(PrimaryKey) 
          try:
            pk_ctx = FactoryCtx.make_pkctx(ctx) 
            serie = ann.generate(pk_ctx) 
            ctx.generated[pk_fld.name] = self._coerce_column(serie, pk_fld.base_type) 
            
            if serie.empty:
              raise Exception(f'{entity.__name__}.{pk_fld.name} has generated 0 primary keys') 
            self._report.update(entity, pk_fld, ann, serie)
          except Exception as e:
            self._report.update(entity, pk_fld, ann, e)

    def _pass_foreign_keys(self) -> None: 
        for entity, ctx in self.entities.items(): 
          for fld in entity.get([ForeignKey]): 
            ann = fld.get(ForeignKey) 
            try:
              fk_ctx = FactoryCtx.make_fkctx(fld.name, ctx, self.entities)
              serie = ann.generate(fk_ctx) 
              ctx.generated[fld.name] = self._coerce_column(serie, fld.base_type) 
              
              if serie.empty:
                raise Exception(f'{entity.__name__}.{fk_ctx.name} has generated 0 foreign keys') 
              self._report.update(entity, fld, ann, serie) 
            except Exception as e: 
              self._report.update(entity, fld, ann, e) 

    def _pass_creation_times(self) -> None:
        for entity, ctx in self.entities.items(): 
          ct_fld = entity.get(CreationTime) 
          if ct_fld is None:
            continue
          ann = ct_fld.get(CreationTime) 
          try:
            ct_ctx = FactoryCtx.make_ctctx(ct_fld.name, ctx, self.entities) 
            serie = ann.generate(ct_ctx) 
            ctx.generated[ct_fld.name] = self._coerce_column(serie, ct_fld.base_type) 
            
            if serie.empty:
              raise Exception(f'{entity.__name__}.{ct_fld.name} has generated 0 creation time') 
            self._report.update(entity, ct_fld, ann, serie) 
          except Exception as e: 
            self._report.update(entity, ct_fld, ann, e) 

    def _pass_standard_generation(self) -> None: 
        for entity, ctx in self.entities.items(): 
          for fld in entity.get([IGen]): 
            ann_gen = fld.get(IGen) 
            ann_trf = fld.get(Transformer) 
            try:
              gen_ctx = FactoryCtx.make_genctx(fld.name, ctx, self.entities) 
              serie = ann_gen.generate(gen_ctx) 
              if ann_trf:
                serie = ann_trf.transform(serie) 
              ctx.generated[fld.name] = self._coerce_column(serie, fld.base_type) 
              
              if serie.empty:
                raise Exception(f'{entity.__name__}.{fld.name} has generated 0 values') 
              self._report.update(entity, fld, ann_gen, serie) 
            except Exception as e:
              self._report.update(entity, fld, ann_gen, e) 


    # ! Fault Injection -----------------------------------
    def fault_injection(self, fault_maps:dict[type[Entity], type[FaultMap]]) -> None: 
      self._fault_injection(fault_maps) 
      
      # If any simulation failure or fault injection failure is reported, then it raises DataSimulatorException
      failures = self._report.failures()
      if failures:
        raise DataSimulatorException(failures)
    
      
    def _fault_injection(self, fault_maps:dict[type[Entity], type[FaultMap]]) -> None: 
      for entity, fault_map in fault_maps.items(): 
        ctx = self.entities[entity] 
        for fld in fault_map.get([IFault]): 
          for ann in fld.get_many(IFault): 
            fault_ctx = FactoryCtx.make_faultctx(fld.name, ctx) 
            try:
              serie = ann.inject(fault_ctx) 
              diff = serie[ctx.generated[fld.name]!=serie]
              ctx.generated[fld.name] = serie 
              
              if diff.empty:
                raise Exception(f'{entity.__name__}.{fld.name} has generated 0 faulty values') 
              self._report.update(entity, fld, ann, diff) 
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
              self._report.update(entity, fld, ann, res) 
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


