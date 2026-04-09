import pandas as pd 
from dataclasses import dataclass, field
import datetime
from typing import Any

from src.interface import IAnnotation 
from src.annotations.primaries import ( 
    PkCtx, PrimaryKey, FkCtx, ForeignKey, CtCtx, CreationTime 
    ) 
from src.annotations.generator import IGen, GenCtx, Transformer 
from src.annotations.fault import IFault, FaultCtx 
from src.annotations.validation import IValid, ValidCtx 
from src.annotations.factory_ctx import FactoryCtx

from src.context import EntityContext 
from src.entity import Entity, EntityField 



@dataclass 
class EntityReport: 
    entity_name: str 
    fld_report: dict[str, FieldReport] 


@dataclass
class FieldReport:
    fld_name: str 
    expected_N: int 
    results: pd.Series = field(default_factory=dict) 
    error: Any | None = None


# ---------------------------------------------------------------------------
# DataSimulator
# ---------------------------------------------------------------------------

@dataclass 
class DataSimulator: 
    entities: dict[type[Entity], EntityContext] 
    
    report: dict[type[Entity], EntityReport] = field(default_factory=dict) 
    
    def __post_init__(self) -> None:
        # ! initiate sim reports 
        for ent, ctx in self.entities.items(): 
            self._init_report(ent, ctx) 
        
    def _init_report(self, entity:type[Entity], ctx:EntityContext) -> None: 
        flds = entity.inspect().items() 
        fld_reports = { k:FieldReport(fld_name=fld.name, expected_N= ctx.N) for k, fld in flds } 
        self.report[entity] = EntityReport( 
            entity_name=entity.__name__, 
            fld_report= fld_reports 
        ) 


    # Public API ==========================================
    def simulate(self) -> dict[type[Entity], pd.DataFrame]:
        
        self._pass_primary_keys() # ! pass 1 
        self._pass_foreign_keys() # ! pass 2 
        self._pass_creation_times() # ! pass 3 
        self._pass_standard_generation() # ! pass 4 
        self._pass_fault_injection() # ! pass 5 
        self._pass_validation() # ! pass 6 
        
        return {entity: ctx.get_data() for entity, ctx in self.entities.items()}

    # ! Pass 1 
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
                self._update_report(entity, pk_fld, ann, serie) 
            except Exception as e:
                self._update_report(entity, pk_fld, ann, error=e) 

    # ! Pass 2 
    def _pass_foreign_keys(self) -> None: 
        for entity, ctx in self.entities.items(): 
            for fld in entity.get([ForeignKey]): 
                ann = fld.get(ForeignKey) 
                fk_ctx = FactoryCtx.make_fkctx(fld.name, ctx, self.entities)
                try:
                    serie = ann.generate(fk_ctx) 
                    ctx.generated[fld.name] = self._coerce_column(serie, fld.base_type) 
                    self._update_report(entity, fld, ann, serie) 
                except Exception as e:
                    self._update_report(entity, fld, ann, error=e) 

    # ! Pass 3
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
                self._update_report(entity, ct_fld, ann, serie) 
            except Exception as e: 
                self._update_report(entity, ct_fld, ann, error=e) 

    # ! pass 4 
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
                    self._update_report(entity, fld, ann_gen, serie) 
                except Exception as e:
                    self._update_report(entity, fld, ann_gen, error=e) 
    
    # ! Pass 5: Fault injection
    def _pass_fault_injection(self) -> None:
        for entity, ctx in self.entities.items():
            for fld in entity.get([IFault]): 
                for ann in fld.get_many(IFault): 
                    fault_ctx = FactoryCtx.make_faultctx(fld.name, ctx) 
                    try:
                        serie = ann.inject(fault_ctx) 
                        ctx.generated[fld.name] = serie 
                        self._update_report(entity, fld, ann, serie) 
                    except Exception as e: 
                        self._update_report(entity, fld, ann, error=e) 
    
    # ! Pass 6: Validation 
    def _pass_validation(self) -> None: 
        for entity, ctx in self.entities.items(): 
            for fld in entity.get([IValid]): 
                valid_ctx = FactoryCtx.make_validctx(fld.name, ctx)
                for ann in fld.get_many(IValid): 
                    try:
                        res = ann.validate(valid_ctx) 
                        self._update_report(entity, fld, ann, res.invalid_values) 
                    except Exception as e: 
                        self._update_report(entity, fld, ann, error=e) 


    # Internal helpers ==================================== 
    def _update_report(self, entity:type[Entity], fld:EntityField, anno:IAnnotation, result:pd.Series = None, error:Exception = None): 
        report = self.report[entity].fld_report[fld.name] 
        k = anno.__class__.__name__ 
        if not result is None:
            report.results = result 
        if not error is None:
            report.error = error 
            
    def print_report(self): 
      for rep in self.report.values(): 
        print(rep.entity_name) 
        for f in rep.fld_report.values(): 
          if f.error: 
            print(f"\t{f.fld_name}, error: {f.error}") 
          else:
            print(f"\t{f.fld_name}, \t\t N:{f.expected_N}/{len(f.results)}") 

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
            # log warning: could not coerce column to base_type
            return series


