import pandas as pd 
from dataclasses import dataclass, field 
import datetime

from src.interface import IAnnotation 
from src.annotations.base import GenCtx, IStandardGen, FaultCtx , IFault, IValid, ValidCtx 
from src.annotations.primaries import PrimaryKey, CreationTime, ForeignKey 
from src.annotations.standardgen import Transformer

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
    results: dict[str, pd.Series] = field(default_factory=dict) 
    error: dict[str, pd.Series] = field(default_factory=dict) 


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
            pk_fld = entity.get_primary_key_field()            
            if pk_fld is None:
                continue
            current_data= ctx.get_data(generated=False) 
            gen_ctx = GenCtx(name=pk_fld.name, entity=entity, N=ctx.N, current_data=current_data) 
            ann = pk_fld.get(PrimaryKey) 
            try:
                serie = ann.generate(gen_ctx) 
                ctx.generated[pk_fld.name] = self._coerce_column(serie, pk_fld.base_type) 
                self.update_report(entity, pk_fld, ann, serie) 
            except Exception as e:
                self.update_report(entity, pk_fld, ann, error=e) 

    # ! Pass 2 
    def _pass_foreign_keys(self) -> None: 
        for entity, ctx in self.entities.items(): 
            for fld in entity.find([ForeignKey]): 
                ann = fld.get(ForeignKey) 
                #target = ann.target 
                foreign_datas = { e:c.get_data() for e, c in self.entities.items() } 
                gen_ctx = GenCtx(name=fld.name, entity=entity, N=ctx.N, foreign_datas=foreign_datas) 
                
                try:
                    serie = ann.generate(gen_ctx) 
                    ctx.generated[fld.name] = self._coerce_column(serie, fld.base_type) 
                    self.update_report(entity, fld, ann, serie) 
                except Exception as e:
                    self.update_report(entity, fld, ann, error=e) 

    # ! Pass 3
    def _pass_creation_times(self) -> pd.Series:
        for entity, ctx in self.entities.items():
            ct_fld = entity.get_creation_time_field()
            if ct_fld is None:
                continue
            
            # ! Aggregate creation time to find lower bound 
            df_agg_ct = self._aggregate_creation_time(ctx) 
            gen_ctx = GenCtx(name=ct_fld.name, entity=entity, N=ctx.N, current_data=df_agg_ct) 
            ann = ct_fld.get(CreationTime) 
            try:
                serie = ann.generate(gen_ctx) 
                ctx.generated[ct_fld.name] = self._coerce_column(serie, ct_fld.base_type) 
                self.update_report(entity, ct_fld, ann, serie) 
            except Exception as e: 
                self.update_report(entity, ct_fld, ann, error=e) 


    # ! pass 4
    def _pass_standard_generation(self) -> None:
        for entity, ctx in self.entities.items():
            for fld in entity.select([IStandardGen]):
                current_data = ctx.get_data(preexisting=False) 
                foreign_data = { e:c.get_data() for e, c in self.entities.items() } 
                gen_ctx = GenCtx(name=fld.name, N=ctx.N, entity=entity, current_data=current_data, foreign_datas=foreign_data) 
                ann_gen = fld.get(IStandardGen) 
                ann_trf = fld.get(Transformer) 
                
                try:
                    serie = ann_gen.generate(gen_ctx) 
                    if ann_trf:
                        serie = ann_trf.transform(serie) 

                    ctx.generated[fld.name] = self._coerce_column(serie, fld.base_type) 
                    self.update_report(entity, fld, ann_gen, serie) 
                except Exception as e:
                    self.update_report(entity, fld, ann_gen, error=e) 
    
    # ! Pass 5: Fault injection
    def _pass_fault_injection(self) -> None:
        for entity, ctx in self.entities.items():
            for fld in entity.find([IFault]):
                # get Standard generator annotation 
                current_serie = ctx.get_data(preexisting=False)[fld.name] 
                fault_ctx = FaultCtx(name=fld.name, current_serie=current_serie) 
                for ann in fld.get_many(IFault): 
                    try:
                        serie = ann.inject(fault_ctx) 
                        ctx.generated[fld.name] = serie 
                        self.update_report(entity, fld, ann, serie) 
                    except Exception as e: 
                        print(e)
                        self.update_report(entity, fld, ann, error=e) 
    
    # ! Pass 6: Validation 
    def _pass_validation(self) -> None: 
        for entity, ctx in self.entities.items(): 
            for fld in entity.find([IValid]): 
                current_data = ctx.get_data(preexisting=False)[fld.name] 
                valid_ctx = ValidCtx(name=fld.name, current_serie=current_data) 
                for ann in fld.get_many(IValid): 
                    try:
                        res = ann.validate(valid_ctx) 
                        self.update_report(entity, fld, ann, res.invalid_values) 
                    except Exception as e: 
                        self.update_report(entity, fld, ann, error=e) 


    # Internal helpers ==================================== 
    def update_report(self, entity:type[Entity], fld:EntityField, anno:IAnnotation, result:pd.Series = None, error:Exception = None): 
        report = self.report[entity].fld_report[fld.name] 
        k = anno.__class__.__name__ 
        if not result is None:
            report.results[k] = result 
        if not error is None:
            report.error[k] = error 

    
    def _aggregate_creation_time(self, current_ctx:EntityContext) -> pd.DataFrame: 
        '''Returns a dataframe of creationtime from 0 to N columns'''
        df = current_ctx.get_data([ForeignKey], preexisting=False) 
        
        for i, fld in enumerate(current_ctx.entity.find([ForeignKey])): 
            fk_ann = fld.get(ForeignKey)
            parent_ctx = self._resolve_ctx(fk_ann.target) 
            parent_pk_fld = parent_ctx.entity.get_primary_key_field() 
            parent_ct_fld = parent_ctx.entity.get_creation_time_field() 
            if not parent_pk_fld or not parent_ct_fld: 
                continue 
            
            df_parent = parent_ctx.get_data([PrimaryKey, CreationTime], preexisting=False) # ! must get data on generate not on all data. 
            df_parent = df_parent.rename(columns={parent_ct_fld.name:f"{i}"}) 
            df = pd.merge(df, df_parent, left_on=fld.name, right_on=parent_pk_fld.name, how='left') 
            # drops keys leaving only creationtime 
            df = df.drop(columns=list({fld.name, parent_pk_fld.name})) 
        return df 


    def _resolve_ctx(self, entity: type[Entity]) -> EntityContext:
        if entity not in self.entities:
            raise KeyError(f"Entity '{entity.__name__}' is not registered in this DataSimulator.")
        return self.entities[entity]


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


