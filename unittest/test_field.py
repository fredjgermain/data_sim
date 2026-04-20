
import pytest 
from _common.interface import Field 

from data_simulator.annotations.primaries import PrimaryKey, ForeignKey
from data_simulator.annotations.generator import GenPoisson, GenUniform 

from fault_injection.annotations import Duplicate, Missing, IFault, Outlier 
from data_validation.annotations import ValidRange, Completeness, IValid, Uniqueness 


# Test fixtures
field_primary_key = Field(name="id", base_type=int, 
      annotations={PrimaryKey: PrimaryKey()})

field_poisson = Field(name="wage", base_type=int, 
      annotations={GenPoisson: GenPoisson(min=0, mean=50000)})

field_age = Field(name="age", base_type=int, 
      annotations={GenUniform: GenUniform(min=20, max=120)})

field_duplicate_id = Field(name="id", base_type=int, 
        annotations={
          Duplicate: Duplicate(prob=0.05), 
          Missing: Missing()})

field_valid_age = Field(name="age", base_type=int, 
        annotations={
          ValidRange: ValidRange(min=20, max=120), 
          Completeness: Completeness(tolerance=0)})

field_empty = Field(name="empty", base_type=str, 
        annotations={})

field_many_fault = Field(name="bad_data", base_type=int,
        annotations={
          Duplicate: Duplicate(prob=0.1),
          Missing: Missing(), 
          Outlier: Outlier()}) 

field_many_valid = Field(name="clean_data", base_type=int,
        annotations={
          Completeness: Completeness(tolerance=0),
          Uniqueness: Uniqueness()})


class TestGet:
    @pytest.mark.parametrize('field, query, expected', [
        # Test missing annotations
        (field_primary_key, ForeignKey, None),
        (field_poisson, PrimaryKey, None),
        (field_age, Missing, None),
        (field_empty, PrimaryKey, None),
        
        # Test existing annotations
        (field_primary_key, PrimaryKey, PrimaryKey()),
        (field_poisson, GenPoisson, GenPoisson(min=0, mean=50000)),
        (field_age, GenUniform, GenUniform(min=20, max=120)),
        (field_duplicate_id, Duplicate, Duplicate(prob=0.05)),
        (field_duplicate_id, Missing, Missing()),
        (field_valid_age, ValidRange, ValidRange(min=20, max=120)),
        (field_valid_age, Completeness, Completeness(tolerance=0)),
             
        # Test empty field
        (field_empty, PrimaryKey, None),
        (field_empty, Missing, None),
        (field_empty, GenPoisson, None),
    ])
    def test_get(self, field: Field, query, expected): 
        result = field.get(query) 
        assert result == expected


class TestGetMany:
    @pytest.mark.parametrize('field, query, expected', [
        # Test with no matches
        (field_empty, IFault, []),
        (field_empty, IValid, []),
        
        # Test with matches
        (field_valid_age, IValid, [ValidRange(min=20, max=120), Completeness(tolerance=0)]),
        (field_duplicate_id, IFault, [Duplicate(prob=0.05), Missing()]),
        (field_many_fault, IFault, [Duplicate(prob=0.1), Missing(), Outlier()]),
        (field_many_valid, IValid, [Completeness(tolerance=0), Uniqueness()]),
        
        # Test primary/foreign keys
        (field_primary_key, PrimaryKey, [PrimaryKey()]),
    ])
    def test_get_many(self, field: Field, query, expected): 
        result = field.get_many(query) 
        assert result == expected


class TestHas:
    @pytest.mark.parametrize('field, query, expected', [
        # Test missing interfaces
        (field_valid_age, IFault, False),
        (field_poisson, IValid, False),
        (field_empty, IValid, False),
        
        # Test existing interfaces
        (field_valid_age, IValid, True),
        (field_duplicate_id, IFault, True),
        (field_primary_key, PrimaryKey, True),
    ])
    def test_has(self, field: Field, query, expected): 
        result = field.has(query) 
        assert result == expected

