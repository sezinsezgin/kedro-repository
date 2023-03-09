import pandas as pd
import numpy as np
import pytest
from crm_pipeline.pipelines.data_processing.nodes import preprocess_crm,preprocess_interactions,preprocess_products,create_pivot_table

#create df with duplication and nulls
@pytest.fixture
def sample_crm():
    crm = pd.DataFrame({
        'Customer_id': [1, 2, 3, 4, 5,5],
        'occupation': ['Jedi', 'Batman', 'Doctor', 'Engineer', 'Teacher','Teacher'],
        'type': ['A', 'B', 'A', 'B', 'A','A']
    })
    return crm

@pytest.fixture
def sample_interactions():
    interactions = pd.DataFrame({
        'customers': [1, 2, 3, 3, 4,np.nan],
        'date_start': ['2021-01-01', '2021-02-02', '2021-01-01', '2021-02-02', '2021-01-01',np.nan],
        'event': ['email', 'call', 'bird', 'call', 'email',np.nan]
    })
    return interactions

@pytest.fixture
def sample_products():
    products = pd.DataFrame({
        'date': ['01-2021', '02-2021', '01-2021', '04-2021', '05-2021'],
        'product': ['a', 'b', 'a', 'b', 'c']
    })
    return products

def test_preprocess_crm(sample_crm):
    preprocessed_crm = preprocess_crm(sample_crm)
    assert len(preprocessed_crm) == 5
    assert preprocessed_crm.columns[0] == 'customer_id'

def test_preprocess_interactions(sample_interactions):
    preprocessed_interactions = preprocess_interactions(sample_interactions)
    assert len(preprocessed_interactions) == 5
    assert preprocessed_interactions['event'].nunique() == 3

def test_preprocess_products(sample_products):
    preprocessed_products = preprocess_products(sample_products)
    assert len(preprocessed_products) == 4
    assert preprocessed_products['product'].nunique() == 3

def test_create_pivot_table(sample_crm, sample_interactions, sample_products):
    preprocessed_crm = preprocess_crm(sample_crm)
    preprocessed_interactions = preprocess_interactions(sample_interactions)
    preprocessed_products = preprocess_products(sample_products)
    pivot_table = create_pivot_table(preprocessed_crm, preprocessed_interactions, preprocessed_products)
    assert len(pivot_table) == 5
    assert isinstance(pivot_table, pd.DataFrame)



