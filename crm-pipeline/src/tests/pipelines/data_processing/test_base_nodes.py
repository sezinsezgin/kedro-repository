import pandas as pd
from numpy import nan

from pytest import mark
from crm_pipeline.pipelines.data_processing.nodes import (
    _parse_month,
    _remove_duplicates,
    _remove_na,
    _lower_column_names,
)

test_data = pd.DataFrame(
    {
        "Customer_id": ["1  ", "2", "3", "3", nan],
        "occupation": ["lawyer    ", "cook", "doc", "doc", nan],
        "event": ["test", "a", "b", "b", "d"],
    }
)


@mark.base_node_functions
def test_remove_na():
    assert len(_remove_na(test_data)) == 4


@mark.base_node_functions
def test_remove_duplicates():
    assert len(_remove_duplicates(test_data)) == 4


@mark.base_node_functions
def test_parse_month():
    test_date = pd.Series("04.10.19 09:00")
    assert _parse_month(test_date).item() == pd.Series("04-2019").item()


@mark.base_node_functions
def test_lower_column_names():
    column_list_set = set(_lower_column_names(test_data).columns)
    output_set = set(["customer_id", "occupation", "event"])
    assert column_list_set == output_set
