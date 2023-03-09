"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.6
"""

from kedro.pipeline import Pipeline, node, pipeline

from .nodes import create_pivot_table, preprocess_crm, preprocess_interactions, preprocess_products


def create_pipeline(**kwargs) -> Pipeline:
    return pipeline(
        [
            node(
                func=preprocess_crm,
                inputs="crm",
                outputs="preprocessed_crm",
                name="preprocess_crm_node",
            ),
            node(
                func=preprocess_interactions,
                inputs="interactions",
                outputs="preprocessed_interactions",
                name="preprocess_interactions_node",
            ),
            node(
                func=preprocess_products,
                inputs="products",
                outputs="preprocessed_products",
                name="preprocess_products_node"

            ),
            node(
                func=create_pivot_table,
                inputs=["preprocessed_crm","preprocessed_interactions","preprocessed_products"],
                outputs="output",
                name="create_pivot_table_node"
            )
        ]
    )
