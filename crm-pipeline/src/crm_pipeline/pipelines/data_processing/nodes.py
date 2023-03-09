"""
This is a boilerplate pipeline 'data_processing'
generated using Kedro 0.18.6
"""

from curses import raw
import pandas as pd
import logging

logging.basicConfig(format="%(asctime)s - %(message)s", level=logging.INFO)
log = logging.getLogger("kedro")


def _remove_na(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Removing NAs")
    return df.dropna()


def _remove_empty_space(x: pd.Series) -> pd.Series:
    log.info("Stripping columns")
    return x.str.strip()


def _remove_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Removing duplicates")
    return df.drop_duplicates()


def _parse_month(x: pd.Series) -> pd.Series:
    log.info("Parsing Month from DateTime")
    x = pd.to_datetime(x).dt.strftime("%m-%Y")
    return x


def _lower_column_names(df: pd.DataFrame) -> pd.DataFrame:
    log.info("Lowering column names")
    df.columns = map(str.lower, df.columns)
    return df


def preprocess_crm(crm: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the data for crm table.

    Args:
        crm: Raw data.
    Returns:
        Preprocessed data, that null customers are removed and string columns stripped
    """
    crm = _remove_duplicates(crm)
    crm = _remove_na(crm)
    crm = _lower_column_names(crm)
    crm["occupation"] = _remove_empty_space(crm["occupation"])
    crm["type"] = _remove_empty_space(crm["type"])
    return crm


def preprocess_interactions(interactions: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the data for interactions.

    Args:
        interactions: Raw data.
    Returns:
        Preprocessed data, with a 'month_of_year'column extracted from date_start,
        `event` column is stripped.
    """
    interactions = _remove_duplicates(interactions)
    interactions = _lower_column_names(interactions)
    interactions = _remove_na(interactions)
    interactions["month_of_year"] = _parse_month(interactions["date_start"])
    interactions["event"] = _remove_empty_space(interactions["event"])
    return interactions


def preprocess_products(products: pd.DataFrame) -> pd.DataFrame:
    """Preprocesses the data for products table.

    Args:
        products: Raw data.
    Returns:
        Preprocessed data, that null rows are removed and if any, duplicates will be removed
    """
    products = _remove_duplicates(products)
    products = _lower_column_names(products)
    products = _remove_na(products)
    return products


def _create_input_table(
    crm: pd.DataFrame, interactions: pd.DataFrame, products: pd.DataFrame
) -> pd.DataFrame:
    """Combines all data to create the output table

    Args:
        crm: Preprocessed data for crm.
        interactions: Preprocessed data for interactions.
        products: Raw data for products.
    Returns:
        output.

    """
    log.info("---Creating Input Table---")
    crm_interactions = crm.merge(
        interactions, left_on="customer_id", right_on="customers"
    )
    raw_output_table = crm_interactions.merge(
        products, left_on="month_of_year", right_on="date"
    )
    raw_output_table.rename(columns={"product": "product_of_interaction"}, inplace=True)
    df = raw_output_table[
        ["customer_id", "occupation", "type", "product_of_interaction", "event"]
    ]
    df["freq"] = 1
    return df


def create_pivot_table(
    crm: pd.DataFrame, interactions: pd.DataFrame, products: pd.DataFrame
) -> pd.DataFrame:
    """Creates a Pivotted Output Table

    Args:
        crm: Preprocessed data for crm.
        interactions: Preprocessed data for interactions.
        products: Raw data for products.
    Returns:
        Pivotted Output.

    """
    model_input_table = _create_input_table(crm, interactions, products)
    log.info("---Creating Pivot Table---")
    df_pivot = model_input_table.pivot_table(
        index=["customer_id", "occupation", "type", "product_of_interaction"],
        columns=["event"],
        values="freq",
        aggfunc="count",
    )
    return df_pivot
