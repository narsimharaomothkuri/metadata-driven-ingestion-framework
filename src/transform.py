from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, col


def apply_transformations(df:DataFrame, transform_operations:dict) -> DataFrame:

    return df