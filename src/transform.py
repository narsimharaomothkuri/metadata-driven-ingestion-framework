from pyspark.sql import DataFrame
from pyspark.sql.functions import expr, col
from pyspark.sql.types import DoubleType, FloatType, StringType, IntegerType, StructType

from util.logger import get_logger




logger = get_logger("apply_transformations")

column_maps = {
    "int" : IntegerType(),
    "double" : DoubleType(),
    "float" : FloatType(),
    "string" : StringType(),
    "struct" : StructType()
}

def apply_rename_columns(df:DataFrame, rename_columns:dict) -> DataFrame :
    try:
        for key, value in rename_columns.items():
            logger.info(f"renaming column {key} to {value}")
            df = df.withColumnRenamed(key, value)
        return df
    except Exception as e:
        logger.error(f"Unknown Exception encountered as {e}")
        raise e
    
def apply_drop_columns(df:DataFrame, drop_columns:list) -> DataFrame:
    try:
        for col in drop_columns:
            df = df.drop(col)
        return df 
    except Exception as e:
        logger.error(f"Unknown Exception as encountered as {e}")
        raise e

def apply_cast_columns(df:DataFrame, cast_columns:dict) -> DataFrame:
    try:
        for key, value in cast_columns.items():
            df = df.withColumn(key, df[key].cast(column_maps.get(value)))
        return df
    except Exception as e:
        logger.error(f"Unknow Exception as encountered as {e}")
        raise e

def apply_transformations(df:DataFrame, transform_operations:dict) -> DataFrame:
    
    try:

        rename_column_operations = transform_operations.get('rename')

        if rename_column_operations is not None:
            df = apply_rename_columns(df, rename_column_operations)

        drop_column_operations = transform_operations.get("drop")

        if drop_column_operations is not None:
            df = apply_drop_columns(df, drop_column_operations)

        cast_column_operations = transform_operations.get("cast")

        if cast_column_operations is not None:
            df = apply_cast_columns(df, cast_column_operations)
    
        return df
    except Exception as e:
        logger.error(f"Unexpected Error encountered as : {e}")
        raise e

    