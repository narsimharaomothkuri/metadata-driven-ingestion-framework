import argparse
import json
import logging
from pathlib import Path

from pyspark.sql import SparkSession

from util.logger import get_logger
from util import job_config
from util.job_config import JobConfig
from transform import apply_transformations


logger = get_logger('ingestor')


def get_job_config(job_name:str) -> JobConfig:
    
    try :
        config_location = f"{Path.cwd()}/configs/sources_config.json"

        config:JobConfig = job_config.get_job_config(config_location, job_name)
    
        return config
    except Exception as e :
        logger.error(f"Unknown Error encounterd as :{e}")
        raise e


def create_spark_session(name:str) -> SparkSession:

    spark = SparkSession \
        .builder \
        .appName(name) \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    logging.getLogger("py4j").setLevel(logging.ERROR)
    
    return spark

def run_job(config:JobConfig, spark:SparkSession):
    logger.info("run_job start")
    
    file_path = f"{Path.cwd()}{config._source_location}"
    logger.info(f"source path is : {file_path}")
    #load the data
    df = spark \
        .read \
        .option("delimiter", config._delimeter) \
        .option("header", config._has_header) \
        .option("inferSchema", True) \
        .csv(file_path)
    
    logger.info(f"Before  : {df.printSchema()}")

    df = apply_transformations(df, config._transform_operations)

    logger.info(f"After  : {df.printSchema()}")

    logger.info("run_job end")

def main():
    try :
        parser = argparse.ArgumentParser()
        parser.add_argument("--job_name", type=str, required=True)
        args = parser.parse_args()
        logger.info(f"job_name : {args.job_name}")
        config = get_job_config(args.job_name)
        spark = create_spark_session(config._name)
        run_job(config=config, spark=spark)
        spark.stop()

    except Exception as e:
        logger.info(f"Unknown Error encountered as : {e}")
    


if __name__ == "__main__":
    main()