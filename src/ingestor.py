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
        logger.error("Unknown Error encounterd as :{e}")
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
    
    #load the data
    df = spark \
        .read \
        .csv \
        .option("delimiter", config._delimeter) \
        .option("header", config._has_header) \
        .option("inferSchema", True) \
        .load(config._source_location)
    
    df = apply_transformations(df, config._transform_operations)

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
        logger.info("Unknown Error encountered as : {e}")
    


if __name__ == "__main__":
    main()