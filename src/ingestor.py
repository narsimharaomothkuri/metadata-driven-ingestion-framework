import argparse
import json
from pathlib import Path

from pyspark.sql import SparkSession

from util.logger import get_logger
from util import job_config
from util.job_config import JobConfig


logger = get_logger('ingestor')


def get_job_config(job_name:str):
    config_location = f"{Path.cwd()}/configs/sources_config.json"

    job:JobConfig = job_config.get_job_config(config_location, job_name)
    print(job._source_location)


def create_spark_session(name:str) -> SparkSession:

    spark = SparkSession \
        .builder \
        .appName(name) \
        .getOrCreate()
    
    return spark


def main():

    parser = argparse.ArgumentParser()
    parser.add_argument("--job_name", type=str, required=True)
    args = parser.parse_args()
    print(f"job_name : {args.job_name}")
    get_job_config(args.job_name)
    

    # spark = create_spark_session("ingestion")

    


if __name__ == "__main__":
    main()