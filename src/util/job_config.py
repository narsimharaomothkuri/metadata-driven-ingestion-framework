
import json
from typing import List, Dict
from pathlib import Path

from util.logger import get_logger

logger = get_logger("job_config")

class JobConfig:

    def __init__(self, name:str,
                 source_name:str,
                 source_type:str,
                 source_location:str,
                 delimeter:str,
                 has_header:bool,
                 load_type:str,
                 destination_location:str,
                 transform_operations:dict):
        self._name = name
        self._source_name = source_name
        self._source_type = source_type
        self._source_location = source_location
        self._delimeter = delimeter
        self._has_header = str
        self._load_type = load_type
        self._destination_location = destination_location
        self._transform_operations = transform_operations
        logger.info("JobConfig Object created")

def get_job_config(config_location : str, job_name:str) -> JobConfig :
    logger.info("get_job_config starts")
    configs = {}
    try:

        with open(config_location, 'r') as file:
            configs = json.load(file)
    except FileNotFoundError as fnfe:
        logger.error(f"Erorr: The file {config_location} was not found")
        raise fnfe
    except Exception as e:
        logger.error(f"An unexpected error occured : {e}")
        raise e

    try :
        for config in configs:
            # logger.info(f"configuration file current job is {config.get('job_name')} and finding job is {job_name}")
            if job_name == config.get("job_name") :
                job_name = config.get('job_name')
                source_name = config.get('source_name')
                source_type = config.get('source_type')
                source_location = config.get('source_location')
                delimiter = config.get('delimiter')
                has_header = True
                if config.get('has_header') != 1 :
                    has_header = False
                load_type = config.get('load_type')
                destination_location= config.get('destination_location')
                transform_rules = config.get('transform_rules')
                job_config = JobConfig(name=job_name, 
                                    source_name=source_name, 
                                    source_type=source_type, 
                                    source_location=source_location, 
                                    delimeter=delimiter,
                                    has_header=has_header, 
                                    load_type=load_type, 
                                    destination_location=destination_location, 
                                    transform_rules=transform_rules)
                print(job_config._destination_location)
                # logger.info(job_config._destination_location)
                logger.info("get_job_config ends")
                return job_config
    except Exception as e:
        logger.error("Unexcepted Error occured {e}") 
        raise e     

    logger.info(f"finding job name {job_name} not found")

    

