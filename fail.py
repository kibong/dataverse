import os
from pathlib import Path
from dataverse.config import Config 
from omegaconf import OmegaConf

# E = Extract, T = Transform, L = Load
ETL_path = "./fail.yaml"

ETL_config = Config.load(ETL_path)
print(OmegaConf.to_yaml(ETL_config))
from dataverse.etl import ETLPipeline

etl_pipeline = ETLPipeline()
# raw -> hf_obj
spark, dataset = etl_pipeline.run(ETL_config)