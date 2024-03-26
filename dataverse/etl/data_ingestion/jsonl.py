"""
Copyright (c) 2024-present Upstage Co., Ltd.
Apache-2.0 license
"""

from typing import List, Union
from pyspark.rdd import RDD
from dataverse.etl import register_etl

@register_etl
def data_ingestion___jsonl___jsonl2raw(
    spark, path: Union[str, List[str]], repartition=20, _args=None, **kwargs
) -> RDD:
    """
    Reads JSONL files into an RDD and repartitions it.

    Args:
        spark (SparkSession): The Spark session.
        path (str or list): The path of the JSONL files.
        repartition (int): The number of partitions.
        _args (optional): Additional arguments. Defaults to None.

    Returns:
        rdd: The repartitioned RDD containing the data from the JSONL files.
    """
    if isinstance(path, str):
        path = [path]

    rdd = spark.sparkContext.textFile(",".join(path))
    rdd = rdd.repartition(repartition)
    rdd = rdd.map(lambda line: eval(line))

    return rdd