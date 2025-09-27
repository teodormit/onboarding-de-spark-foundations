from pyspark.sql import DataFrame, SparkSession

def read_raw_json(spark: SparkSession, path_glob: str) -> DataFrame:
    return spark.read.json(path_glob, multiLine=True)

def write_parquet_partitioned(df: DataFrame, out_path: str, partition_col: str, mode: str = "overwrite"):
    (df.write.mode(mode)
       .partitionBy(partition_col)
       .parquet(out_path))