# tests/python/conftest.py
import os
import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    # Quiet Spark logs for tests
    os.environ["PYSPARK_PYTHON"] = os.environ.get("PYSPARK_PYTHON", "python")
    spark = (
        SparkSession.builder
        .appName("ct-etl-tests")
        .master("local[2]")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    # helpful for overwrite-by-partition in any integration tests
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    yield spark
    spark.stop()
