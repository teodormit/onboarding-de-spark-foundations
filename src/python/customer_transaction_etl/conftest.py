import pytest
from pyspark.sql import SparkSession

@pytest.fixture(scope="session")
def spark():
    spark = (SparkSession.builder
             .appName("tests")
             .master("local[2]")
             .config("spark.ui.enabled", "false")
             .getOrCreate())
    yield spark
    spark.stop()
