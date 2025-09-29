from pyspark.sql import SparkSession

class SparkSessionFactory:
    """Builds a configured SparkSession for jobs and notebooks."""
    @staticmethod
    def build(app_name: str = "customer-transaction-etl") -> SparkSession:
        return (
            SparkSession.builder
            .appName(app_name)
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
            .getOrCreate()
        )


def build_spark(app_name: str = "customer-transaction-etl") -> SparkSession:
    return SparkSessionFactory.build(app_name)