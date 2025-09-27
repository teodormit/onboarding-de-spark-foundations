from pyspark.sql import DataFrame

def per_product_revenue(df_clean: DataFrame) -> DataFrame:
    ...

def top_products_by_revenue(df_clean: DataFrame, n: int) -> DataFrame:
    ...

def average_order_value(df_clean: DataFrame) -> DataFrame:
    ...