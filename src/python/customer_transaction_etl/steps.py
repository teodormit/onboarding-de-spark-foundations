# src/python/ecommerce/transforms/steps.py
from pyspark.sql import DataFrame, functions as F
from .base import TransformStep

class ExplodeToLines(TransformStep):
    """Explode nested products into one row per (order_id, product_id)."""
    def __init__(self): super().__init__("explode_to_lines")
    def transform(self, df: DataFrame) -> DataFrame:
        # Pseudocode – we’ll fill the exact columns later in the build step
        return (df
            .withColumn("item", F.explode("products"))
            .select(
                F.col("order_id"),
                F.col("customer_id"),
                F.to_date(F.col("purchase_date")).alias("purchase_date"),
                F.col("item.product_id").alias("product_id"),
                F.col("item.product_name").alias("product_name"),
                F.col("item.quantity").alias("quantity"),
                F.col("item.price").alias("price"),
            ))

class CleanseAndCast(TransformStep):
    """Trim strings, normalize numbers (comma→dot), cast to target types."""
    def __init__(self): super().__init__("cleanse_and_cast")
    def transform(self, df: DataFrame) -> DataFrame:
        # Example normalization; we’ll refine as we see real data
        normalize_num = F.udf(lambda s: s.replace(",", ".").strip() if s else s)
        df2 = (df
               .withColumn("product_name", F.trim("product_name"))
               .withColumn("quantity_str", F.trim(F.col("quantity").cast("string")))
               .withColumn("price_str", F.trim(F.col("price").cast("string")))
               .withColumn("price_str", normalize_num("price_str"))
               .withColumn("quantity", F.col("quantity_str").cast("int"))
               .withColumn("price", F.col("price_str").cast("decimal(10,2)"))
               .drop("quantity_str", "price_str"))
        return df2

class Deduplicate(TransformStep):
    """Drop duplicate order lines or merge them (policy: keep first)."""
    def __init__(self): super().__init__("deduplicate")
    def transform(self, df: DataFrame) -> DataFrame:
        return df.dropDuplicates(["order_id", "product_id"])

class WithLineAmount(TransformStep):
    """Compute line_amount = quantity * price."""
    def __init__(self): super().__init__("with_line_amount")
    def transform(self, df: DataFrame) -> DataFrame:
        return df.withColumn("line_amount", F.col("quantity") * F.col("price"))
