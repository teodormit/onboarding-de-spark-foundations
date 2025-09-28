# src/python/ecommerce/transforms/steps.py
from pyspark.sql import DataFrame, functions as F, Window
from .base import TransformStep
#from decimal import Decimal

class ExplodeToLines(TransformStep):
    """Explode nested products into one row per (order_id, product_id).
    INPUT SHAPE (raw)
    -----------------
    - One row per order:
      - order_id: string
      - customer_id: string
      - purchase_date: string (to be cast to date)
      - products: array<struct<
          product_id: string,
          name: string,         # raw name field in JSON
          quantity: string,     # raw string (may be '2,00' or ' 1')
          price: string         # raw string (may be '10,99' or ' 5.99 ')
          
    OUTPUT SHAPE (lines; still raw types)
    -------------------------------------
    - One row per product line:
      - order_id, customer_id, purchase_date (unchanged, still strings)
      - product_id, product_name (renamed from 'name')
      - quantity, price (still strings; to be cleansed & cast later)
        >>
    
    """
    def __init__(self): super().__init__("explode_to_lines")
    def transform(self, df: DataFrame) -> DataFrame:
        # Pseudocode – we’ll fill the exact columns later in the build step
        exploded = df.withColumn("item", F.explode("products"))
        
        lines = (
            exploded
            .select(
                F.col("order_id"),
                F.col("customer_id"),
                F.col("purchase_date"),
                F.col("item.product_id").alias("product_id"),
                F.col("item.name").alias("product_name"),
                F.col("item.quantity").alias("quantity"),
                F.col("item.price").alias("price"),
            )
        )        
        return lines

class CleanseAndCast(TransformStep):
    """
    Trim strings, normalize numeric text, and cast to target types:
      - purchase_date: date
      - quantity: int
      - price: decimal(10,2)
    Also converts empty strings to nulls, but does NOT drop invalid rows.
    """
    name="cleanse_and_cast"
        
    def __init__(self): super().__init__("cleanse_and_cast")
    
    @staticmethod
    def _null_if_empty(col):
        # empty or whitespace-only strings → null
        return F.when(F.length(F.trim(col)) == 0, F.lit(None)).otherwise(col)
    
    @staticmethod
    def _normalize_num_str(col):
        # 1) trim → 2) remove inner spaces → 3) convert ',' → '.' for decimals
        return F.regexp_replace(
            F.regexp_replace(F.trim(col), r"\s+", ""), ",", "."
        )
    
    def transform(self, df: DataFrame) -> DataFrame:
        # --- standardize text columns ---
        out = (
            df
            .withColumn("order_id", F.trim(F.col("order_id")))
            .withColumn("customer_id", F.trim(F.col("customer_id")))
            .withColumn("product_id", F.trim(F.col("product_id")))
            .withColumn("product_name", F.trim(F.col("product_name")))
            .withColumn("purchase_date", F.trim(F.col("purchase_date")))
        )

        out = (
            out
            # make empty strings null for numeric fields
            .withColumn("quantity_raw", self._null_if_empty(F.col("quantity")))
            .withColumn("price_raw", self._null_if_empty(F.col("price")))
            # normalize string representation
            .withColumn("quantity_norm", self._normalize_num_str(F.col("quantity_raw")))
            .withColumn("price_norm", self._normalize_num_str(F.col("price_raw")))
            # cast:
            # quantity may appear as "2", "2.0", "2,00" → cast to double then round to nearest int
            .withColumn("quantity_dbl", F.col("quantity_norm").cast("double"))
            .withColumn("quantity", F.when(F.col("quantity_dbl").isNull(), F.lit(None))
                                      .otherwise(F.round(F.col("quantity_dbl")).cast("int")))
            # price to decimal(10,2)
            .withColumn("price", F.col("price_norm").cast("decimal(10,2)"))
            # drop temp cols
            .drop("quantity_raw", "price_raw", "quantity_norm", "price_norm", "quantity_dbl")
        )
        
        # --- cast date (with a simple fallback) ---
        date1 = F.to_date(F.col("purchase_date"), "yyyy-MM-dd")
        date2 = F.to_date(F.col("purchase_date"), "yyyy/MM/dd")  # optional fallback
        out = out.withColumn("purchase_date", F.coalesce(date1, date2))


        return out

class Deduplicate(TransformStep):
    """
    Remove duplicate order lines by a key (default: order_id + product_id).

    Strategies:
      - "drop":          fast, keep an arbitrary row (Spark's dropDuplicates)
      - "prefer_latest": deterministic; keep the row with the latest purchase_date
    """
    
    name = "deduplicate_lines"

    def __init__(self, key_cols=("order_id", "product_id"), strategy: str = "prefer_latest"):
        self.key_cols = list(key_cols)
        self.strategy = strategy
    
    def transform(self, df: DataFrame) -> DataFrame:
        if self.strategy == "drop":
            return df.dropDuplicates(self.key_cols)
        
               # otherwise "prefer_latest" is used. It partitions by key_cols and orders by purchase_date desc to keep the only latest.
        w = Window.partitionBy(*[F.col(c) for c in self.key_cols]) \
                  .orderBy(F.col("purchase_date").desc_nulls_last())
        return (df
                .withColumn("_rn", F.row_number().over(w))
                .where(F.col("_rn") == 1)
                .drop("_rn"))


class WithLineAmount(TransformStep):
    """
    Compute line_amount = quantity * price.
    - quantity: int
    - price: decimal(10,2)
    - line_amount: decimal(18,2)   (roomy to avoid overflow)
    """
    name = "with_line_amount"
    
    def transform(self, df: DataFrame) -> DataFrame:
        # Cast quantity to a decimal to control the resulting precision/scale deterministically.
        qty_dec = F.col("quantity").cast("decimal(10,0)")
        line_amt = (qty_dec * F.col("price")).cast("decimal(18,2)")
        return df.withColumn("line_amount", line_amt)

class WithCustomerTotalRevenue:
    """
    Add a column 'customer_total_revenue' = SUM(line_amount) per customer.
    Uses a window so the value is available on every row for that customer.
    """
    name = "with_customer_total_revenue"

    def __init__(self, col_name: str = "customer_total_revenue"):
        self.col_name = col_name

    def transform(self, df: DataFrame) -> DataFrame:
        w = Window.partitionBy("customer_id")
        return df.withColumn(self.col_name, F.sum("line_amount").over(w))