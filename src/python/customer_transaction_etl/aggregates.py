from pyspark.sql import DataFrame, functions as F, Window

### Aggregate functions for customer transaction analysis

def product_revenue(df: DataFrame, by: str = "overall") -> DataFrame:
    """
    Compute revenue per product.
    by="overall": across all dates.
    by="daily": per product per purchase_date.
    Returns a DataFrame with columns:
      - product_id, product_name, revenue, (optional) purchase_date
      - total_quantity, order_count
    """
    
    base_group = ["product_id", "product_name"]
    if by == "daily":
        base_group = ["purchase_date"] + base_group
        
    return (
        df.groupBy(*base_group) #unpack the list
          .agg(
              F.sum("line_amount").alias("revenue"),
              F.sum("quantity").alias("total_quantity"),
              F.countDistinct("order_id").alias("order_count"),
          )
          .orderBy(F.col("revenue").desc()) # shuffle occurs here
    )

    
def top_products_by_revenue(df: DataFrame, n: int = 10, by: str = "overall") -> DataFrame:
    """
    Return the top-N products by revenue.
    If by="overall": rank globally.
    If by="daily": rank within each purchase_date.
    Columns:
      - product_id, product_name, revenue, rank, (optional) purchase_date
    """
    if by == "overall":
        pr = product_revenue(df, by="overall")
        w = Window.orderBy(F.col("revenue").desc())
        return (pr
                .withColumn("rank", F.row_number().over(w))
                .where(F.col("rank") <= n))
    elif by == "daily":
        pr = product_revenue(df, by="daily")
        w = Window.partitionBy("purchase_date").orderBy(F.col("revenue").desc())
        return (pr
                .withColumn("rank", F.row_number().over(w))
                .where(F.col("rank") <= n))
    else:
        raise ValueError("by must be 'overall' or 'daily'")


def average_order_value(df: DataFrame) -> DataFrame:
    """
    AOV = (sum of line_amount per order) / (# of distinct orders)
    Returns a single-row DataFrame with:
      - average_order_value (decimal(18,2))
      - order_count
      - total_revenue
    """
    order_totals = df.groupBy("order_id").agg(F.sum("line_amount").alias("order_total"))
    totals = order_totals.agg(
        F.sum("order_total").alias("total_revenue"),
        F.count("*").alias("order_count"),
    )
    # AOV with explicit decimal cast for determinism
    return totals.select(
        F.col("total_revenue"),
        F.col("order_count"),
        (F.col("total_revenue") / F.col("order_count")).cast("decimal(18,2)").alias("average_order_value")
    )