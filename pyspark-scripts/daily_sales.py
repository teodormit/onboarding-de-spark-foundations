from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import DecimalType
import os
import sys

def die(msg: str, code: int = 1):
    print(f"[ERROR] {msg}", file=sys.stderr)
    sys.exit(code)

def main(orders_path: str, products_path: str, output_path: str):
    print(f"[INFO] Starting DailySales")
    print(f"[INFO] orders      : {orders_path}")
    print(f"[INFO] products    : {products_path}")
    print(f"[INFO] output      : {output_path}")

    # sanity checks
    if not os.path.exists(orders_path):
        die(f"orders.csv not found at {orders_path}")
    if not os.path.exists(products_path):
        die(f"products.csv not found at {products_path}")
        
        
    # Initialize Spark session
    spark = SparkSession.builder.appName("DailySales").config("spark.ui.enabled", "true").getOrCreate()
    print(f"[INFO] Spark UI: {spark.sparkContext.uiWebUrl}")

    # Load data
    orders = (spark.read.option("header", True).option("inferSchema", True).csv(orders_path)
              .withColumn("order_ts", F.to_timestamp("order_ts"))
              .withColumn("order_date", F.to_date("order_ts"))
              .withColumn("qty", F.col("qty").cast("int")))
    
    #products = spark.read.option("header", True).option("inferSchema", True).csv(input_products)
    products = spark.read.option("header", True).option("inferSchema", True).csv(products_path)
    # (optional) ensure price is numeric/decimal for precise math
    products = products.withColumn("price", F.col("price").cast(DecimalType(10, 2)))


    # Join orders with products to get product prices
    li = (
    orders.join(products, "product_id")
          .withColumn(
              "line_amount",
              (F.col("qty") * F.col("price")).cast(DecimalType(12, 2))
          )
)
    
    daily = li.groupBy("order_date")\
            .agg(F.sum("line_amount").alias("gross_sales"),\
                F.countDistinct("order_id").alias("orders"),\
                F.sum("qty").alias("units"))
    
    # write partitioned output
    daily.repartition(1).write.mode("overwrite").partitionBy("order_date").parquet(output_path)
    
    print("[INFO] Write complete.")
    spark.stop()
    print("[INFO] Spark stopped. All done.")
    
    
if __name__ == "__main__":
    import argparse
    p = argparse.ArgumentParser(description="Compute daily sales from orders + products")
    p.add_argument("--orders", default="/home/iceberg/data/testing-spark/orders.csv",
                   help="Absolute path to orders.csv inside the container")
    p.add_argument("--products", default="/home/iceberg/data/testing-spark/products.csv",
                   help="Absolute path to products.csv inside the container")
    p.add_argument("--out", default="/home/iceberg/data/testing-spark/processed_script",
                   help="Absolute output directory for parquet")
    args = p.parse_args()
    main(args.orders, args.products, args.out)
    
    
    
    
   