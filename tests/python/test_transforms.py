# tests/python/test_transforms.py
from decimal import Decimal
from pyspark.sql import functions as F, types as T

from customer_transaction_etl.steps import (
    ExplodeToLines, CleanseAndCast, Deduplicate,
    WithLineAmount, WithCustomerTotalRevenue
)

def _raw_df(spark):
    # Two orders; o1 has two products; mixed spacing/commas
    data = [
        {
            "order_id": "o1", "customer_id": "c1", "purchase_date": " 2025-09-25 ",
            "products": [
                {"product_id": "p1", "name": " A ", "quantity": "2,00", "price": " 10,99 "},
                {"product_id": "p2", "name": "B",   "quantity": "1",    "price": "5.50"}
            ]
        },
        {
            "order_id": "o2", "customer_id": "c1", "purchase_date": "2025-09-26",
            "products": [
                {"product_id": "p1", "name": "A", "quantity": "1", "price": "10.99"}
            ]
        },
    ]
    schema = T.StructType([
        T.StructField("order_id", T.StringType()),
        T.StructField("customer_id", T.StringType()),
        T.StructField("purchase_date", T.StringType()),
        T.StructField("products", T.ArrayType(
            T.StructType([
                T.StructField("product_id", T.StringType()),
                T.StructField("name", T.StringType()),
                T.StructField("quantity", T.StringType()),
                T.StructField("price", T.StringType()),
            ])
        ))
    ])
    return spark.createDataFrame(data, schema)

def test_explode_to_lines_rowcount(spark):
    df_raw = _raw_df(spark)
    step = ExplodeToLines()
    out = step.transform(df_raw)
    # expected line count = sum of array sizes
    exp = df_raw.select(F.size("products").alias("n")).agg(F.sum("n")).first()[0]
    assert out.count() == exp

def test_cleanse_and_cast_types_and_values(spark):
    df_raw = _raw_df(spark)
    df = ExplodeToLines().transform(df_raw)
    df = CleanseAndCast().transform(df)

    dtypes = {f.name: f.dataType.simpleString() for f in df.schema.fields}
    assert dtypes["purchase_date"] == "date"
    assert dtypes["quantity"] == "int"
    assert dtypes["price"].startswith("decimal(")

    # check a couple of parsed values
    row = (df.where((F.col("order_id") == "o1") & (F.col("product_id") == "p1"))
             .select("product_name","quantity","price","purchase_date").first())
    assert row["product_name"] == "A"
    assert row["quantity"] == 2
    assert float(row["price"]) == 10.99
    assert row["purchase_date"].strftime("%Y-%m-%d") == "2025-09-25"

def test_deduplicate_prefer_latest(spark):
    # Build a tiny cleansed DF with a duplicate order line for (o1,p1)
    rows = [
        ("o1","c1","2025-09-24","p1","A",1,Decimal("10.99")),
        ("o1","c1","2025-09-25","p1","A",1,Decimal("10.99")),  # should be kept (latest date)
        ("o1","c1","2025-09-25","p2","B",1,Decimal("5.50")),
    ]
    df = spark.createDataFrame(rows, ["order_id","customer_id","purchase_date","product_id","product_name","quantity","price"]) \
              .withColumn("purchase_date", F.to_date("purchase_date"))
    out = Deduplicate(strategy="prefer_latest").transform(df)
    # Only two unique product lines remain
    assert out.count() == 2
    # Ensure the kept row for (o1,p1) has 2025-09-25
    kept = (out.where((F.col("order_id")=="o1") & (F.col("product_id")=="p1"))
              .select(F.col("purchase_date").cast("string")).first()[0])
    assert kept == "2025-09-25"

def test_with_line_amount_and_customer_total(spark):
    rows = [
        ("c1","o1","p1",2,Decimal("5.50")),
        ("c1","o2","p2",1,Decimal("7.00")),
    ]
    df = spark.createDataFrame(rows, ["customer_id","order_id","product_id","quantity","price"])
    df = WithLineAmount().transform(df)
    df = WithCustomerTotalRevenue().transform(df)

    vals = { (r["order_id"], r["product_id"]): float(r["line_amount"]) for r in df.select("order_id","product_id","line_amount").collect() }
    assert vals == {("o1","p1"): 11.0, ("o2","p2"): 7.0}

    per_cust = { r["customer_id"]: float(r["customer_total_revenue"]) for r in df.select("customer_id","customer_total_revenue").dropDuplicates().collect() }
    assert per_cust == {"c1": 18.0}
