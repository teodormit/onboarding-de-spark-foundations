
from customer_transaction_etl.aggregates import product_revenue, top_products_by_revenue, average_order_value
from decimal import Decimal
from pyspark.sql import functions as F

def _lines_df(spark):
    # Minimal "silver" like DF with line_amount ready
    rows = [
        ("o1","c1","2025-09-25","p1","A", 1, Decimal("5.00"),  Decimal("5.00")),
        ("o2","c1","2025-09-26","p1","A", 2, Decimal("5.00"),  Decimal("10.00")),
        ("o3","c2","2025-09-26","p2","B", 1, Decimal("7.50"),  Decimal("7.50")),
    ]
    cols = ["order_id","customer_id","purchase_date","product_id","product_name","quantity","price","line_amount"]
    df = spark.createDataFrame(rows, cols)
    return df.withColumn("purchase_date", F.to_date("purchase_date"))

def test_product_revenue_overall(spark):
    df = _lines_df(spark)
    out = product_revenue(df, by="overall").collect()
    got = { (r["product_id"], r["product_name"]): float(r["revenue"]) for r in out }
    assert got == {("p1","A"): 15.0, ("p2","B"): 7.5}

def test_top_products_by_revenue_daily(spark):
    df = _lines_df(spark)
    out = top_products_by_revenue(df, n=1, by="daily").collect()
    # Expect one row per date, always the top by revenue
    by_date = { r["purchase_date"].strftime("%Y-%m-%d"): (r["product_id"], float(r["revenue"])) for r in out }
    assert by_date == {"2025-09-25": ("p1", 5.0), "2025-09-26": ("p1", 10.0)}

def test_average_order_value(spark):
    df = _lines_df(spark)
    aov = average_order_value(df).first()
    assert float(aov["total_revenue"]) == 22.5
    assert aov["order_count"] == 3
    assert float(aov["average_order_value"]) == 7.5