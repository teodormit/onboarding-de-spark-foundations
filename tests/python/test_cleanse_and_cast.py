# tests/python/test_cleanse_and_cast.py
from customer_transaction_etl.steps import CleanseAndCast

def test_cleanse_and_cast_basic(spark):
    df = spark.createDataFrame(
        [("o1","c1"," 2025-09-25 ","p1"," Prod A ","2,00"," 10,99 "),
         ("o2","c2","2025-09-26","p2","Prod B","3","5.50")],
        ["order_id","customer_id","purchase_date","product_id","product_name","quantity","price"]
    )
    out = CleanseAndCast().transform(df)
    rows = out.collect()
    assert str(out.schema["price"].dataType) == "DecimalType(10,2)"
    assert rows[0]["quantity"] == 2 and float(rows[0]["price"]) == 10.99
    assert rows[0]["purchase_date"].strftime("%Y-%m-%d") == "2025-09-25"
