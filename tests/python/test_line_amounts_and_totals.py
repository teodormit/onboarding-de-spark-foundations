def test_with_line_amount(spark):
    from customer_transaction_etl.steps import WithLineAmount
    df = spark.createDataFrame([("o1","p1", 2, Decimal("5.50"))],
                               ["order_id","product_id","quantity","price"])
    out = WithLineAmount().transform(df)
    assert float(out.selectExpr("line_amount").first()[0]) == 11.00

def test_with_customer_total_revenue(spark):
    from customer_transaction_etl.steps import WithCustomerTotalRevenue
    df = spark.createDataFrame(
        [("c1","o1","p1", Decimal("10.00")),
         ("c1","o2","p2", Decimal("5.00")),
         ("c2","o3","p3", Decimal("7.50"))],
        ["customer_id","order_id","product_id","line_amount"]
    )
    out = WithCustomerTotalRevenue().transform(df)
    vals = {r["customer_id"]: float(r["customer_total_revenue"]) for r in out.select("customer_id","customer_total_revenue").dropDuplicates().collect()}
    assert vals == {"c1": 15.00, "c2": 7.50}
