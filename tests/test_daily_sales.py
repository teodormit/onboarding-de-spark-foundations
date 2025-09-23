from pyspark.sql import functions as F, Row

def test_line_amount_calc(spark):
    df = spark.createDataFrame([Row(qty=2, price=3.5), Row(qty=1, price=10.0)])
    out = df.withColumn("line_amount", F.col("qty") * F.col("price"))
    vals = [r["line_amount"] for r in out.collect()]
    assert vals == [7.0, 10.0]
