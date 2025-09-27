# src/python/ecommerce/cli.py
import os, typer
from .spark import SparkSessionFactory
from .config import paths, opts
from .io.readers import RawJsonReader
from .io.writers import PartitionedParquetWriter
from .pipeline import Pipeline
from .transforms.steps import ExplodeToLines, CleanseAndCast, Deduplicate, WithLineAmount
from . import aggregates

app = typer.Typer()

@app.command()
def raw_to_silver(ingest_date: str, debug: bool = True):
    spark = SparkSessionFactory.build("raw-to-silver")
    raw_glob = os.path.join(paths.raw, f"ingest_date={ingest_date}", "*.json")

    df_raw = RawJsonReader(spark).read(raw_glob)

    pipeline = Pipeline([
        ExplodeToLines(),
        CleanseAndCast(),
        Deduplicate(),
        WithLineAmount(),
    ])
    df_silver = pipeline.run(df_raw, debug=debug)

    PartitionedParquetWriter(opts.partitions_col).write(df_silver, paths.silver)

@app.command()
def silver_to_gold():
    spark = SparkSessionFactory.build("silver-to-gold")
    df = spark.read.parquet(paths.silver)

    (aggregates.per_product_revenue(df)
        .write.mode("overwrite").parquet(os.path.join(paths.gold, "product_revenue")))
    (aggregates.top_products_by_revenue(df, opts.top_n_products)
        .write.mode("overwrite").parquet(os.path.join(paths.gold, "top_products")))
    (aggregates.average_order_value(df)
        .write.mode("overwrite").parquet(os.path.join(paths.gold, "order_kpis")))

if __name__ == "__main__":
    app()
