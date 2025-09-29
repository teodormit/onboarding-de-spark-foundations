""" 
What this CLI will do
raw-to-silver
    -Read raw JSON for a given ingest_date
    -Run your steps: ExplodeToLines → CleanseAndCast → Deduplicate → WithLineAmount → WithCustomerTotalRevenue
    -Quarantine rows missing critical fields (to an _errors folder)
    -Incremental write: overwrite only the affected purchase_date partitions in silver (dynamic partition overwrite)

silver-to-gold
    -Read the cleansed silver table
    -Produce product revenue (daily & overall), top-N, and AOV
    -Write:
        --Daily product revenue partitioned by purchase_date (incremental overwrite)
        --Top products and AOV as snapshots (append partition snapshot_date) and also update a current table (full overwrite) for convenience 
        """

import os
from datetime import date
import typer
import time
from pyspark.sql import functions as F

from .session import SparkSessionFactory
from .config import paths, opts
from .io import read_raw_json
from .steps import (
    ExplodeToLines, CleanseAndCast, Deduplicate,
    WithLineAmount, WithCustomerTotalRevenue
)
from .aggregates import product_revenue, top_products_by_revenue, average_order_value

app = typer.Typer(help="Customer Data ETL: raw→silver→gold")  ## CLI librabry for Python

def _ts():
    return time.strftime("%Y-%m-%d %H:%M:%S")

def _echo(msg):
    typer.echo(f"[{_ts()}] {msg}")

CRITICAL_COLS = ["order_id", "product_id", "purchase_date", "quantity", "price"]

def split_clean_bad(df):
    """Split DF into clean (no nulls in critical cols) and bad with error_reason."""
    cond = " AND ".join([f"{c} IS NOT NULL" for c in CRITICAL_COLS])
    df_clean = df.where(cond)
    # build a simple error_reason string for audit
    reason = F.concat_ws(", ", *[F.when(F.col(c).isNull(), F.lit(f"{c}=NULL")) for c in CRITICAL_COLS])
    df_bad = df.where(f"NOT ({cond})").withColumn("error_reason", reason)
    return df_clean, df_bad

def enable_dynamic_partition_overwrite(spark):
    # Overwrite only partitions that appear in the incoming DataFrame
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

def distinct_purchase_dates(df):
    return [r["purchase_date"] for r in df.select("purchase_date").where("purchase_date IS NOT NULL").distinct().collect()]

# --- commands --------------------------------------------------------------

@app.command()
def raw_to_silver(
    ingest_date: str = typer.Argument(..., help="Folder like ingest_date=YYYY-MM-DD in data/raw"),
    dedup_strategy: str = typer.Option("prefer_latest", help="dedup strategy: prefer_latest or drop"),
    errors_out: str = typer.Option(None, help="Optional path for quarantined rows (defaults under silver/_errors)")
):
    """
    Transform raw JSON for a given ingest_date into cleansed, partitioned silver order_lines.
    """
    spark = SparkSessionFactory.build("raw-to-silver")
    enable_dynamic_partition_overwrite(spark)

    # Input
    raw_glob = os.path.join(paths.raw, f"ingest_date={ingest_date}", "*.json")
    typer.echo(f"Reading raw: {raw_glob}")
    df_raw = read_raw_json(spark).read(raw_glob)

    _echo(f"Reading raw from: {raw_glob}")
    raw_cnt = df_raw.count()
    _echo(f"Raw rows: {raw_cnt}")
    
    # Transform pipeline (explicit, readable)
    df = ExplodeToLines().transform(df_raw)
    df = CleanseAndCast().transform(df)
    df = Deduplicate(strategy=dedup_strategy).transform(df)
    df = WithLineAmount().transform(df)
    df = WithCustomerTotalRevenue().transform(df)

    # Split clean vs bad rows (audit)
    df_clean, df_bad = split_clean_bad(df)

    # Write quarantined rows if any
    if errors_out is None:
        errors_out = os.path.join(paths.silver, "_errors", f"ingest_date={ingest_date}")
    bad_cnt = df_bad.count()
    if bad_cnt > 0:
        typer.echo(f"Quarantining {bad_cnt} bad rows → {errors_out}")
        (df_bad.write.mode("overwrite").parquet(errors_out))
    else:
        typer.echo("No bad rows to quarantine.")

    # Incremental write: only affected purchase_date partitions
    changed_dates = distinct_purchase_dates(df_clean)
    if not changed_dates:
        typer.echo("No purchase dates found in clean data. Nothing to write.")
        raise typer.Exit(code=0)

    typer.echo(f"Writing silver partitions for dates: {', '.join([d.isoformat() for d in changed_dates])}")
    (df_clean
        .where(F.col("purchase_date").isin(changed_dates))
        .write
        .mode("overwrite")                  # dynamic partition overwrite is ON
        .partitionBy("purchase_date")
        .parquet(paths.silver))

    typer.echo(f"Done. Silver written to: {paths.silver}")
    _echo("Raw to Silver done")


@app.command()
def silver_to_gold(
    top_n: int = typer.Option(10, help="Top N products by revenue"),
    snapshot: bool = typer.Option(True, help="Write snapshot partitions for top_products & AOV"),
    write_current: bool = typer.Option(True, help="Also overwrite 'current' tables for easy consumption")
):
    """
    Build gold outputs from silver: product revenue (daily & overall), top-N, and AOV.
    - Daily product revenue is written partitioned by purchase_date (incremental overwrite).
    - Top-N & AOV can be snapshot-partitioned by snapshot_date and/or written as current (overwritten each run).
    """
    spark = SparkSessionFactory.build("silver-to-gold")
    enable_dynamic_partition_overwrite(spark)

    # Read silver
    typer.echo(f"Reading silver: {paths.silver}")
    df = spark.read.parquet(paths.silver)

    # 1) Product revenue
    pr_overall = product_revenue(df, by="overall")
    pr_daily   = product_revenue(df, by="daily")

    # 2) Top-N
    top_overall = top_products_by_revenue(df, n=top_n, by="overall")
    top_daily   = top_products_by_revenue(df, n=top_n, by="daily")

    # 3) AOV
    aov = average_order_value(df)

    gold_root = paths.gold
    snapshot_date = date.today().isoformat()

    # --- Write daily product revenue incrementally (partition by purchase_date)
    pr_daily_out = os.path.join(gold_root, "product_revenue")
    typer.echo(f"Writing daily product revenue → {pr_daily_out} (partitioned by purchase_date)")
    (pr_daily
        .write
        .mode("overwrite")
        .partitionBy("purchase_date")
        .parquet(pr_daily_out))

    # --- Write top products & AOV
    # Snapshot sinks (append new snapshot_date partition)
    if snapshot:
        top_snap_out = os.path.join(gold_root, "top_products_snapshot")
        aov_snap_out = os.path.join(gold_root, "order_kpis_snapshot")
        typer.echo(f"Appending snapshots for {snapshot_date}")
        (top_overall.withColumn("snapshot_date", F.lit(snapshot_date))
            .write.mode("append").partitionBy("snapshot_date").parquet(top_snap_out))
        (aov.withColumn("snapshot_date", F.lit(snapshot_date))
            .write.mode("append").partitionBy("snapshot_date").parquet(aov_snap_out))

    # Current sinks (overwrite a single logical table)
    if write_current:
        top_cur_out = os.path.join(gold_root, "top_products_current")
        aov_cur_out = os.path.join(gold_root, "order_kpis_current")
        typer.echo("Overwriting current gold tables")
        top_overall.write.mode("overwrite").parquet(top_cur_out)
        aov.write.mode("overwrite").parquet(aov_cur_out)

    typer.echo("Gold build complete.")

