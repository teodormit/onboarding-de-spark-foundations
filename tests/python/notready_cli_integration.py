
import os, json, importlib
from typer.testing import CliRunner
from pyspark.sql import functions as F

def _write_raw(tmp_root, raw_base_name, ingest_date, objects):
    raw_dir = os.path.join(tmp_root, "raw", raw_base_name, f"ingest_date={ingest_date}")
    os.makedirs(raw_dir, exist_ok=True)
    fp = os.path.join(raw_dir, "customer_data.json")
    with open(fp, "w", encoding="utf-8") as f:
        for obj in objects:
            f.write(json.dumps(obj) + "\n")
    return fp

def test_raw_to_silver_and_gold_end_to_end(monkeypatch, spark, tmp_path):
    # IMPORTANT: set DATA_ROOT and reload config so 'paths' points at tmp_root
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    import customer_transaction_etl.config as cfg
    importlib.reload(cfg)

    # reload CLI AFTER config so it picks up new paths
    import customer_transaction_etl.cli as cli
    importlib.reload(cli)

    # monkeypatch session builder to reuse our test Spark session
    if hasattr(cli, "build_spark"):
        monkeypatch.setattr(cli, "build_spark", lambda name="x": spark)
    elif hasattr(cli, "SparkSessionFactory"):
        monkeypatch.setattr(cli.SparkSessionFactory, "build", lambda name="x": spark)

    # write tiny raw input to the expected raw domain
    raw_domain = os.path.basename(cfg.paths.raw)   # e.g., 'customer_data_ingest' or 'ecommerce_sales'
    ingest_date = "2025-09-25"
    objs = [{
        "order_id":"o1","customer_id":"c1","purchase_date":"2025-09-25",
        "products":[{"product_id":"p1","name":"A","quantity":"2","price":"10.00"}]
    }]
    _write_raw(str(tmp_path), raw_domain, ingest_date, objs)

    runner = CliRunner()

    # run raw->silver
    res1 = runner.invoke(cli.app, ["raw-to-silver", ingest_date])
    assert res1.exit_code == 0, res1.output

    # run silver->gold
    res2 = runner.invoke(cli.app, ["silver-to-gold", "--top-n", "1", "--no-snapshot", "--write-current"])
    assert res2.exit_code == 0, res2.output

    # verify silver exists
    silver_glob = os.path.join(str(tmp_path), "silver")
    df_silver = spark.read.parquet(silver_glob + "/**/order_lines")
    assert df_silver.count() == 1
    assert df_silver.select("purchase_date").distinct().count() == 1

    # verify gold current exists
    gold_root = os.path.join(str(tmp_path), "gold")
    top_cur = spark.read.parquet(gold_root + "/**/top_products_current")
    assert top_cur.count() == 1
