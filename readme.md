# Data Engineering with Apache Spark on AWS (20-Day Learning Plan)

This repository tracks my 20-day intensive learning journey to become proficient in **Apache Spark** and its integration with **AWS Data Engineering services** (S3, Glue, Athena, Redshift), along with **Terraform for IaC**.  
Goal: Prepare for my company's Data Engineering practice assessment and start contributing to Spark-based projects.

---

# Adastra Task -<br> Customer Transactions ETL (PySpark) — Medallion

A compact, production-minded **PySpark** ETL that ingests raw customer order data, cleanses & deduplicates it (**Silver**), and publishes curated analytics (**Gold**) such as per-product revenue, Top-N products, and AOV.  
Runs inside a Dockerized Spark environment and is callable from a simple CLI.

---

## Highlights

- **Medallion flow:** Raw → Silver (cleansed fact) → Gold (analytics)
- **Transforms:** `ExplodeToLines` → `CleanseAndCast` → `DeduplicateLines` → `WithLineAmount` → `WithCustomerTotalRevenue`
- **Gold analytics:** `product_revenue` (daily & overall), `top_products_by_revenue`, `average_order_value`
- **Incremental writes:** dynamic partition overwrite (only affected `purchase_date` partitions)
- **Error quarantine:** invalid rows to `silver/_errors/...`
- **Notebooks:** step-by-step exploration used to design and validate each transform
- **Ready for tests:** pytest + chispa scaffolding 

> Prior learning in this repo also includes Spark practice notebooks (joins, windows, Parquet I/O) and dimensional modeling exercises from the Dataexpert.io curriculum.

---

### Repository Structure

- [data](./data) — set via `$DATA_ROOT` (see below)
  - [raw](./data/raw)
    - [customer_data](./data/raw/customer_data)
      - [ingest_date=YYYY-MM-DD](./data/raw/customer_data/ingest_date=YYYY-MM-DD)
        - [*.json](./data/raw/customer_data/ingest_date=YYYY-MM-DD/%2A.json)
  - [silver](./data/silver)
    - [customer_data](./data/silver/customer_data)
      - [order_lines](./data/silver/customer_data/order_lines)
        - [purchase_date=YYYY-MM-DD](./data/silver/customer_data/order_lines/purchase_date=YYYY-MM-DD)
          - [part-*.parquet](./data/silver/customer_data/order_lines/purchase_date=YYYY-MM-DD/part-%2A.parquet)
  - [gold](./data/gold)
    - [customer_data](./data/gold/customer_data)
      - [product_revenue](./data/gold/customer_data/product_revenue)
        - [purchase_date=YYYY-MM-DD](./data/customer_data/product_revenue/purchase_date=YYYY-MM-DD)
          - [...]
      - [top_products_snapshot](./data/gold/customer_data/top_products_snapshot)
        - [snapshot_date=YYYY-MM-DD](./data/gold/customer_data/top_products_snapshot/snapshot_date=YYYY-MM-DD)
          - [...]
      - [top_products_current](./data/gold/customer_data/top_products_current)
        - [...]
      - [order_kpis_snapshot](./data/gold/customer_data/order_kpis_snapshot)
        - [snapshot_date=YYYY-MM-DD](./data/gold/customer_data/order_kpis_snapshot/snapshot_date=YYYY-MM-DD)
          - [...]
      - [order_kpis_current](./data/gold/customer_data/order_kpis_current)
        - [...]
- [src](./src)
  - [python](./src/python)
    - [customer_transaction_etl](./src/python/customer_transaction_etl)
      - [__init__.py](./src/python/customer_transaction_etl/__init__.py)
      - [cli.py](./src/python/customer_transaction_etl/cli.py) — CLI (Typer)
      - [session.py](./src/python/customer_transaction_etl/session.py) — SparkSessionFactory / `build_spark()`
      - [config.py](./src/python/customer_transaction_etl/config.py) — paths & options
      - [aggregates.py](./src/python/customer_transaction_etl/aggregates.py) — gold analytics (pure functions)
      - [io](./src/python/customer_transaction_etl/io) — readers & writers
      - [transforms](./src/python/customer_transaction_etl/transforms) — ETL steps (classes)
- [notebooks](./notebooks) — hands-on exploration
- [tests](./tests) — add `pytest`/`chispa` here
- [pyproject.toml](./pyproject.toml) — editable install




## How to Run the cli.py and automate the whole ETL
#### from inside the container  
    #Prerequisite "Dev Containers" - Terminal inside VMs-   F1 -> Dev Containers: Attach to running container
    cd /home/iceberg
    pip install -e .
    export DATA_ROOT=/home/iceberg/data

    ##### run ETL
    python -m customer_transaction_etl.cli raw-to-silver 2025-09-25
    python -m customer_transaction_etl.cli silver-to-gold --top-n 10

OR

    docker exec -it spark-iceberg_de_onboarding bash -lc \
    'export DATA_ROOT=/home/iceberg/data && cd /home/iceberg && \
    python -m customer_transaction_etl.cli raw-to-silver 2025-09-25'

### Running tests
    From repo root:
    pytest -q

    Useful tricks:
    pytest -q tests/python/test_transforms.py::test_cleanse_and_cast_types_and_values
    pytest -q -k "aggregates"
    pytest -vv  # more detail

    -v verbose (names of tests)
    -s show print() output
    -k "cleanse or aov" filter by substring
    run one test:
    pytest tests/python/test_transforms.py::test_cleanse_and_cast_types_and_values -q    


### CLI Usage
        # Raw → Silver
        python -m customer_transaction_etl.cli raw-to-silver YYYY-MM-DD \
        --dedup-strategy prefer_latest     # or: drop
        # --errors-out /path/to/quarantine # optional override

        # Silver → Gold
        python -m customer_transaction_etl.cli silver-to-gold \
        --top-n 10                         # Top-N by revenue
        --no-snapshot                      # skip snapshot partitions
        --write-current                    # overwrite “current” gold tables






