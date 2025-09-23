# 🧠 SQL Cheat Sheet — Data Engineering Edition

**Context:** PostgreSQL locally, Spark SQL in pipelines.  
**Use:** Copy a section, fill placeholders, run. Add your own variants as you learn.

---

## 📋 Index

1. Profiling & Granularity  
2. Safe Aggregations & Joins  
3. Deduping & Keys  
4. Window Functions (ranking, gaps/islands)  
5. Time/Date Patterns  
6. Data Quality & Freshness Checks  
7. Idempotent Loads (MERGE/UPSERT)  
8. SCD Type-2 Template  
9. Performance & Explain  
10. DDL Snippets (tables, indexes, partitions)  
11. Handy Utilities (pivot/unpivot, string/JSON)

---

## 1) Profiling & Granularity

### 1.1 Table shape & basic stats
```sql
-- [PG]
SELECT relname AS table_name, n_live_tup AS est_rows,
       pg_size_pretty(pg_total_relation_size(c.oid)) AS total_size
FROM pg_stat_all_tables t
JOIN pg_class c ON c.relname = t.relname
WHERE schemaname = 'public' AND relname = '<TABLE>';

-- [Spark]
SELECT COUNT(*) FROM <db>.<table>;
DESCRIBE EXTENDED <db>.<table>;
```

### 1.2 Identify granularity (candidate primary grain)
```sql
WITH cte AS (
  SELECT <key_col1>, <key_col2>, COUNT(*) AS cnt
  FROM <schema>.<table>
  GROUP BY 1,2
)
SELECT SUM(CASE WHEN cnt=1 THEN 1 ELSE 0 END) AS unique_groups,
       SUM(CASE WHEN cnt>1 THEN 1 ELSE 0 END) AS dup_groups
FROM cte;
```
OR
```sql
SELECT 
    dim #1 --game_id,
    dim #2 --player_id,
    dim #3,
    COUNT(1) 
FROM table --game_details
GROUP BY 1, 2, 3
HAVING COUNT(1) > 1;
```

### 1.3 Column profiling (null %, min/max, sample)
```sql
SELECT COUNT(*) AS row_cnt,
       SUM(CASE WHEN <col> IS NULL THEN 1 ELSE 0 END)::float/COUNT(*) AS null_ratio,
       MIN(<col>) AS min_val, MAX(<col>) AS max_val
FROM <schema>.<table>;
```

> ✅ **BP:** Decide & document the official grain: _“1 row per `<order_id>` per `<line_number>`”_.

---

## 2) Safe Aggregations & Joins

### 2.1 Null-safe aggregates
```sql
SELECT COALESCE(SUM(<amount>),0) AS total_amount,
       COUNT(*) AS row_cnt,
       COUNT(<nullable_col>) AS non_null_cnt
FROM <schema>.<table>;
```

### 2.2 Join patterns
```sql
WITH f AS (
  SELECT <cols_you_need>
  FROM fact_sales
  WHERE sales_dt BETWEEN DATE '<from>' AND DATE '<to>'
), d AS (
  SELECT <dim_key>, <attrs>
  FROM dim_customer
)
SELECT f.<dim_key>, d.<attrs>, f.<measures>
FROM f
LEFT JOIN d ON f.<dim_key> = d.<dim_key>;
```

> ✅ **BP:** Pre-project columns, use broadcast for small dims in Spark, and test row counts pre/post join.

---

## 3) Deduplication & Keys

### 3.1 Keep latest per business key
```sql
WITH ranked AS (
  SELECT *,
         ROW_NUMBER() OVER (PARTITION BY <biz_key> ORDER BY <ts_col> DESC) AS rn
  FROM <schema>.<table>
)
SELECT * FROM ranked WHERE rn = 1;
```

### 3.2 Surrogate key (stable hash)
```sql
-- [PG]
SELECT MD5(CONCAT_WS('|', col1, col2)) AS sk;
-- [Spark]
SELECT SHA2(CONCAT_WS('|', col1, col2), 256) AS sk;
```

---

## 4) Window Functions

### 4.1 Rankings & top-N per group
```sql
SELECT *
FROM (
  SELECT <group_key>, <metric>,
         RANK() OVER (PARTITION BY <group_key> ORDER BY <metric> DESC) AS rnk
  FROM <schema>.<table>
) t
WHERE rnk <= 3;
```

### 4.2 Running totals
```sql
SELECT <group_key>, <dt>,
       SUM(<amount>) OVER (PARTITION BY <group_key> ORDER BY <dt>
                           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS run_total
FROM <schema>.<table>;
```

---

## 5) Time/Date Patterns

### 5.1 Truncation & bucketing
```sql
-- [PG]
DATE_TRUNC('day', <ts>) AS day_bucket
-- [Spark]
DATE_TRUNC('DAY', <ts>) AS day_bucket
```

### 5.2 Freshness SLA
```sql
SELECT CURRENT_DATE - MAX(<event_dt>) AS days_since_last_event
FROM <schema>.<table>;
```

---

## 6) Data Quality & Freshness

### 6.1 Null/empty checks
```sql
SELECT SUM(CASE WHEN <col> IS NULL OR TRIM(<col>)='' THEN 1 ELSE 0 END) AS bad_rows,
       COUNT(*) AS total_rows
FROM <schem