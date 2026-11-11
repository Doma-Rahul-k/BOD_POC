# AGENTS.md

Databricks medallion-pipeline POC (AIS vessel tracking, "BigOceanData"). The repo contains only 4 Databricks notebooks exported as `.py`. It has no manifests, tests, lint config, CI, or local runner.

## Running / verifying
- Notebooks run only on Databricks (serverless, `environment_version = "6"`). They rely on the ambient `spark` session and Unity Catalog, so they can't be run locally. Don't try to add local test or build tooling unless asked.
- Run order is `01_bronze_ingestion` → `02_silver_transformation` → `03_gold_aggregation`. `04_poc_summary` is read-only reporting. Each layer reads the previous layer's table.
- The source is a Volume file, `/Volumes/bigoceandata_poc/bronze/raw_files/ais_raw_sample.csv`. The notebook `00_generate_sample_data` that creates it is referenced in `04` but is **not in this repo**.
- You can only verify edits by reading the code carefully (`python -m py_compile <file>` checks syntax).

## Notebook format (preserve it)
- Files use Databricks source format: the first line is `# Databricks notebook source`, cells are separated by `# COMMAND ----------`, and markdown cells use `# MAGIC %md`. The `# /// script` header block sets the environment version. Don't remove or reformat these markers.
- Cells are labelled `# Cell N — Title`. If you add or remove cells, update the cell counts in the checklist in `04_poc_summary.py` (Cell 12).
- Each notebook defines its own config in Cell 1. There is no shared module, so a name or constant change must be made in all notebooks that use it. Table names are `bigoceandata_poc.{bronze|silver|gold}.<table>`. Some notebooks hard-code them (for example the `DESCRIBE HISTORY` in 01 and the Cell 8–9 text in 04).
- Every layer writes with `.mode("overwrite")` + `overwriteSchema=true` via `saveAsTable`. The pipeline is full-refresh and not incremental.

## Gotchas
- Bronze: do NOT use `timestampFormat` with `inferSchema`. Silver expects `timestamp` as a string and parses it with `try_to_timestamp(..., "yyyy-MM-dd'T'HH:mm:ss'Z'")`. Silver skips the cast if the column is already a `TimestampType`.
- Silver falls back to NULL/`'unknown'` for the optional columns `true_heading`, `length`, `source` and `anomaly_injected` when they are missing from Bronze.
- `anomaly_injected` is the synthetic ground-truth label from the generated CSV. It is used only for the ground-truth vs `anomaly_flag` comparisons, not for detection.
- `anomaly_flag` is a single value chosen by priority: dark_ship > speed_anomaly > route_deviation > loitering. The `is_*` booleans are independent, and the Gold event counts sum the booleans, not `anomaly_flag`.
- Anomaly thresholds are in `02` Cell 1: dark gap 2h, loitering <2 kn, speed >25 kn, course delta >45°. `course_delta` is a plain `abs()` difference with no 360° wraparound.
- Risk tiers are HIGH>=25 / MEDIUM>=10 / LOW (<10). The code in `03` Cell 5, the `03` comment and Cell 12 print, and the `04` legend all use these values. If you change the thresholds, update all of them.
- `04_poc_summary.py` is the only "dashboard": it renders with `display()` tables and markdown cells. No SQL dashboard or Lakeview spec exists in the repo. It has no plotting libraries.
- `03` also has a hard-coded `HIGH_RISK_FLAGS` list that gives a +5 bonus, and the final score is capped at 100.
- Gold `port_activity` groups by `destination` too, so one vessel can produce several rows.
