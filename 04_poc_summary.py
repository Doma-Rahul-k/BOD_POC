# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # BigOceanData POC — Executive Summary & Results
# MAGIC **Full pipeline audit across all Medallion layers.** This notebook is read-only: it only reports on the tables built by notebooks 01–03.
# MAGIC
# MAGIC ### How to read this
# MAGIC | Term | Meaning |
# MAGIC |---|---|
# MAGIC | **Risk score** | 0–100 per vessel. Sum of weighted anomaly event counts (each capped) plus a +5 bonus for high-risk flag states, capped at 100. |
# MAGIC | **Risk tier** | `HIGH` = score ≥ 25 · `MEDIUM` = score ≥ 10 · `LOW` = score < 10 |
# MAGIC | **Anomaly types** | `dark_ship`, `speed_anomaly`, `route_deviation`, `loitering` (rules in Section 7) |
# MAGIC | **Severity** | `HIGH` = dark_ship, speed_anomaly · `MEDIUM` = route_deviation, loitering |
# MAGIC | **Anomaly rate** | Share of a vessel's position reports flagged as anomalous (%) |
# MAGIC | **Dwell time** | Hours between a vessel's first and last stationary report (speed < 0.5 kn, or status At anchor / Moored) at a destination |
# MAGIC
# MAGIC **Sections:** 1 Data completeness · 2 Fleet KPIs · 3 Anomaly detection · 4 Risk leaderboard · 5 Ground-truth check · 6 Port activity · 7 Reference (architecture, rules, stack, scaling) · 8 Checklist

# COMMAND ----------

# Cell 1 — Configuration

from pyspark.sql import functions as F

CATALOG       = "bigoceandata_poc"
BRONZE_TABLE  = f"{CATALOG}.bronze.vessel_positions_raw"
SILVER_TABLE  = f"{CATALOG}.silver.vessel_positions_clean"
GOLD_FLEET    = f"{CATALOG}.gold.fleet_summary"
GOLD_RISK     = f"{CATALOG}.gold.vessel_risk_scores"
GOLD_PORT     = f"{CATALOG}.gold.port_activity"
GOLD_TIMELINE = f"{CATALOG}.gold.anomaly_timeline"

# --- Display helpers (formatting only; no analytics logic) ---
NA = "–"

def fmt_int(value):
    """Integer with thousands separators; '–' when the value is missing."""
    return NA if value is None else f"{int(value):,}"

def fmt_pct(value, digits=2):
    """Percentage with fixed decimals; '–' when the value is missing."""
    return NA if value is None else f"{float(value):,.{digits}f}%"

# Text cue so risk tiers are scannable in a table.
TIER_LABEL_COL = (
    F.when(F.col("risk_tier") == "HIGH",   "▲ HIGH")
     .when(F.col("risk_tier") == "MEDIUM", "■ MEDIUM")
     .when(F.col("risk_tier") == "LOW",    "▼ LOW")
     .otherwise(F.col("risk_tier"))
)

print("=== Table Registry ===")
for t in [BRONZE_TABLE, SILVER_TABLE, GOLD_FLEET, GOLD_RISK, GOLD_PORT, GOLD_TIMELINE]:
    print(f"  {t}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Data Completeness
# MAGIC Row count per table across all layers. `OK` = table has rows, `EMPTY` = table exists but has no rows, `ERROR` = table could not be read.

# COMMAND ----------

# Cell 2 — Row Count Audit: All Layers

spark.sql(f"USE CATALOG {CATALOG}")

layers = [
    ("Bronze", "vessel_positions_raw",   BRONZE_TABLE),
    ("Silver", "vessel_positions_clean", SILVER_TABLE),
    ("Gold",   "fleet_summary",          GOLD_FLEET),
    ("Gold",   "vessel_risk_scores",     GOLD_RISK),
    ("Gold",   "port_activity",          GOLD_PORT),
    ("Gold",   "anomaly_timeline",       GOLD_TIMELINE),
]

audit_rows = []
for layer, name, table in layers:
    try:
        count = spark.table(table).count()
        audit_rows.append((layer, name, count, "OK" if count > 0 else "EMPTY"))
    except Exception as e:
        audit_rows.append((layer, name, None, f"ERROR: {str(e)[:50]}"))

display(spark.createDataFrame(audit_rows, "Layer string, Table string, Rows long, Status string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Fleet KPIs
# MAGIC Headline numbers from `gold.vessel_risk_scores`. Tier counts use the thresholds in the legend above.

# COMMAND ----------

# Cell 3 — Fleet KPI Summary

df_risk = spark.table(GOLD_RISK)
total_vessels     = df_risk.count()
total_reports     = df_risk.agg(F.sum("total_position_reports")).collect()[0][0]
total_anomalies   = df_risk.agg(F.sum("total_anomaly_events")).collect()[0][0]
avg_anomaly_rate  = df_risk.agg(F.round(F.avg("anomaly_rate_pct"), 2)).collect()[0][0]
high_risk_count   = df_risk.filter(F.col("risk_tier") == "HIGH").count()
medium_risk_count = df_risk.filter(F.col("risk_tier") == "MEDIUM").count()
low_risk_count    = df_risk.filter(F.col("risk_tier") == "LOW").count()

def share_of_fleet(n):
    return fmt_pct(100.0 * n / total_vessels, 1) if total_vessels else NA

kpi_rows = [
    ("Fleet",      "Vessels tracked",                          fmt_int(total_vessels)),
    ("Volume",     "AIS position reports",                     fmt_int(total_reports)),
    ("Volume",     "Anomaly events detected",                  fmt_int(total_anomalies)),
    ("Volume",     "Average anomaly rate (per vessel)",        fmt_pct(avg_anomaly_rate)),
    ("Risk tiers", "▲ HIGH-risk vessels (score ≥ 25)",         f"{fmt_int(high_risk_count)}  ({share_of_fleet(high_risk_count)} of fleet)"),
    ("Risk tiers", "■ MEDIUM-risk vessels (score 10–24)",      f"{fmt_int(medium_risk_count)}  ({share_of_fleet(medium_risk_count)} of fleet)"),
    ("Risk tiers", "▼ LOW-risk vessels (score < 10)",          f"{fmt_int(low_risk_count)}  ({share_of_fleet(low_risk_count)} of fleet)"),
]

display(spark.createDataFrame(kpi_rows, "Group string, Metric string, Value string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Anomaly Detection Results
# MAGIC **3a.** How every Silver position report was classified (`none` = normal). One flag per report, chosen by priority: dark_ship > speed_anomaly > route_deviation > loitering.
# MAGIC
# MAGIC **3b.** Anomaly events in the Gold timeline, grouped by severity (HIGH first).

# COMMAND ----------

# Cell 4 — Anomaly Detection Results

print("3a. Position reports by anomaly type (Silver)")
df_silver_flags = spark.table(SILVER_TABLE).groupBy("anomaly_flag").count()
silver_total = df_silver_flags.agg(F.sum("count")).collect()[0][0] or 0
display(
    df_silver_flags
    .select(
        F.col("anomaly_flag").alias("Anomaly type"),
        F.col("count").alias("Reports"),
        (F.round(F.col("count") * 100.0 / F.lit(silver_total), 2) if silver_total
         else F.lit(None).cast("double")).alias("% of all reports"),
    )
    .orderBy(F.col("Reports").desc())
)

print("3b. Anomaly events by severity (Gold — anomaly timeline)")
severity_rank = (
    F.when(F.col("anomaly_severity") == "HIGH",   1)
     .when(F.col("anomaly_severity") == "MEDIUM", 2)
     .otherwise(3)
)
display(
    spark.table(GOLD_TIMELINE)
    .groupBy("anomaly_severity", "anomaly_flag")
    .count()
    .withColumn("_sev_rank", severity_rank)
    .orderBy("_sev_rank", F.col("count").desc())
    .select(
        F.col("anomaly_severity").alias("Severity"),
        F.col("anomaly_flag").alias("Anomaly type"),
        F.col("count").alias("Events"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Risk Score Leaderboard
# MAGIC Top 10 vessels by risk score (highest first). Columns are grouped: vessel identity → risk → event counts by type → totals.

# COMMAND ----------

# Cell 5 — Risk Score Leaderboard

from pyspark.sql import Window

_rank_order = [F.col("risk_score").desc(), F.col("mmsi")]  # mmsi breaks score ties so the top 10 is stable
_rank_window = Window.orderBy(*_rank_order)

display(
    spark.table(GOLD_RISK)
    .orderBy(*_rank_order)
    .limit(10)
    .withColumn("Rank", F.row_number().over(_rank_window))
    .select(
        "Rank",
        F.col("mmsi").alias("MMSI"),
        F.col("vessel_name").alias("Vessel"),
        F.col("vessel_type").alias("Type"),
        F.col("flag").alias("Flag"),
        F.col("risk_score").alias("Risk score (0–100)"),
        TIER_LABEL_COL.alias("Risk tier"),
        F.col("dark_ship_events").alias("Dark ship events"),
        F.col("speed_anomaly_events").alias("Speed events"),
        F.col("route_deviation_events").alias("Route deviation events"),
        F.col("loitering_events").alias("Loitering events"),
        F.col("total_anomaly_events").alias("Total anomaly events"),
        F.round("anomaly_rate_pct", 2).alias("Anomaly rate (%)"),
    )
    .orderBy("Rank")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Ground-Truth Check
# MAGIC The synthetic data carries an injected label (`ground_truth_label`) for each anomaly. This table shows how the pipeline actually flagged those same reports (`pipeline_detected_as`).
# MAGIC
# MAGIC - A label that maps entirely to the same name was detected correctly.
# MAGIC - `none` in `pipeline_detected_as` means the injected anomaly was **missed**.
# MAGIC - A different anomaly name means it was **detected under another type** (flags are assigned by priority).

# COMMAND ----------

# Cell 6 — Ground Truth Comparison

display(
    spark.table(SILVER_TABLE)
    .filter(F.col("anomaly_injected").isNotNull())
    .filter(~F.col("anomaly_injected").isin("none", "unknown"))
    .groupBy("anomaly_injected", "anomaly_flag")
    .count()
    .withColumnRenamed("anomaly_injected", "ground_truth_label")
    .withColumnRenamed("anomaly_flag",     "pipeline_detected_as")
    .orderBy("ground_truth_label", F.col("count").desc())
    .select(
        F.col("ground_truth_label").alias("Injected (ground truth)"),
        F.col("pipeline_detected_as").alias("Pipeline detected as"),
        F.col("count").alias("Reports"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Port / Anchor Activity
# MAGIC Top 10 vessel–destination pairs by dwell time. One vessel can appear more than once if it has several destinations.

# COMMAND ----------

# Cell 7 — Port Activity Summary

display(
    spark.table(GOLD_PORT)
    .orderBy(F.col("dwell_time_hours").desc())
    .limit(10)
    .select(
        F.col("vessel_name").alias("Vessel"),
        F.col("vessel_type").alias("Type"),
        F.col("flag").alias("Flag"),
        F.col("destination").alias("Destination"),
        F.round("dwell_time_hours", 1).alias("Dwell time (hours)"),
        F.col("anchor_reports").alias("Stationary or anchored reports"),
        F.round("avg_latitude", 3).alias("Avg latitude (°)"),
        F.round("avg_longitude", 3).alias("Avg longitude (°)"),
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Reference
# MAGIC ### Cell 8 — Architecture
# MAGIC **Source:** `ais_raw_sample.csv` (7,160 rows · 15 vessels · 48 h window)
# MAGIC
# MAGIC | Step | Layer | Table(s) | What happens |
# MAGIC |---|---|---|---|
# MAGIC | 1 | Bronze | `vessel_positions_raw` | Raw ingest, no transforms, +3 metadata columns |
# MAGIC | 2 | Silver | `vessel_positions_clean` | Type cast, dedup, 4 anomaly rules, window functions |
# MAGIC | 3 | Gold | `fleet_summary`, `vessel_risk_scores`, `port_activity`, `anomaly_timeline` | Per-vessel, port and timeline aggregates |
# MAGIC | 4 | Reporting | this notebook | Tables above; a Databricks SQL dashboard (10 tiles: KPIs, risk, timeline, map) is **planned, not yet built** |
# MAGIC
# MAGIC **Orchestration (planned):** Lakeflow Workflow · 3-task DAG · daily 06:00 UTC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 9 — Anomaly Detection Rules
# MAGIC | Anomaly type | Rule | Severity |
# MAGIC |---|---|---|
# MAGIC | `dark_ship` | `time_gap_hours` > 2 (AIS off for more than 2 hours) | HIGH |
# MAGIC | `speed_anomaly` | `speed_over_ground` > 25 knots | HIGH |
# MAGIC | `route_deviation` | \|`course_delta`\| > 45 degrees | MEDIUM |
# MAGIC | `loitering` | speed < 2 knots AND not anchored/moored | MEDIUM |
# MAGIC
# MAGIC **Risk score components**
# MAGIC
# MAGIC | Component | Points per event | Cap |
# MAGIC |---|---|---|
# MAGIC | dark ship | 25 | 50 |
# MAGIC | speed anomaly | 15 | 30 |
# MAGIC | route deviation | 10 | 20 |
# MAGIC | loitering | 5 | 15 |
# MAGIC | high-risk flag state | +5 (flat) | – |
# MAGIC
# MAGIC Total is capped at 100. **Tiers:** HIGH ≥ 25 · MEDIUM ≥ 10 · LOW < 10

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 10 — Technology Stack
# MAGIC | Area | Choice |
# MAGIC |---|---|
# MAGIC | Platform | Databricks Free Edition (Serverless) |
# MAGIC | Table format | Delta Lake (ACID, time travel, schema enforcement) |
# MAGIC | Catalog | Unity Catalog (`bigoceandata_poc.{bronze\|silver\|gold}`) |
# MAGIC | Language | PySpark (Python 3 + Spark DataFrame API) |
# MAGIC | Orchestration | Databricks Lakeflow Workflows (3-task DAG, planned) |
# MAGIC | SQL analytics | Databricks SQL Warehouse (10 dashboard queries, planned) |
# MAGIC | Data source | Synthetic AIS CSV via Databricks Volume |
# MAGIC | Key patterns | `try_to_timestamp`, window functions, `F.least()` for caps |

# COMMAND ----------

# MAGIC %md
# MAGIC ### Cell 11 — Production Scalability Path
# MAGIC **POC:** 7,160 rows · 15 vessels · 48 h · Serverless  
# MAGIC **Production target:** 80M messages/day · 450,000+ vessels
# MAGIC
# MAGIC | # | Area | Step |
# MAGIC |---|---|---|
# MAGIC | 1 | Streaming | Replace `spark.read.csv()` with Auto Loader (`cloudFiles` format) |
# MAGIC | 2 | Compute | Autoscaling clusters on Azure/AWS Databricks Premium |
# MAGIC | 3 | Partitioning | `ZORDER BY (mmsi, timestamp)` on Silver/Gold tables |
# MAGIC | 4 | Real AIS | Connect to AISHub / MarineTraffic API / satellite AIS |
# MAGIC | 5 | Monitoring | Delta Live Tables for row count assertions per layer |
# MAGIC | 6 | Alerting | Slack/PagerDuty on HIGH-severity `anomaly_timeline` events |
# MAGIC | 7 | ML path | Replace rule-based detection with Isolation Forest / LSTM (MLflow) |

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. POC Completion Checklist

# COMMAND ----------

# Cell 12 — POC Completion Checklist

print("""
=== BigOceanData POC Completion Checklist ===

INFRASTRUCTURE
  [✓] Databricks Free Edition workspace
  [✓] Unity Catalog: bigoceandata_poc
  [✓] Schemas: bronze, silver, gold
  [✓] Volume: /Volumes/bigoceandata_poc/bronze/raw_files/
  [✓] CSV uploaded (ais_raw_sample.csv — 7,160 rows)

PIPELINE
  [✓] 00_generate_sample_data  (7 cells — data generation)
  [✓] 01_bronze_ingestion      (9 cells)
  [✓] 02_silver_transformation (13 cells)
  [✓] 03_gold_aggregation      (12 cells)
  [✓] 04_poc_summary           (12 numbered cells + markdown section headers)

DELTA TABLES
  [✓] bronze.vessel_positions_raw
  [✓] silver.vessel_positions_clean
  [✓] gold.fleet_summary
  [✓] gold.vessel_risk_scores
  [✓] gold.port_activity
  [✓] gold.anomaly_timeline

ANOMALY DETECTION
  [✓] Dark ship | Speed anomaly | Route deviation | Loitering
  [✓] Risk scoring model (0-100, 3 tiers)

DASHBOARD
  [✓] Summary notebook with formatted tables (04_poc_summary)
  [ ] 10 queries added to Databricks SQL Dashboard
  [ ] Dashboard published

ORCHESTRATION
  [ ] Lakeflow Workflow created (3-task DAG)
  [ ] Daily 06:00 UTC schedule set
""")
