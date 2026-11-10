# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # BigOceanData POC — Executive Summary & Results
# MAGIC **Full pipeline audit across all Medallion layers**

# COMMAND ----------

# Cell 1 — Configuration

CATALOG       = "bigoceandata_poc"
BRONZE_TABLE  = f"{CATALOG}.bronze.vessel_positions_raw"
SILVER_TABLE  = f"{CATALOG}.silver.vessel_positions_clean"
GOLD_FLEET    = f"{CATALOG}.gold.fleet_summary"
GOLD_RISK     = f"{CATALOG}.gold.vessel_risk_scores"
GOLD_PORT     = f"{CATALOG}.gold.port_activity"
GOLD_TIMELINE = f"{CATALOG}.gold.anomaly_timeline"

print("=== Table Registry ===")
for t in [BRONZE_TABLE, SILVER_TABLE, GOLD_FLEET, GOLD_RISK, GOLD_PORT, GOLD_TIMELINE]:
    print(f"  {t}")

# COMMAND ----------

# Cell 2 — Row Count Audit: All Layers

from pyspark.sql import functions as F
spark.sql(f"USE CATALOG {CATALOG}")

layers = {
    "Bronze → vessel_positions_raw":   BRONZE_TABLE,
    "Silver → vessel_positions_clean": SILVER_TABLE,
    "Gold   → fleet_summary":          GOLD_FLEET,
    "Gold   → vessel_risk_scores":     GOLD_RISK,
    "Gold   → port_activity":          GOLD_PORT,
    "Gold   → anomaly_timeline":       GOLD_TIMELINE,
}
print(f"{'Layer':<42} {'Rows':>10}")
print("-" * 55)
for name, table in layers.items():
    try:
        count = spark.table(table).count()
        print(f"{name:<42} {count:>10,}")
    except Exception as e:
        print(f"{name:<42} {'ERROR':>10}  ({str(e)[:50]})")

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

print("=" * 50)
print("FLEET KPI SUMMARY")
print("=" * 50)
print(f"Total vessels tracked        : {total_vessels}")
print(f"Total AIS position reports   : {total_reports:,}")
print(f"Total anomaly events detected: {total_anomalies:,}")
print(f"Average anomaly rate         : {avg_anomaly_rate}%")
print(f"HIGH-risk vessels            : {high_risk_count}")
print(f"MEDIUM-risk vessels          : {medium_risk_count}")
print(f"LOW-risk vessels             : {low_risk_count}")

# COMMAND ----------

# Cell 4 — Anomaly Detection Results

print("=== Anomaly Type Breakdown (Silver) ===")
spark.table(SILVER_TABLE).groupBy("anomaly_flag").count().orderBy("count", ascending=False).show()

print("=== Anomaly Events by Severity (Gold — Anomaly Timeline) ===")
(
    spark.table(GOLD_TIMELINE)
    .groupBy("anomaly_flag", "anomaly_severity")
    .count()
    .orderBy("anomaly_severity", "count", ascending=[True, False])
    .show()
)

# COMMAND ----------

# Cell 5 — Risk Score Leaderboard

print("=== Top 10 Vessels by Risk Score ===")
(
    spark.table(GOLD_RISK)
    .select(
        "mmsi", "vessel_name", "vessel_type", "flag",
        "risk_score", "risk_tier",
        "dark_ship_events", "speed_anomaly_events",
        "route_deviation_events", "loitering_events",
        "total_anomaly_events", "anomaly_rate_pct"
    )
    .orderBy("risk_score", ascending=False)
    .show(10, truncate=False)
)

# COMMAND ----------

# Cell 6 — Ground Truth Comparison

print("=== Ground Truth vs Pipeline Detection ===")
(
    spark.table(SILVER_TABLE)
    .filter(F.col("anomaly_injected").isNotNull())
    .filter(~F.col("anomaly_injected").isin("none", "unknown"))
    .groupBy("anomaly_injected", "anomaly_flag")
    .count()
    .withColumnRenamed("anomaly_injected", "ground_truth_label")
    .withColumnRenamed("anomaly_flag",     "pipeline_detected_as")
    .orderBy("ground_truth_label")
    .show(truncate=False)
)

# COMMAND ----------

# Cell 7 — Port Activity Summary

print("=== Port / Anchor Dwell Times (Top 10) ===")
(
    spark.table(GOLD_PORT)
    .select(
        "vessel_name", "vessel_type", "flag", "destination",
        "dwell_time_hours", "anchor_reports", "avg_latitude", "avg_longitude"
    )
    .orderBy("dwell_time_hours", ascending=False)
    .show(10, truncate=False)
)

# COMMAND ----------

# Cell 8 — Architecture Diagram

print("""
╔══════════════════════════════════════════════════════════════════════════════╗
║        BigOceanData POC — Medallion Architecture on Databricks              ║
╠══════════════════════════════════════════════════════════════════════════════╣
║  SOURCE: ais_raw_sample.csv (7,160 rows, 15 vessels, 48h window)            ║
║     ↓                                                                        ║
║  BRONZE: vessel_positions_raw                                                ║
║  (raw ingest, no transforms, +3 metadata columns)                            ║
║     ↓                                                                        ║
║  SILVER: vessel_positions_clean                                              ║
║  (type cast, dedup, 4 anomaly rules, window functions)                       ║
║     ↓                                                                        ║
║  GOLD: fleet_summary | vessel_risk_scores | port_activity | anomaly_timeline ║
║                                                                              ║
║  DATABRICKS SQL DASHBOARD: 10 tiles (KPIs, risk, timeline, map)             ║
║                                                                              ║
║  ORCHESTRATION: Lakeflow Workflow → 3-task DAG → daily 06:00 UTC            ║
╚══════════════════════════════════════════════════════════════════════════════╝
""")

# COMMAND ----------

# Cell 9 — Anomaly Rules Reference

print("""
=== Anomaly Detection Rules ===
┌──────────────────┬────────────────────────────────────────────┬───────────┐
│ Anomaly Type     │ Rule                                        │ Severity  │
├──────────────────┼────────────────────────────────────────────┼───────────┤
│ dark_ship        │ time_gap_hours > 2 (AIS off > 2 hours)     │ HIGH      │
│ speed_anomaly    │ speed_over_ground > 25 knots               │ HIGH      │
│ route_deviation  │ |course_delta| > 45 degrees                │ MEDIUM    │
│ loitering        │ speed < 2 AND NOT anchored/moored          │ MEDIUM    │
└──────────────────┴────────────────────────────────────────────┴───────────┘
Risk Score: dark(25/cap50) + speed(15/cap30) + route(10/cap20) + loiter(5/cap15) + flag(+5)
Tiers: HIGH>=25 | MEDIUM>=10 | LOW<10
""")

# COMMAND ----------

# Cell 10 — Technology Stack

print("""
=== Technology Stack ===
Platform     : Databricks Free Edition (Serverless)
Table Format : Delta Lake (ACID, time travel, schema enforcement)
Catalog      : Unity Catalog (bigoceandata_poc.{bronze|silver|gold})
Language     : PySpark (Python 3 + Spark DataFrame API)
Orchestration: Databricks Lakeflow Workflows (3-task DAG)
SQL Analytics: Databricks SQL Warehouse (10 dashboard queries)
Data Source  : Synthetic AIS CSV via Databricks Volume
Key Patterns : try_to_timestamp, Window functions, F.least() for caps
""")

# COMMAND ----------

# Cell 11 — Scalability Notes

print("""
=== Production Scalability Path ===
POC: 7,160 rows | 15 vessels | 48h | Serverless
Production target: 80M messages/day | 450,000+ vessels

Steps to scale:
1. STREAMING:     Replace spark.read.csv() with Auto Loader (cloudFiles format)
2. COMPUTE:       Autoscaling clusters on Azure/AWS Databricks Premium
3. PARTITIONING:  ZORDER BY (mmsi, timestamp) on Silver/Gold tables
4. REAL AIS:      Connect to AISHub / MarineTraffic API / satellite AIS
5. MONITORING:    Delta Live Tables for row count assertions per layer
6. ALERTING:      Slack/PagerDuty on HIGH-severity anomaly_timeline events
7. ML PATH:       Replace rule-based detection with Isolation Forest / LSTM (MLflow)
""")

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
  [✓] 04_poc_summary           (12 cells)

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
  [ ] 10 queries added to Databricks SQL Dashboard
  [ ] Dashboard published

ORCHESTRATION
  [ ] Lakeflow Workflow created (3-task DAG)
  [ ] Daily 06:00 UTC schedule set
""")