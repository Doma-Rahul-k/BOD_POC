# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Gold Layer — Fleet Intelligence KPIs & Risk Scoring
# MAGIC **BigOceanData POC | Medallion Architecture**
# MAGIC
# MAGIC Purpose: Aggregate Silver data into 4 Gold tables with risk scoring model
# MAGIC
# MAGIC Output:
# MAGIC - `bigoceandata_poc.gold.fleet_summary`
# MAGIC - `bigoceandata_poc.gold.vessel_risk_scores`
# MAGIC - `bigoceandata_poc.gold.port_activity`
# MAGIC - `bigoceandata_poc.gold.anomaly_timeline`

# COMMAND ----------

# Cell 1 — Configuration

CATALOG               = "bigoceandata_poc"
SILVER_TABLE          = f"{CATALOG}.silver.vessel_positions_clean"
GOLD_FLEET_SUMMARY    = f"{CATALOG}.gold.fleet_summary"
GOLD_RISK_SCORES      = f"{CATALOG}.gold.vessel_risk_scores"
GOLD_PORT_ACTIVITY    = f"{CATALOG}.gold.port_activity"
GOLD_ANOMALY_TIMELINE = f"{CATALOG}.gold.anomaly_timeline"

print("Gold tables:")
print(f"  {GOLD_FLEET_SUMMARY}")
print(f"  {GOLD_RISK_SCORES}")
print(f"  {GOLD_PORT_ACTIVITY}")
print(f"  {GOLD_ANOMALY_TIMELINE}")

# COMMAND ----------

# Cell 2 — Read Silver

from pyspark.sql import functions as F

spark.sql(f"USE CATALOG {CATALOG}")
df_silver = spark.table(SILVER_TABLE)
print(f"Silver rows loaded: {df_silver.count():,}")
df_silver.printSchema()

# COMMAND ----------

# Cell 3 — Fleet Summary (Per Vessel KPIs)

df_fleet_summary = (
    df_silver
    .groupBy("mmsi", "vessel_name", "vessel_type", "flag", "length")
    .agg(
        F.count("*").alias("total_position_reports"),
        F.min("timestamp").alias("first_seen"),
        F.max("timestamp").alias("last_seen"),
        F.round(F.avg("speed_over_ground"), 2).alias("avg_speed_knots"),
        F.round(F.max("speed_over_ground"), 2).alias("max_speed_knots"),
        F.round(F.min("speed_over_ground"), 2).alias("min_speed_knots"),
        F.round(F.min("latitude"),  4).alias("lat_min"),
        F.round(F.max("latitude"),  4).alias("lat_max"),
        F.round(F.min("longitude"), 4).alias("lon_min"),
        F.round(F.max("longitude"), 4).alias("lon_max"),
        F.sum(F.col("is_dark_ship").cast("int")).alias("dark_ship_events"),
        F.sum(F.col("is_speed_anomaly").cast("int")).alias("speed_anomaly_events"),
        F.sum(F.col("is_route_deviation").cast("int")).alias("route_deviation_events"),
        F.sum(F.col("is_loitering").cast("int")).alias("loitering_events"),
        F.sum(F.col("is_anomalous").cast("int")).alias("total_anomaly_events"),
        F.sum(F.when(F.col("source") == "satellite_ais",   1).otherwise(0)).alias("satellite_reports"),
        F.sum(F.when(F.col("source") == "terrestrial_ais", 1).otherwise(0)).alias("terrestrial_reports"),
        F.first("destination").alias("primary_destination"),
    )
    .withColumn("anomaly_rate_pct",
        F.round(F.col("total_anomaly_events") / F.col("total_position_reports") * 100, 2))
    .withColumn("observation_hours",
        F.round((F.unix_timestamp("last_seen") - F.unix_timestamp("first_seen")) / 3600.0, 1))
    .withColumn("_gold_timestamp", F.current_timestamp())
    .orderBy("mmsi")
)
print(f"Fleet summary rows: {df_fleet_summary.count()}")
df_fleet_summary.show(5, truncate=False)

# COMMAND ----------

# Cell 4 — Write fleet_summary

(
    df_fleet_summary.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_FLEET_SUMMARY)
)
print(f"Written: {GOLD_FLEET_SUMMARY}")

# COMMAND ----------

# Cell 5 — Vessel Risk Scores
# Risk model: dark(25/cap50) + speed(15/cap30) + route(10/cap20) + loiter(5/cap15) + flag(+5)
# Tiers: HIGH>=60 | MEDIUM>=30 | LOW<30

HIGH_RISK_FLAGS = ["PA", "KM", "TG", "SL", "KH", "VU", "TV"]

df_risk = (
    spark.table(GOLD_FLEET_SUMMARY)
    .withColumn("dark_ship_score",  F.least(F.col("dark_ship_events") * 25,        F.lit(50)))
    .withColumn("speed_score",      F.least(F.col("speed_anomaly_events") * 15,    F.lit(30)))
    .withColumn("route_score",      F.least(F.col("route_deviation_events") * 10,  F.lit(20)))
    .withColumn("loitering_score",  F.least(F.col("loitering_events") * 5,         F.lit(15)))
    .withColumn("flag_risk_bonus",  F.when(F.col("flag").isin(HIGH_RISK_FLAGS), 5).otherwise(0))
    .withColumn("raw_risk_score",
        F.col("dark_ship_score") + F.col("speed_score") +
        F.col("route_score")    + F.col("loitering_score") + F.col("flag_risk_bonus"))
    .withColumn("risk_score",   F.least(F.col("raw_risk_score"), F.lit(100)))
    .withColumn("risk_tier",
        F.when(F.col("risk_score") >= 25, "HIGH")
         .when(F.col("risk_score") >= 10, "MEDIUM")
         .otherwise("LOW"))
    .select(
        "mmsi", "vessel_name", "vessel_type", "flag", "length",
        "total_position_reports", "total_anomaly_events", "anomaly_rate_pct",
        "dark_ship_events",      "dark_ship_score",
        "speed_anomaly_events",  "speed_score",
        "route_deviation_events","route_score",
        "loitering_events",      "loitering_score",
        "flag_risk_bonus", "risk_score", "risk_tier",
        "first_seen", "last_seen", "observation_hours",
        "primary_destination", "_gold_timestamp"
    )
    .orderBy("risk_score", ascending=False)
)
df_risk.select(
    "mmsi", "vessel_name", "flag", "risk_score", "risk_tier",
    "dark_ship_events", "speed_anomaly_events", "route_deviation_events", "loitering_events"
).show(20, truncate=False)

# COMMAND ----------

# Cell 6 — Write vessel_risk_scores

(
    df_risk.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_RISK_SCORES)
)
print(f"Written: {GOLD_RISK_SCORES}")

# COMMAND ----------

# Cell 7 — Port Activity

df_port_events = (
    df_silver
    .filter(
        (F.col("speed_over_ground") < 0.5) |
        (F.col("navigation_status").isin("At anchor", "Moored"))
    )
    .select(
        "mmsi", "vessel_name", "vessel_type", "flag",
        "timestamp", "latitude", "longitude",
        "speed_over_ground", "navigation_status", "destination"
    )
)

df_port_activity = (
    df_port_events
    .groupBy("mmsi", "vessel_name", "vessel_type", "flag", "destination")
    .agg(
        F.count("*").alias("anchor_reports"),
        F.min("timestamp").alias("port_entry_time"),
        F.max("timestamp").alias("port_exit_time"),
        F.round(F.avg("latitude"),  4).alias("avg_latitude"),
        F.round(F.avg("longitude"), 4).alias("avg_longitude"),
        F.round(F.avg("speed_over_ground"), 2).alias("avg_speed_while_docked"),
    )
    .withColumn("dwell_time_hours",
        F.round(
            (F.unix_timestamp("port_exit_time") - F.unix_timestamp("port_entry_time")) / 3600.0,
            2
        ))
    .withColumn("_gold_timestamp", F.current_timestamp())
    .orderBy("dwell_time_hours", ascending=False)
)
print(f"Port activity rows: {df_port_activity.count()}")
df_port_activity.show(10, truncate=False)

# COMMAND ----------

# Cell 8 — Write port_activity

(
    df_port_activity.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_PORT_ACTIVITY)
)
print(f"Written: {GOLD_PORT_ACTIVITY}")

# COMMAND ----------

# Cell 9 — Anomaly Timeline

df_anomaly_timeline = (
    df_silver
    .filter(F.col("is_anomalous") == True)
    .select(
        "mmsi", "vessel_name", "vessel_type", "flag",
        "timestamp", "latitude", "longitude",
        "speed_over_ground", "course_over_ground",
        "navigation_status", "destination",
        "anomaly_flag",
        "is_dark_ship", "is_speed_anomaly", "is_route_deviation", "is_loitering",
        "time_gap_hours", "course_delta",
        "anomaly_injected",
        "_ingestion_timestamp",
    )
    .withColumn("anomaly_severity",
        F.when(F.col("anomaly_flag") == "dark_ship",       "HIGH")
         .when(F.col("anomaly_flag") == "speed_anomaly",   "HIGH")
         .when(F.col("anomaly_flag") == "route_deviation", "MEDIUM")
         .when(F.col("anomaly_flag") == "loitering",       "MEDIUM")
         .otherwise("LOW"))
    .withColumn("_gold_timestamp", F.current_timestamp())
    .orderBy("timestamp")
)
print(f"Total anomaly events: {df_anomaly_timeline.count():,}")
df_anomaly_timeline.groupBy("anomaly_flag", "anomaly_severity").count().show()

# COMMAND ----------

# Cell 10 — Write anomaly_timeline

(
    df_anomaly_timeline.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(GOLD_ANOMALY_TIMELINE)
)
print(f"Written: {GOLD_ANOMALY_TIMELINE}")

# COMMAND ----------

# Cell 11 — Verify All Gold Tables

gold_tables = {
    "fleet_summary":      GOLD_FLEET_SUMMARY,
    "vessel_risk_scores": GOLD_RISK_SCORES,
    "port_activity":      GOLD_PORT_ACTIVITY,
    "anomaly_timeline":   GOLD_ANOMALY_TIMELINE,
}
print("=== Gold Layer Summary ===")
for name, table in gold_tables.items():
    row_count = spark.table(table).count()
    print(f"  {name:25s} → {row_count:>6,} rows   [{table}]")

# COMMAND ----------

# Cell 12 — High-Risk Vessel Alert

print("=== HIGH RISK VESSELS (Risk Score >= 60) ===")
(
    spark.table(GOLD_RISK_SCORES)
    .filter(F.col("risk_tier") == "HIGH")
    .select(
        "mmsi", "vessel_name", "vessel_type", "flag",
        "risk_score", "risk_tier",
        "dark_ship_events", "speed_anomaly_events", "total_anomaly_events"
    )
    .show(truncate=False)
)