# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Silver Layer — Cleaning, Enrichment & Anomaly Detection
# MAGIC **BigOceanData POC | Medallion Architecture**
# MAGIC
# MAGIC Purpose: Cast types, deduplicate, apply 4 anomaly detection rules via window functions
# MAGIC
# MAGIC Output: `bigoceandata_poc.silver.vessel_positions_clean`

# COMMAND ----------

# Cell 1 — Configuration

CATALOG       = "bigoceandata_poc"
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"
BRONZE_TABLE  = f"{CATALOG}.{BRONZE_SCHEMA}.vessel_positions_raw"
SILVER_TABLE  = f"{CATALOG}.{SILVER_SCHEMA}.vessel_positions_clean"

DARK_SHIP_GAP_HOURS     = 2
LOITERING_SPEED_KNOTS   = 2.0
SPEED_ANOMALY_KNOTS     = 25.0
ROUTE_DEVIATION_DEGREES = 45

print(f"Bronze source : {BRONZE_TABLE}")
print(f"Silver target : {SILVER_TABLE}")

# COMMAND ----------

# Cell 2 — Read Bronze & Inspect Schema

from pyspark.sql import functions as F
from pyspark.sql import Window
from pyspark.sql.types import DoubleType, IntegerType, StringType, TimestampType

df_raw = spark.table(BRONZE_TABLE)
bronze_row_count = df_raw.count()
print(f"Bronze rows read: {bronze_row_count:,}")
df_raw.printSchema()

bronze_columns = set(df_raw.columns)
print(f"\nAll columns: {sorted(bronze_columns)}")

ts_field = next((f for f in df_raw.schema.fields if f.name == "timestamp"), None)
if ts_field is None:
    raise ValueError("FATAL: 'timestamp' column not found in Bronze table.")
print(f"\n'timestamp' stored as: {ts_field.dataType}")

# COMMAND ----------

# Cell 3 — Safe Type Casting

if isinstance(ts_field.dataType, TimestampType):
    df_ts = df_raw
    print("timestamp is already TimestampType — skipping cast")
else:
    df_ts = df_raw.withColumn(
        "timestamp",
        F.expr("try_to_timestamp(timestamp, 'yyyy-MM-dd\\'T\\'HH:mm:ss\\'Z\\'')")
    )
    null_ts_count = df_ts.filter(F.col("timestamp").isNull()).count()
    print(f"timestamp cast StringType → TimestampType")
    print(f"Rows with unparseable timestamp: {null_ts_count:,}")

df_typed = (
    df_ts
    .withColumn("mmsi",               F.col("mmsi").cast(StringType()))
    .withColumn("latitude",           F.col("latitude").cast(DoubleType()))
    .withColumn("longitude",          F.col("longitude").cast(DoubleType()))
    .withColumn("speed_over_ground",  F.col("speed_over_ground").cast(DoubleType()))
    .withColumn("course_over_ground", F.col("course_over_ground").cast(DoubleType()))
    .withColumn("draught",            F.col("draught").cast(DoubleType()))
)

if "true_heading" in bronze_columns:
    df_typed = df_typed.withColumn("true_heading", F.col("true_heading").cast(IntegerType()))
    print("true_heading found — cast to IntegerType")
else:
    df_typed = df_typed.withColumn("true_heading", F.lit(None).cast(IntegerType()))
    print("true_heading NOT found — added as NULL fallback")

if "length" in bronze_columns:
    df_typed = df_typed.withColumn("length", F.col("length").cast(IntegerType()))
    print("length found — cast to IntegerType")
else:
    df_typed = df_typed.withColumn("length", F.lit(None).cast(IntegerType()))
    print("length NOT found — added as NULL fallback")

if "source" in bronze_columns:
    df_typed = df_typed.withColumn("source", F.col("source").cast(StringType()))
    print("source found — kept as StringType")
else:
    df_typed = df_typed.withColumn("source", F.lit("unknown").cast(StringType()))
    print("source NOT found — added as 'unknown' fallback")

df_typed.printSchema()

# COMMAND ----------

# Cell 4 — Deduplicate & Filter Invalid Records

df_dedup = df_typed.dropDuplicates(["mmsi", "timestamp"])

df_valid = (
    df_dedup
    .filter(F.col("timestamp").isNotNull())
    .filter(F.col("latitude").isNotNull())
    .filter(F.col("longitude").isNotNull())
    .filter(F.col("latitude").between(-90.0, 90.0))
    .filter(F.col("longitude").between(-180.0, 180.0))
    .filter(F.col("speed_over_ground") >= 0.0)
    .filter(F.col("mmsi").isNotNull())
)

rows_valid   = df_valid.count()
rows_removed = bronze_row_count - rows_valid
print(f"Rows after dedup + validation : {rows_valid:,}")
print(f"Rows removed (dupes/nulls)    : {rows_removed:,}")

# COMMAND ----------

# Cell 5 — Window Functions

vessel_window = Window.partitionBy("mmsi").orderBy("timestamp")

df_windowed = (
    df_valid
    .withColumn("prev_timestamp", F.lag("timestamp", 1).over(vessel_window))
    .withColumn("prev_course",    F.lag("course_over_ground", 1).over(vessel_window))
    .withColumn("prev_speed",     F.lag("speed_over_ground", 1).over(vessel_window))
    .withColumn(
        "time_gap_hours",
        (F.unix_timestamp("timestamp") - F.unix_timestamp("prev_timestamp")) / 3600.0
    )
    .withColumn(
        "course_delta",
        F.abs(F.col("course_over_ground") - F.col("prev_course"))
    )
)
print("Window columns added: prev_timestamp, prev_course, prev_speed, time_gap_hours, course_delta")

# COMMAND ----------

# Cell 6 — Anomaly: Dark Ship

df_dark = df_windowed.withColumn(
    "is_dark_ship",
    F.when(
        F.col("time_gap_hours").isNotNull() & (F.col("time_gap_hours") > DARK_SHIP_GAP_HOURS),
        True
    ).otherwise(False)
)
dark_vessels = df_dark.filter(F.col("is_dark_ship") == True).select("mmsi").distinct().count()
print(f"Dark ship events detected across {dark_vessels} vessel(s)")
df_dark.filter(F.col("is_dark_ship") == True).select(
    "mmsi", "vessel_name", "timestamp", "prev_timestamp", "time_gap_hours"
).show(10, truncate=False)

# COMMAND ----------

# Cell 7 — Anomaly: Speed Anomaly

df_speed = df_dark.withColumn(
    "is_speed_anomaly",
    F.when(
        F.col("speed_over_ground").isNotNull() & (F.col("speed_over_ground") > SPEED_ANOMALY_KNOTS),
        True
    ).otherwise(False)
)
speed_rows = df_speed.filter(F.col("is_speed_anomaly") == True).count()
print(f"Speed anomaly rows detected: {speed_rows}")
df_speed.filter(F.col("is_speed_anomaly") == True).select(
    "mmsi", "vessel_name", "timestamp", "speed_over_ground", "vessel_type"
).show(10, truncate=False)

# COMMAND ----------

# Cell 8 — Anomaly: Route Deviation

df_route = df_speed.withColumn(
    "is_route_deviation",
    F.when(
        F.col("course_delta").isNotNull() & (F.col("course_delta") > ROUTE_DEVIATION_DEGREES),
        True
    ).otherwise(False)
)
route_rows = df_route.filter(F.col("is_route_deviation") == True).count()
print(f"Route deviation rows detected: {route_rows}")
df_route.filter(F.col("is_route_deviation") == True).select(
    "mmsi", "vessel_name", "timestamp", "course_over_ground", "prev_course", "course_delta"
).show(10, truncate=False)

# COMMAND ----------

# Cell 9 — Anomaly: Loitering

df_loiter = df_route.withColumn(
    "is_loitering",
    F.when(
        F.col("speed_over_ground").isNotNull() &
        (F.col("speed_over_ground") < LOITERING_SPEED_KNOTS) &
        F.col("navigation_status").isNotNull() &
        (~F.col("navigation_status").isin("At anchor", "Moored", "Not under command")) &
        (F.col("vessel_type") != "Passenger"),
        True
    ).otherwise(False)
)
loiter_vessels = df_loiter.filter(F.col("is_loitering") == True).select("mmsi").distinct().count()
print(f"Loitering detected for {loiter_vessels} vessel(s)")
df_loiter.filter(F.col("is_loitering") == True).select(
    "mmsi", "vessel_name", "vessel_type", "timestamp", "speed_over_ground", "navigation_status"
).show(10, truncate=False)

# COMMAND ----------

# Cell 10 — Combine Anomaly Flags

df_anomaly = df_loiter.withColumn(
    "anomaly_flag",
    F.when(F.col("is_dark_ship"),       F.lit("dark_ship"))
     .when(F.col("is_speed_anomaly"),   F.lit("speed_anomaly"))
     .when(F.col("is_route_deviation"), F.lit("route_deviation"))
     .when(F.col("is_loitering"),       F.lit("loitering"))
     .otherwise(F.lit("none"))
).withColumn(
    "is_anomalous",
    F.when(F.col("anomaly_flag") != "none", True).otherwise(False)
)
print("=== Anomaly flag distribution ===")
df_anomaly.groupBy("anomaly_flag").count().orderBy("count", ascending=False).show()

# COMMAND ----------

# Cell 11 — Add Silver Metadata & Select Final Columns

if "anomaly_injected" not in df_anomaly.columns:
    df_anomaly = df_anomaly.withColumn("anomaly_injected", F.lit("unknown"))
    print("anomaly_injected NOT in Bronze — added as 'unknown' fallback")
else:
    print("anomaly_injected column present")

df_silver = (
    df_anomaly
    .withColumn("_silver_timestamp", F.current_timestamp())
    .withColumn("_silver_version",   F.lit("1.0.0"))
    .select(
        "mmsi", "vessel_name", "vessel_type", "flag",
        "timestamp", "latitude", "longitude",
        "speed_over_ground", "course_over_ground", "true_heading",
        "navigation_status", "destination", "draught", "length", "source",
        "prev_timestamp", "time_gap_hours", "course_delta",
        "is_dark_ship", "is_speed_anomaly", "is_route_deviation", "is_loitering",
        "anomaly_flag", "is_anomalous",
        "anomaly_injected",
        "_ingestion_timestamp", "_source_file",
        "_silver_timestamp", "_silver_version",
    )
)
print(f"Silver rows ready to write: {df_silver.count():,}")
df_silver.printSchema()

# COMMAND ----------

# Cell 12 — Write to Silver Delta Table

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {SILVER_SCHEMA}")

(
    df_silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(SILVER_TABLE)
)
print(f"Silver table written: {SILVER_TABLE}")

# COMMAND ----------

# Cell 13 — Verification & Ground Truth Comparison

df_verify = spark.table(SILVER_TABLE)
total_rows = df_verify.count()
print(f"Total rows in Silver table: {total_rows:,}")

print("=== Anomaly type breakdown ===")
df_verify.groupBy("anomaly_flag").count().orderBy("count", ascending=False).show()

print("=== Per-vessel anomaly summary ===")
(
    df_verify
    .groupBy("mmsi", "vessel_name")
    .agg(
        F.count("*").alias("total_reports"),
        F.sum(F.col("is_anomalous").cast("int")).alias("anomaly_count"),
        F.collect_set("anomaly_flag").alias("anomaly_types")
    )
    .orderBy("anomaly_count", ascending=False)
    .show(20, truncate=False)
)

print("=== Ground truth vs detected ===")
(
    df_verify
    .filter(F.col("anomaly_injected").isNotNull())
    .filter(F.col("anomaly_injected") != "none")
    .filter(F.col("anomaly_injected") != "unknown")
    .groupBy("anomaly_injected", "anomaly_flag")
    .count()
    .orderBy("anomaly_injected")
    .show(truncate=False)
)