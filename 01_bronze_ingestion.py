# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # Bronze Layer — Raw AIS Ingestion
# MAGIC **BigOceanData POC | Medallion Architecture**
# MAGIC
# MAGIC Purpose: Read raw AIS CSV from Volume → Delta Lake Bronze table (no transforms)
# MAGIC
# MAGIC Output: `bigoceandata_poc.bronze.vessel_positions_raw`

# COMMAND ----------

# Cell 1 — Configuration

CATALOG       = "bigoceandata_poc"
BRONZE_SCHEMA = "bronze"
BRONZE_TABLE  = f"{CATALOG}.{BRONZE_SCHEMA}.vessel_positions_raw"
SOURCE_PATH   = f"/Volumes/{CATALOG}/{BRONZE_SCHEMA}/raw_files/ais_raw_sample.csv"

print(f"Catalog  : {CATALOG}")
print(f"Schema   : {BRONZE_SCHEMA}")
print(f"Table    : {BRONZE_TABLE}")
print(f"Source   : {SOURCE_PATH}")

# COMMAND ----------

# Cell 2 — Set Active Catalog & Schema

spark.sql(f"USE CATALOG {CATALOG}")
spark.sql(f"USE SCHEMA {BRONZE_SCHEMA}")
print(f"Active context: {CATALOG}.{BRONZE_SCHEMA}")

# COMMAND ----------

# Cell 3 — Read CSV from Volume
# IMPORTANT: Do NOT use timestampFormat option with inferSchema.
# Keep timestamp as StringType for consistent Silver parsing.

from pyspark.sql import functions as F

df_raw = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(SOURCE_PATH)
)

print(f"Rows read from CSV : {df_raw.count():,}")
print(f"Columns            : {len(df_raw.columns)}")
df_raw.printSchema()

# COMMAND ----------

# Cell 4 — Add Ingestion Metadata Columns

df_bronze = df_raw.withColumns({
    "_ingestion_timestamp": F.current_timestamp(),
    "_source_file":         F.lit(SOURCE_PATH),
    "_pipeline_version":    F.lit("1.0.0"),
})
print("Schema after adding metadata columns:")
df_bronze.printSchema()

# COMMAND ----------

# Cell 5 — Write to Delta Bronze Table

(
    df_bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(BRONZE_TABLE)
)
print(f"Bronze table written: {BRONZE_TABLE}")

# COMMAND ----------

# Cell 6 — Verification

df_verify = spark.table(BRONZE_TABLE)
total_rows = df_verify.count()
print(f"Total rows in bronze table : {total_rows:,}")
df_verify.show(5, truncate=False)

# COMMAND ----------

# Cell 7 — Row Count Per Vessel

print("=== Row count per vessel ===")
(
    spark.table(BRONZE_TABLE)
    .groupBy("mmsi", "vessel_name", "vessel_type")
    .count()
    .orderBy("mmsi")
    .show(20, truncate=False)
)

# COMMAND ----------

# Cell 8 — Anomaly Injection Sanity Check

print("=== Injected anomaly distribution ===")
bronze_df = spark.table(BRONZE_TABLE)
if "anomaly_injected" in bronze_df.columns:
    bronze_df.groupBy("anomaly_injected").count().orderBy("count", ascending=False).show()
else:
    print("WARNING: 'anomaly_injected' column not found — wrong CSV uploaded!")
    print("Columns found:", bronze_df.columns)

# COMMAND ----------

# Cell 9 — Delta History

spark.sql("DESCRIBE HISTORY bigoceandata_poc.bronze.vessel_positions_raw").show(5, truncate=False)