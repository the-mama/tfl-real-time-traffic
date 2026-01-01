# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "9c05095c-888a-46d1-8f0a-c031def94f4d",
# META       "default_lakehouse_name": "lh_tfl_traffic",
# META       "default_lakehouse_workspace_id": "232ba7cc-995c-4e77-8728-30a3c165e464",
# META       "known_lakehouses": [
# META         {
# META           "id": "9c05095c-888a-46d1-8f0a-c031def94f4d"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# ##### Read Bronze JSON from OneLake
# - Handles multiline JSON arrays


# CELL ********************

from pyspark.sql.functions import current_timestamp

bronze_path = "Files/traffic/tfl/bronze/incidents/"

df_bronze = (
    spark.read
         .option("multiLine", "true")
         .json(bronze_path)
         .withColumn("ingested_ts", current_timestamp())
)

display(df_bronze)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Inspect and Select Required Fields
# -Apply Silver Transformations

# CELL ********************

from pyspark.sql.functions import col, to_timestamp

df_silver = (
    df_bronze
    .select(
        col("id").alias("incident_id"),
        col("category"),
        col("severity"),
        to_timestamp(col("startDateTime")).alias("start_ts"),
        to_timestamp(col("endDateTime")).alias("end_ts"),
        col("location"),
        col("comments"),
        to_timestamp(col("lastModifiedTime")).alias("last_modified_ts"),
        col("ingested_ts")
    )
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Deduplicate Incidents
# 
# TfL returns the same incident multiple times until resolved.
# ###### Rule:
# - Keep latest version per incident_id
# - Use last_modified_ts

# CELL ********************

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

window_spec = Window.partitionBy("incident_id").orderBy(col("last_modified_ts").desc())

df_silver_deduped = (
    df_silver
    .withColumn("rn", row_number().over(window_spec))
    .filter(col("rn") == 1)
    .drop("rn")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Write to Silver Delta Table

# CELL ********************

(
    df_silver_deduped
    .write
    .format("delta")
    .mode("append")
    .saveAsTable("silver.tfl_traffic_incidents")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### Validate the Silver Table

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT
# MAGIC   severity,
# MAGIC   COUNT(*) AS incident_count
# MAGIC FROM silver.tfl_traffic_incidents
# MAGIC GROUP BY severity
# MAGIC ORDER BY incident_count DESC;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS control.gold_watermarks (
# MAGIC   job_name STRING,
# MAGIC   last_processed_ts TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC INSERT INTO control.gold_watermarks
# MAGIC VALUES ('tfl_incidents_hourly', TIMESTAMP '1900-01-01');


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
