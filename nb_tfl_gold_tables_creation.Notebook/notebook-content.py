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

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE gold.tfl_incidents_active_15m
# MAGIC AS
# MAGIC SELECT
# MAGIC   severity,
# MAGIC   COUNT(*) AS incident_count,
# MAGIC   current_timestamp() AS as_of_ts
# MAGIC FROM silver.tfl_traffic_incidents
# MAGIC WHERE start_ts >= current_timestamp() - INTERVAL 15 MINUTES
# MAGIC   AND (end_ts IS NULL OR end_ts >= current_timestamp())
# MAGIC GROUP BY severity;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE gold.tfl_incidents_hourly
# MAGIC AS
# MAGIC SELECT
# MAGIC   date_trunc('hour', start_ts) AS hour_bucket,
# MAGIC   severity,
# MAGIC   COUNT(*) AS incident_count
# MAGIC FROM silver.tfl_traffic_incidents
# MAGIC GROUP BY 1,2;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE OR REPLACE TABLE gold.tfl_top_roads_24h
# MAGIC AS
# MAGIC SELECT
# MAGIC   location,
# MAGIC   COUNT(*) AS incident_count
# MAGIC FROM silver.tfl_traffic_incidents
# MAGIC WHERE start_ts >= current_timestamp() - INTERVAL 24 HOURS
# MAGIC GROUP BY location
# MAGIC ORDER BY incident_count DESC
# MAGIC LIMIT 20;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC SELECT * FROM gold.tfl_incidents_active_15m;
# MAGIC SELECT * FROM gold.tfl_incidents_hourly ORDER BY hour_bucket DESC;
# MAGIC SELECT * FROM gold.tfl_top_roads_24h;


# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }
