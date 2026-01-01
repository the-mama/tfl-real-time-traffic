-- Fabric notebook source

-- METADATA ********************

-- META {
-- META   "kernel_info": {
-- META     "name": "sqldatawarehouse"
-- META   },
-- META   "dependencies": {
-- META     "lakehouse": {
-- META       "default_lakehouse": "9c05095c-888a-46d1-8f0a-c031def94f4d",
-- META       "default_lakehouse_name": "lh_tfl_traffic",
-- META       "default_lakehouse_workspace_id": "232ba7cc-995c-4e77-8728-30a3c165e464",
-- META       "known_lakehouses": [
-- META         {
-- META           "id": "9c05095c-888a-46d1-8f0a-c031def94f4d"
-- META         }
-- META       ]
-- META     },
-- META     "warehouse": {
-- META       "default_warehouse": "f388ab0f-3b7a-af64-4a81-ec17f88776c1",
-- META       "known_warehouses": [
-- META         {
-- META           "id": "f388ab0f-3b7a-af64-4a81-ec17f88776c1",
-- META           "type": "Datawarehouse"
-- META         }
-- META       ]
-- META     }
-- META   }
-- META }

-- CELL ********************

/*===========================================================
  1) CREATE TABLES (Warehouse)
     a) Staging table (loaded from Lakehouse silver.tfl_traffic_incidents)
     b) Target table
===========================================================*/

IF OBJECT_ID('dbo.stg_tfl_traffic_incidents','U') IS NULL
BEGIN
    CREATE TABLE dbo.stg_tfl_traffic_incidents
    (
        incident_id       VARCHAR(50)    NOT NULL,
        category          VARCHAR(100)   NULL,
        severity          VARCHAR(50)    NULL,
        start_ts          DATETIME2(6)   NULL,
        end_ts            DATETIME2(6)   NULL,
        location          VARCHAR(4000)  NULL,
        comments          VARCHAR(8000)  NULL,
        last_modified_ts  DATETIME2(6)   NULL,
        ingested_ts       DATETIME2(6)   NULL
    );
END;
GO

IF OBJECT_ID('dbo.tfl_traffic_incidents','U') IS NULL
BEGIN
    CREATE TABLE dbo.tfl_traffic_incidents
    (
        incident_id       VARCHAR(50)    NOT NULL,
        category          VARCHAR(100)   NULL,
        severity          VARCHAR(50)    NULL,
        start_ts          DATETIME2(6)   NULL,
        end_ts            DATETIME2(6)   NULL,
        location          VARCHAR(4000)  NULL,
        comments          VARCHAR(8000)  NULL,
        last_modified_ts  DATETIME2(6)   NULL,
        ingested_ts       DATETIME2(6)   NULL,
        dw_updated_ts     DATETIME2(6)   NULL
    );
END;
GO

-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }

-- CELL ********************

/*===========================================================
  2) MERGE (Type 1 upsert) from Warehouse staging table
===========================================================*/
MERGE dbo.tfl_traffic_incidents AS tgt
USING
(
    SELECT
        incident_id,
        category,
        severity,
        start_ts,
        end_ts,
        location,
        comments,
        last_modified_ts,
        ingested_ts
    FROM
    (
        SELECT
            s.*,
            ROW_NUMBER() OVER
            (
                PARTITION BY s.incident_id
                ORDER BY s.last_modified_ts DESC, s.ingested_ts DESC
            ) AS rn
        FROM dbo.stg_tfl_traffic_incidents s
    ) d
    WHERE d.rn = 1
) AS src
ON tgt.incident_id = src.incident_id

WHEN MATCHED
     AND (
            tgt.last_modified_ts IS NULL
            OR src.last_modified_ts > tgt.last_modified_ts
            OR (src.last_modified_ts = tgt.last_modified_ts AND src.ingested_ts > tgt.ingested_ts)
         )
THEN UPDATE SET
    tgt.category         = src.category,
    tgt.severity         = src.severity,
    tgt.start_ts         = src.start_ts,
    tgt.end_ts           = src.end_ts,
    tgt.location         = src.location,
    tgt.comments         = src.comments,
    tgt.last_modified_ts = src.last_modified_ts,
    tgt.ingested_ts      = src.ingested_ts,
    tgt.dw_updated_ts    = SYSUTCDATETIME()

WHEN NOT MATCHED THEN
    INSERT
    (
        incident_id,
        category,
        severity,
        start_ts,
        end_ts,
        location,
        comments,
        last_modified_ts,
        ingested_ts,
        dw_updated_ts
    )
    VALUES
    (
        src.incident_id,
        src.category,
        src.severity,
        src.start_ts,
        src.end_ts,
        src.location,
        src.comments,
        src.last_modified_ts,
        src.ingested_ts,
        SYSUTCDATETIME()
    );
GO


-- METADATA ********************

-- META {
-- META   "language": "sparksql",
-- META   "language_group": "synapse_pyspark"
-- META }
