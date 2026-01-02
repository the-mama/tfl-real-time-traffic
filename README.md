## TfL Road Disruptions – End-to-End Data Pipeline (Microsoft Fabric)

This project ingests live London road disruption data from the TfL Unified API and builds an analytics-ready model in Microsoft Fabric for reporting in Power BI.

### Data Source

* TfL Unified API endpoint: `https://api.tfl.gov.uk/Road/all/Disruption/`
* Authentication: TfL Application + `app_key` (created via TfL developer portal)

### Architecture (Medallion + Warehouse)

1. **Ingestion (Fabric Data Pipeline – Copy Data)**

   * Calls the TfL API on a scheduled frequency (e.g., every 5 minutes)
   * Writes raw JSON into the **Lakehouse Bronze** layer

2. **Transformation (Notebook: Bronze → Silver)**

   * Reads Bronze JSON and extracts curated columns into a Silver Delta table:

     * `incident_id, category, severity, start_ts, end_ts, location, comments, last_modified_ts, ingested_ts`
   * Standardizes timestamps, cleans fields, and deduplicates within each load to keep the most recent record per `incident_id`

3. **Serving Layer (Fabric Warehouse)**

   * Loads Silver data into a **Warehouse staging table**
   * Executes a **MERGE (Type 1 upsert)** into the final Warehouse table to maintain the latest state of each incident
   * Update logic is based on `last_modified_ts` (only newer records overwrite older ones)

4. **Orchestration**

   * Pipeline 1 (API → Bronze/Silver) runs on schedule
   * On success, it invokes Pipeline 2 (Silver → Warehouse staging + MERGE)
   * This ensures the Warehouse refresh happens only after a successful ingestion/transform run

### Reporting

* Power BI dashboard built on the Fabric Warehouse table to track:

  * Active incidents
  * Incidents by category and severity
  * Recent incidents (last 24 hours)
  * Latest refresh timestamp

### Outcome

A lightweight, production-style pipeline demonstrating:

* API ingestion and incremental refresh patterns
* Medallion architecture in Fabric (Bronze/Silver) + Warehouse serving layer
* Reliable orchestration and upsert strategy for near-real-time analytics.
