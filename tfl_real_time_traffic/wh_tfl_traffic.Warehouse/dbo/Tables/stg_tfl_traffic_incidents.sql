CREATE TABLE [dbo].[stg_tfl_traffic_incidents] (

	[incident_id] varchar(50) NOT NULL, 
	[category] varchar(100) NULL, 
	[severity] varchar(50) NULL, 
	[start_ts] datetime2(6) NULL, 
	[end_ts] datetime2(6) NULL, 
	[location] varchar(4000) NULL, 
	[comments] varchar(8000) NULL, 
	[last_modified_ts] datetime2(6) NULL, 
	[ingested_ts] datetime2(6) NULL
);