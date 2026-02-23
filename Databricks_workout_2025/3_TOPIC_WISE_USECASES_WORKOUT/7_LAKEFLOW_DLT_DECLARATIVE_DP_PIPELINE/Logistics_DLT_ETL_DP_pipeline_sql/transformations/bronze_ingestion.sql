CREATE OR REFRESH STREAMING TABLE soundhar_catalog.logistics_dlt_schema.bronze_staff2
COMMENT "Raw data ingested from landing zone"
TBLPROPERTIES ("quality" = "bronze","delta.autoOptimize.optimizeWrite" = "true")
AS 
SELECT * FROM cloud_files(
  "/Volumes/soundhar_catalog/logistics_dlt_schema/datalake2/staff/",
  "csv",
  map("inferColumnTypes","true",
  "schemaEvolutionMode","addNewColumns")
);

CREATE OR REFRESH STREAMING TABLE soundhar_catalog.logistics_dlt_schema.bronze_geotag2
COMMENT "geotag raw data ingested from landing zone"
TBLPROPERTIES (
  "quality" = "bronze"
)
AS
SELECT * FROM cloud_files(
  "/Volumes/soundhar_catalog/logistics_dlt_schema/datalake2/geotag/",
  "csv",
  map(
    "inferColumnTypes","true",
    "schemaEvolutionMode","true"
  )
);

CREATE OR REFRESH STREAMING TABLE soundhar_catalog.logistics_dlt_schema.bronze_shipment_data2
COMMENT "Shipment data ingested from landing zone"
TBLPROPERTIES (
  "quality" = "bronze"
)
AS
SELECT * FROM cloud_files(
  "/Volumes/soundhar_catalog/logistics_dlt_schema/datalake2/shipments/",
  "json",
  map(
    "inferColumnTypes","true",
    "multiLine","true"
  )
)
