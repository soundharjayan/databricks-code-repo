CREATE OR REFRESH STREAMING TABLE catalog2_we47.schema2_we47.bronze_staff2
COMMENT "Raw staff data ingested from landing zone"
TBLPROPERTIES ("quality" = "bronze",
  "delta.autoOptimize.optimizeWrite" = "true")
AS SELECT * FROM cloud_files(
  "/Volumes/catalog1_we47/schema1_we47/datalake/staff", 
  "csv", 
  map("inferColumnTypes", "true",
    "schemaEvolutionMode", "addNewColumns"));

CREATE OR REFRESH STREAMING TABLE catalog2_we47.schema2_we47.bronze_geotag2
COMMENT "Raw geotag data ingested from landing zone"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT * FROM cloud_files(
  "/Volumes/catalog1_we47/schema1_we47/datalake/geotag", 
  "csv", 
  map("inferColumnTypes", "true")
);

CREATE OR REFRESH STREAMING TABLE catalog2_we47.schema2_we47.bronze_shipments2
COMMENT "Raw shipments data ingested from landing zone"
TBLPROPERTIES ("quality" = "bronze")
AS SELECT
  shipment_id,
  order_id,
  source_city,
  destination_city,
  shipment_status,
  cargo_type,
  vehicle_type,
  payment_mode,
  shipment_weight_kg,
  shipment_cost,
  shipment_date
FROM cloud_files(
  "/Volumes/catalog1_we47/schema1_we47/datalake/shipment/", 
  "json", 
  map("inferColumnTypes", "true", "multiLine", "true")
);