-- GOLD LAYER (Aggregation & Business Logic)
-- Note: Materialized Views (LIVE TABLE) are  preferred for Gold aggregations 
CREATE OR REFRESH LIVE TABLE catalog2_we47.schema2_we47.gold_staff_geo_enriched_dlt2
COMMENT "Staff enriched with Geo Location data"
TBLPROPERTIES ("quality" = "gold")
AS SELECT 
  s.*, 
  g.latitude, 
  g.longitude
FROM catalog2_we47.schema2_we47.silver_staff s
INNER JOIN catalog2_we47.schema2_we47.silver_geotag_dlt g 
ON s.hub_location = g.city_name;

CREATE OR REFRESH LIVE TABLE catalog2_we47.schema2_we47.gold_shipment_stats2
COMMENT "Aggregated Shipment statistics by Source City"
TBLPROPERTIES ("quality" = "gold")
AS SELECT 
  source_city,
  SUM(shipment_cost_clean) AS total_cost,
  COUNT(shipment_id) AS total_shipments,
  AVG(shipment_weight_clean) AS avg_weight
FROM catalog2_we47.schema2_we47.silver_shipments_dlt
GROUP BY source_city;