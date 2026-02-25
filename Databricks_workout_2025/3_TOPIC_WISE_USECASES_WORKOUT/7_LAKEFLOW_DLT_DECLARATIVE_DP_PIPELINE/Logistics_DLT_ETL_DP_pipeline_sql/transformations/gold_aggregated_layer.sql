-- GOLD LAYER (Aggregation & Business Logic)
-- Note: Materialized Views (LIVE TABLE) are  preferred for Gold aggregations 
CREATE OR REFRESH LIVE TABLE soundhar_catalog.logistics_dlt_schema.gold_staff_geo_enriched_dlt2
COMMENT "Staff enriched with Geo Location data"
TBLPROPERTIES ("quality" = "gold")
AS SELECT 
  s.*, 
  g.latitude, 
  g.longitude
FROM soundhar_catalog.logistics_dlt_schema.silver_staff_dlt_2 s
INNER JOIN soundhar_catalog.logistics_dlt_schema.silver_geotag_2 g 
ON s.hub_location = g.city_name;

CREATE OR REFRESH LIVE TABLE soundhar_catalog.logistics_dlt_schema.gold_shipment_stats2
COMMENT "Aggregated Shipment statistics by Source City"
TBLPROPERTIES ("quality" = "gold")
AS SELECT 
  source_city,
  SUM(shipment_cost_clean) AS total_cost,
  COUNT(shipment_id) AS total_shipments,
  AVG(shipment_weight_clean) AS avg_weight
FROM soundhar_catalog.logistics_dlt_schema.silver_shipments_dlt_2
GROUP BY source_city;