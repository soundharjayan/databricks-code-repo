CREATE OR REFRESH STREAMING TABLE catalog2_we47.schema2_we47.silver_staff_dlt2
COMMENT "Standardized staff data"
-- Adding the expectation here
AS
SELECT
  CAST(shipment_id AS BIGINT) AS shipment_id,
  CAST(age AS INT) AS age,
  LOWER(role) AS role,
  INITCAP(hub_location) AS origin_hub_city,
  current_timestamp() AS load_dt,
  CONCAT_WS(' ', first_name, last_name) AS staff_full_name,
  INITCAP(hub_location) AS hub_location
FROM STREAM(catalog2_we47.schema2_we47.bronze_staff2);

CREATE OR REFRESH STREAMING TABLE catalog2_we47.schema2_we47.silver_geotag_dlt2
COMMENT "Cleaned geotag data"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT DISTINCT
  INITCAP(city_name) AS city_name,
  INITCAP(country) AS masked_hub_location,
  latitude,
  longitude
FROM STREAM(catalog2_we47.schema2_we47.bronze_geotag2);

CREATE OR REFRESH STREAMING TABLE catalog2_we47.schema2_we47.silver_shipments_dlt2
COMMENT "Enriched and split shipments data"
TBLPROPERTIES ("quality" = "silver")
AS
SELECT
  *,
  'Logistics' AS domain,
  current_timestamp() AS ingestion_timestamp,
  CAST(False AS BOOLEAN) AS is_expedited_flag_initial,
  TO_DATE(shipment_date, 'yy-MM-dd') AS shipment_date_clean,
  ROUND(shipment_cost, 2) AS shipment_cost_clean,
  CAST(shipment_weight_kg AS DOUBLE) AS shipment_weight_clean,
  CONCAT_WS('-', source_city, destination_city) AS route_segment,
  CONCAT_WS('_', vehicle_type, shipment_id) AS vehicle_identifier,
  YEAR(TO_DATE(shipment_date, 'yy-MM-dd')) AS shipment_year,
  MONTH(TO_DATE(shipment_date, 'yy-MM-dd')) AS shipment_month,
  CASE WHEN dayofweek(TO_DATE(shipment_date, 'yy-MM-dd')) IN (1, 7) THEN TRUE ELSE FALSE END AS is_weekend,
  CASE WHEN shipment_status IN ('IN_TRANSIT', 'DELIVERED') THEN TRUE ELSE FALSE END AS is_expedited,
  ROUND(shipment_cost / shipment_weight_kg, 2) AS cost_per_kg,
  ROUND(shipment_cost * 0.18, 2) AS tax_amount,
  DATEDIFF(current_date(), TO_DATE(shipment_date, 'yy-MM-dd')) AS days_since_shipment,
  CASE WHEN shipment_cost > 50000 THEN TRUE ELSE FALSE END AS is_high_value,
  SUBSTRING(order_id, 1, 3) AS order_prefix,
  SUBSTRING(order_id, 4, 10) AS order_sequence,
  DAY(TO_DATE(shipment_date, 'yy-MM-dd')) AS ship_day,
  CONCAT_WS('->', source_city, destination_city) AS route_lane
FROM STREAM(catalog2_we47.schema2_we47.bronze_shipments2);

