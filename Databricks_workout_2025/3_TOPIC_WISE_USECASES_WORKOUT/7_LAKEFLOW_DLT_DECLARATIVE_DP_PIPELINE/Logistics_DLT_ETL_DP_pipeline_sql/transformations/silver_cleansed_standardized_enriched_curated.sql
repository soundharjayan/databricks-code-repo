CREATE OR REFRESH STREAMING TABLE soundhar_catalog.logistics_dlt_schema.silver_staff_dlt_2
COMMENT "Standardized staff data"
AS
SELECT 
  CAST("shipment_id" AS BIGINT) AS shipment_id,
  CAST("age" AS BIGINT) AS age,
  LOWER("role") AS role,
  INITCAP("hub_location") AS origin_hub_city,
  CURRENT_TIMESTAMP() AS load_dt,
  CONCAT_WS(" ","first_name","last_name") AS staff_full_name,
  INITCAP("hub_location") AS hub_location
FROM STREAM(soundhar_catalog.logistics_dlt_schema.bronze_staff2);

CREATE OR REFRESH STREAMING TABLE soundhar_catalog.logistics_dlt_schema.silver_geotag_2
COMMENT " geotag cleansed data"
TBLPROPERTIES (
  "quality" = "silver"
)
AS
SELECT DISTINCT 
  INITCAP(city_name) AS city_name,
  INITCAP(country) AS masked_hub_location,
  latitude,
  longitude
FROM STREAM(soundhar_catalog.logistics_dlt_schema.bronze_geotag2);

