# Databricks notebook source
dbutils.widgets.text("catalog","")
CATALOG=dbutils.widgets.get("catalog").strip()
dbutils.widgets.text("schema","")
SCHEMA=dbutils.widgets.get("schema").strip()

# COMMAND ----------

import json

config_nb_output = dbutils.notebook.run(
    "/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/4_logistics_usecase/generic_project/general_conf_utils_1_2/configs_path1",
    120,{"catalog": CATALOG,"schema": SCHEMA})

config_dict = json.loads(config_nb_output)

CATALOG = config_dict["CATALOG"]
SCHEMA = config_dict["SCHEMA"]
SRC=config_dict["SRC"]
BRONZE = config_dict["BRONZE"]
SILVER = config_dict["SILVER"]
GOLD = config_dict["GOLD"]
SILVERDB = config_dict["SILVERDB"]
GOLDDB = config_dict["GOLDDB"]

# COMMAND ----------

# MAGIC %run /Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/4_logistics_usecase/generic_project/general_conf_utils_1_2/util_functions2

# COMMAND ----------

shipments=f"{SILVER}/shipments"
staff=f"{SILVER}/staff"

# COMMAND ----------

spark.sql(f"""
          CREATE OR REPLACE TEMP VIEW silver_staff_geo_tv
AS
SELECT s.*  
FROM {SILVERDB}.silver_staff s
SEMI JOIN {SILVERDB}.silver_geotag s_geo
ON s.hub_location = s_geo.city_name
          """)

spark.sql(f"""
          CREATE OR REPLACE TEMP VIEW silver_staff_geo_latlong_tv
AS
SELECT s.*,s_geo.latitude,s_geo.longitude 
FROM silver_staff_geo_tv s
INNER JOIN {SILVERDB}.silver_geotag s_geo
ON s.hub_location = s_geo.city_name
          """)

# COMMAND ----------

spark.sql(f"""
          CREATE OR REPLACE TABLE {GOLDDB}.gold_core_curated_tbl
USING DELTA
AS
SELECT
    s.shipment_id,
    CONCAT(
        SUBSTRING(s.staff_full_name, 1, 2),
        '****',
        SUBSTRING(s.staff_full_name, -1, 1)
    ) AS masked_staff_name,
    s.role,
    s.origin_hub_city,
    s.latitude,
    s.longitude,
    sh.shipment_cost,
    sh.shipment_year,
    sh.shipment_month,
    sh.route_segment,
    sh.cost_per_kg,
    sh.tax_amount,
    sh.ingestion_timestamp,
    sh.is_expedited,
    sh.is_weekend,
    sh.is_high_value,
    sh.order_prefix,
    sh.order_sequence,
    sh.ship_day,
    sh.route_lane
FROM silver_staff_geo_latlong_tv s
INNER JOIN {SILVERDB}.silver_shipments sh
    USING (shipment_id);
          """)