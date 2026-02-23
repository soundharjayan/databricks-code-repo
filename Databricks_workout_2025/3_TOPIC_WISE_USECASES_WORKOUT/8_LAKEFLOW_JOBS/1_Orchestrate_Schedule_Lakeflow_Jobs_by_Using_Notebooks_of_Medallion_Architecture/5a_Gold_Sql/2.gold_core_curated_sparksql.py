# Databricks notebook source
dbutils.widgets.text("catalog","")
CATALOG=dbutils.widgets.get("catalog").strip()
dbutils.widgets.text("schema","")
SCHEMA=dbutils.widgets.get("schema").strip()

# COMMAND ----------

import json
config_nb_output =dbutils.notebook.run("/Workspace/Users/soundharjayan@gmail.com/databricks-code-repo/Databricks_workout_2025/3_TOPIC_WISE_USECASES_WORKOUT/8_LAKEFLOW_JOBS/1_Orchestrate_Schedule_Lakeflow_Jobs_by_Using_Notebooks_of_Medallion_Architecture/1_2_general_config_utils/configs_path1",120,{"catalog":CATALOG,"schema":SCHEMA})

config_dict = json.loads(config_nb_output)

print(config_dict)

CATALOG = config_dict["CATALOG"]
SCHEMA = config_dict["SCHEMA"]
SRC=config_dict["SRC"]
BRONZE = config_dict["BRONZE"]
SILVER = config_dict["SILVER"]
GOLD = config_dict["GOLD"]
SILVERDB = config_dict["SILVERDB"]
GOLDDB = config_dict["GOLDDB"]

print("returned source location is ",SRC)
print("returned target bronze location is ",BRONZE)

# COMMAND ----------

# MAGIC %run /Workspace/Users/soundharjayan@gmail.com/databricks-code-repo/Databricks_workout_2025/3_TOPIC_WISE_USECASES_WORKOUT/8_LAKEFLOW_JOBS/1_Orchestrate_Schedule_Lakeflow_Jobs_by_Using_Notebooks_of_Medallion_Architecture/1_2_general_config_utils/utils_functions

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW silver_staff_geo_tv
AS
SELECT s.*
FROM {SILVERDB}.silver_staff s
SEMI JOIN {SILVERDB}.silver_geotag g
ON s.hub_location = g.city_name
""")

spark.sql(f"""
          CREATE OR REPLACE TEMP VIEW silver_staff_geo_latlong_tv
AS
SELECT s.*,g.latitude,g.longitude 
FROM {SILVERDB}.silver_staff s
INNER JOIN {SILVERDB}.silver_geotag g
ON s.hub_location = g.city_name
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
