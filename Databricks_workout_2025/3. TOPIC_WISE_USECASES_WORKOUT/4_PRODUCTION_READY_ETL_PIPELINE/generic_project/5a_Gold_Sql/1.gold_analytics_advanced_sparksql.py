# Databricks notebook source
dbutils.widgets.text("catalog","")
CATALOG=dbutils.widgets.get("catalog").strip()
dbutils.widgets.text("schema","")
SCHEMA=dbutils.widgets.get("schema").strip()

# COMMAND ----------

import json
config_nb_output =dbutils.notebook.run("/Workspace/Users/soundharjayan@gmail.com/databricks-code-repo/Databricks_workout_2025/3_TOPIC_WISE_USECASES_WORKOUT/4_PRODUCTION_READY_ETL_PIPELINE/generic_project/1_2_general_config_utils/configs_path1",120,{"catalog":CATALOG,"schema":SCHEMA})

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

# MAGIC %run /Workspace/Users/soundharjayan@gmail.com/databricks-code-repo/Databricks_workout_2025/3_TOPIC_WISE_USECASES_WORKOUT/4_PRODUCTION_READY_ETL_PIPELINE/generic_project/1_2_general_config_utils/utils_functions

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLDDB}.gold_top3_drivers_tbl
USING DELTA
AS
SELECT * 
FROM (
    SELECT *,DENSE_RANK() OVER (PARTITION BY origin_hub_city ORDER BY shipment_cost DESC) AS rank
FROM {GOLDDB}.gold_core_curated_tbl
) 
WHERE rank <=3
""")

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLDDB}.gold_cube_costs_table
USING DELTA
AS
SELECT 
    origin_hub_city,
    SUM(shipment_cost) as total_cost
FROM {GOLDDB}.gold_core_curated_tbl
GROUP BY CUBE(origin_hub_city)
""")