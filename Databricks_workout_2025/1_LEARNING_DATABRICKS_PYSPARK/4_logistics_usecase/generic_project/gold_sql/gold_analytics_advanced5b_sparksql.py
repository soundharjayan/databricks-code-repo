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

spark.sql(f"""
CREATE OR REPLACE TABLE {GOLDDB}.gold_top3_drivers_tbl
USING DELTA
AS
SELECT *
FROM (
    SELECT *,
           DENSE_RANK() OVER (
               PARTITION BY origin_hub_city
               ORDER BY shipment_cost DESC
           ) AS rank
    FROM {GOLDDB}.gold_core_curated_tbl
)
WHERE rank <= 3
""")



# COMMAND ----------

#We are building a Lakehouse table
spark.sql(f"""
CREATE OR REPLACE TABLE {GOLDDB}.gold_cube_costs_tbl
USING DELTA
AS
SELECT
    origin_hub_city,
    SUM(shipment_cost) AS total_cost
FROM {GOLDDB}.gold_core_curated_tbl
GROUP BY CUBE (origin_hub_city)
""")
