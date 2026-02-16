# Databricks notebook source
dbutils.widgets.text("catalog","")
CATALOG = dbutils.widgets.get("catalog").strip()
dbutils.widgets.text("schema","")
SCHEMA = dbutils.widgets.get("schema").strip()

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

staff = read_file(spark,"delta",f"{SILVER}/staff/")
shipments = read_file(spark,"delta",f"{SILVER}/shipments/")

joined_df = join_df(staff,shipments,"inner","shipment_id")

gold_core = joined_df.select(
    "shipment_id",
    mask_name("staff_full_name").alias("masked_staff_name"),
    "role",
    "origin_hub_city",
    "shipment_cost",
    "shipment_year",
    "shipment_month",
    "route_segment",
    "cost_per_kg",
    "tax_amount",
    "ingestion_timestamp"
)

write_file(gold_core,f"{GOLD}/core_curated")
write_table(gold_core,f"{GOLDDB}.core_curated_tbl", mode="overwrite")

