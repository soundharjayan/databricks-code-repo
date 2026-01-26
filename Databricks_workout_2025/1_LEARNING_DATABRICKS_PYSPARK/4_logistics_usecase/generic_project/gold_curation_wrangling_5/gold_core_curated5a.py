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

staff = spark.read.format("delta").load(f"{SILVER}/staff")
shipments = spark.read.format("delta").load(f"{SILVER}/shipments")

joined = join_df(staff,shipments, "inner", "shipment_id")

gold_core = joined.select(
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
