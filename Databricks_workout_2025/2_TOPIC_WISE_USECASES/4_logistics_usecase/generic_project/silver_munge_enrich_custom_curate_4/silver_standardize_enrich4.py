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

# DBTITLE 1,Untitled
from pyspark.sql import functions as F

staff = spark.read.format("delta").load(f"{BRONZE}/staff")
geotag=spark.read.format("delta").load(f"{BRONZE}/geotag")
shipments = spark.read.format("delta").load(f"{BRONZE}/shipments")


# COMMAND ----------

silver_staff = standardize_staff(staff)

silver_geotag=scrub_geotag(geotag).distinct()

silver_shipments = (shipments.where("shipment_weight_kg>0")
    .transform(standardize_shipments)
    .transform(enrich_shipments)
    .transform(split_columns))

# COMMAND ----------

write_file(silver_staff,f"{SILVER}/staff",mode="overwrite",format="delta")
write_file(silver_geotag,f"{SILVER}/geotag",mode="overwrite",format="delta")
write_file(silver_shipments,f"{SILVER}/shipments",mode="overwrite",format="delta")

write_table(silver_staff,f"{SILVERDB}.silver_staff", mode="overwrite")
write_table(silver_geotag,f"{SILVERDB}.silver_geotag", mode="overwrite")
write_table(silver_shipments,f"{SILVERDB}.silver_shipments", mode="overwrite")