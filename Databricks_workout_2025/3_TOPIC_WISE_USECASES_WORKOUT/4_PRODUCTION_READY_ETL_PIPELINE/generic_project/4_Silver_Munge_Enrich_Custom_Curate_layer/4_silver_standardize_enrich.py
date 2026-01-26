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

spark = get_spark_session("Logistics Data Engineering Project")

# COMMAND ----------

staff = read_file(spark,"delta",f"{BRONZE}/staff/")
geo_tag = read_file(spark,"delta",f"{BRONZE}/geo_tagging/")
shipments = read_file(spark,"delta",f"{BRONZE}/shipments/")


# COMMAND ----------

silver_staff = standardize_staff(staff)

silver_geotag = scrub_geotag(geo_tag).distinct()

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