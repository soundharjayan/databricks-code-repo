# Databricks notebook source
# MAGIC %md
# MAGIC #Enterprise Fleet Analytics Pipeline: Focuses on the business outcome (analytics) and the domain (fleet/logistics).

# COMMAND ----------

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

spark = get_spark_session("Logistics Data Engineering Project")

# COMMAND ----------

#All Read ops
#Staff data read operations

staff_df1 = read_file(spark,"csv",f"{SRC}/logistics_source1",True,False,",")
staff_df2 = read_file(spark,"csv",f"{SRC}/logistics_source2",True,False,",")
print(staff_df1.schema)
print(staff_df2.schema)

staff_bronze = mergeDF(staff_df1,staff_df2)

geo_tagging_df= read_csv_df(spark,f"{SRC}/Master_City_List.csv",True,False,",")
print(geo_tagging_df)

shipments_df = read_file(spark,"json",f"{SRC}/logistics_shipment_detail_3000.json",True)
print(shipments_df)

# COMMAND ----------

# All write ops ( from Datalake to Bronze layer)

write_file(staff_bronze,f"{BRONZE}/staff",mode="overwrite",format="delta")
write_file(geo_tagging_df,f"{BRONZE}/geo_tagging",mode="overwrite",format="delta")
write_file(shipments_df,f"{BRONZE}/shipments",mode="overwrite",format="delta")

write_table(staff_df1,"bronze_staff_table")    # we don't write the df as a table in bronze layer in general