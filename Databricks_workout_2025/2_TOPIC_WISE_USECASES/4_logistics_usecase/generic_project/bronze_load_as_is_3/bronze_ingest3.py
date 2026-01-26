# Databricks notebook source
# MAGIC %md
# MAGIC ###Enterprise Fleet Analytics Pipeline: Focuses on the business outcome (analytics) and the domain (fleet/logistics).

# COMMAND ----------

# MAGIC %md
# MAGIC ![](/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/4_logistics_usecase/generic_project/general_conf_utils_1_2/medallion.png)

# COMMAND ----------

dbutils.widgets.text("catalog","")
CATALOG=dbutils.widgets.get("catalog").strip()
dbutils.widgets.text("schema","")
SCHEMA=dbutils.widgets.get("schema").strip()

# COMMAND ----------

#As we are parameterizing, we don't need to hardcode, which is not production ready..
#CATALOG='prodcatalog'
#SCHEMA='logistics'

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

print("returned source location is ",SRC)
print("returned target bronze location is ",BRONZE)

# COMMAND ----------

# MAGIC %run /Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/4_logistics_usecase/generic_project/general_conf_utils_1_2/util_functions2

# COMMAND ----------

#Adapting Generic Framework
spark=get_spark_session("Logistics Data Engineering Project")

# COMMAND ----------

#No Adoption of Generic Framework (Inline programming)
from pyspark.sql.session import SparkSession
spark = SparkSession.builder.appName("Logistics Data Engineering Project").getOrCreate()
'''
We lost all the below features...
    Centralized and controllable
    Production Ready
    Reusability
    Seperation of Concern
    Modularized
    Simple/Reasonable
    Optimization
'''

# COMMAND ----------

#All Read ops
#Staff data read operations
staff1=read_file(spark,'csv',f"{SRC}/logistics_source1.txt",True,False)
staff2=read_csv_df(spark,f"{SRC}/logistics_source2.txt",True,False)
print(staff1.schema)
print(staff2.schema)

staff_bronze=mergeDf(staff1,staff2)

#Geo tagging data read operations
geo_tagging=read_csv_df(spark,f"{SRC}/Master_City_List.csv",True,False)

#Shipment data read operations
shipments_bronze = read_json_df(spark,f"{SRC}/logistics_shipment_detail_3000.json",True)


# COMMAND ----------

#All Write ops (from datalake to the bronze layer (datalake))
#write_file(staff_bronze, f"{BRONZE}/staff", mode="overwrite", format="json")
write_file(staff_bronze, f"{BRONZE}/staff", mode="overwrite", format="delta")#datalake
write_table(staff_bronze, 'bronze_staff_table')#lakehouse (we don't do it in bronze layer in general)
write_file(geo_tagging, f"{BRONZE}/geotag", mode="overwrite", format="delta")
write_file(shipments_bronze, f"{BRONZE}/shipments", mode="overwrite", format="delta")