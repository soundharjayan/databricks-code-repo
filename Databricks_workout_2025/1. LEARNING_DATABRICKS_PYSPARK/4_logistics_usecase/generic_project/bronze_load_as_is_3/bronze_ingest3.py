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

# MAGIC %md
# MAGIC Setting generic configurations

# COMMAND ----------

import json
#When I run a notebook using dbutils.notebook.run(notebook,maxrunseconds,paramerters as dictionary)
#1. It will run the notebook in the respective folder (hence we don't get in the parent notebook, all the variables/values set in the child notebook). If we use %run, the child notebook variable/values can be accessed in the parent notebook.
#2. Using dbutils.notebook.run(notebook,maxrunseconds,paramerters as dictionary) - we can pass parameters to the child notebook, but %run will not allow us to pass params
config_nb_output = dbutils.notebook.run(
    "/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/4_logistics_usecase/generic_project/general_conf_utils_1_2/configs_path1",
    120,{"catalog": CATALOG,"schema": SCHEMA})
#print(SRC)#this will throw error

config_dict = json.loads(config_nb_output)
print(config_dict)

#CATALOG = config_dict["CATALOG"]
#SCHEMA = config_dict["SCHEMA"]
SRC=config_dict["SRC"]
BRONZE = config_dict["BRONZE"]
SILVER = config_dict["SILVER"]
GOLD = config_dict["GOLD"]
SILVERDB = config_dict["SILVERDB"]
GOLDDB = config_dict["GOLDDB"]

print("returned source location is ",SRC)
print("returned target bronze location is ",BRONZE)

# COMMAND ----------

# MAGIC %md
# MAGIC Defining functions

# COMMAND ----------

# MAGIC %run /Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/4_logistics_usecase/generic_project/general_conf_utils_1_2/util_functions2

# COMMAND ----------

# MAGIC %md
# MAGIC Starting the Bronze layer execution - Read data from source (SRC) datalake and load into target datalake (bronze volume)

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
#staff1=spark.read.csv(f"{SRC}/logistics_source1.txt",header=True,inferSchema=True)
staff1=read_file(spark,'csv',f"{SRC}/logistics_source1.txt",True,False)#Referring the program, rather than writing it inline
staff2=read_csv_df(spark,f"{SRC}/logistics_source2.txt",True,False)
#print(staff1.schema)
#print(staff2.schema)

#staff_bronze_same_structure=unionDf(staff1,staff2)#Needed if the EDA output says both data sources have same structure
#staff_bronze_same_structure=staff1.union(staff2)
'''view1=staff1+"_view"
view2=staff2+"_view"
df1.createOrReplaceTempView(view1)
df2.createOrReplaceTempView(view2)
staff_bronze=unionDfSql(spark,view1,view2)
'''
staff_bronze=mergeDf(staff1,staff2)#Needed if the EDA output says both data sources have different structure

#Geo tagging data read operations
geo_tagging=read_csv_df(spark,f"{SRC}/Master_City_List.csv",True,False)

#Shipment data read operations
shipments_bronze = read_json_df(spark,f"{SRC}/logistics_shipment_detail_3000.json",True)


# COMMAND ----------

#All Write ops (from datalake to the bronze layer (datalake))
#write_file(staff_bronze, f"{BRONZE}/staff", mode="overwrite", format="json")
write_file(staff_bronze, f"{BRONZE}/staff", mode="overwrite", format="delta")#datalake
write_table(staff_bronze, 'bronze_staff_table')#lakehouse (we don't do it in bronze layer in general, but in a case our Data governance team wanted to analyse/EDA the raw data in bronze layer)
write_file(geo_tagging, f"{BRONZE}/geotag", mode="overwrite", format="delta")
write_file(shipments_bronze, f"{BRONZE}/shipments", mode="overwrite", format="delta")