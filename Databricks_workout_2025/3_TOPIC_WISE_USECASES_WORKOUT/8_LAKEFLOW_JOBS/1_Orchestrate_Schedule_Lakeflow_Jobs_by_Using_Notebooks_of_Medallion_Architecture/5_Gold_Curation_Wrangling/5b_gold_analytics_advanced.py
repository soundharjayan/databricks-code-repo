# Databricks notebook source
dbutils.widgets.text("catalog","")
CATALOG = dbutils.widgets.get("catalog").strip()
dbutils.widgets.text("schema","")
SCHEMA = dbutils.widgets.get("schema").strip()

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

spark = get_spark_session("Logistics Data Engineering Project")


# COMMAND ----------

from pyspark.sql.window import Window

df = read_file(spark,"delta",f"{GOLD}/core_curated")
df.show()

# Top 3 Drivers by cost per hub
w = Window.partitionBy("origin_hub_city").orderBy(F.col("shipment_cost").desc())

top3 = df.withColumn("rank",F.dense_rank().over(w)).filter("rank <= 3")
display(top3)

# LEAD & LAG

lag_df = df.withColumn(
    "previous_shipment_year", 
    F.lag("shipment_year").over(
        Window.partitionBy("masked_staff_name").orderBy("shipment_year")
        )
    )

display(lag_df)

# CUBE AGGREGATION
cube_df = df.cube("origin_hub_city") \
    .agg(F.sum("shipment_cost").alias("total_cost"))

display(cube_df)

write_file(top3,f"{GOLD}/top3_drivers")
write_file(lag_df,f"{GOLD}/prev_shipment_days")
write_file(cube_df,f"{GOLD}/cube_costs")
