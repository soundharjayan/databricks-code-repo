# Databricks notebook source
dbutils.widgets.text("catalog","")
CATALOG = dbutils.widgets.get("catalog").strip()

dbutils.widgets.text("schema","")
SCHEMA = dbutils.widgets.get("schema").strip()


# COMMAND ----------

spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.datalake")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.bronze")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.silver")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.gold")

# COMMAND ----------

SRC = f"/Volumes/{CATALOG}/{SCHEMA}/datalake"   # the datalake,bronze,silver,gold these all are volumes or datalake where we can store the files system
BRONZE = f"/Volumes/{CATALOG}/{SCHEMA}/bronze"        
SILVER = f"/Volumes/{CATALOG}/{SCHEMA}/silver"
GOLD = f"/Volumes/{CATALOG}/{SCHEMA}/gold"

SILVERDB = f"{CATALOG}.{SCHEMA}"           # the silverdb,golddb these all are lakehouse where we can store the delta tables
GOLDDB = f"{CATALOG}.{SCHEMA}"

print("source datalake location",SRC)
print("bronze datalake location",BRONZE)
print("bronze datalake location",SILVER)
print("bronze datalake location",GOLD)
print("silver database",SILVERDB)
print("gold database",GOLDDB)



# COMMAND ----------

import json
dbutils.notebook.exit(
    json.dumps({
        "CATALOG": CATALOG,
        "SCHEMA": SCHEMA,
        "SRC": SRC,
        "BRONZE": BRONZE,
        "SILVER": SILVER,
        "GOLD": GOLD,
        "SILVERDB": SILVERDB,
        "GOLDDB": GOLDDB
    })
)
