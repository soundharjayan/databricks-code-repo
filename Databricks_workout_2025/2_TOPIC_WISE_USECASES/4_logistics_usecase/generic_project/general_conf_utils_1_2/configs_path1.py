# Databricks notebook source
dbutils.widgets.text("catalog","")
CATALOG=dbutils.widgets.get("catalog").strip()
dbutils.widgets.text("schema","")
SCHEMA=dbutils.widgets.get("schema").strip()

# COMMAND ----------

# DBTITLE 1,Cell 2
spark.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA};")

# COMMAND ----------

spark.sql(f"""CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.datalake;""")
spark.sql(f"""CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.bronze;""")
spark.sql(f"""CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.silver;""")
spark.sql(f"""CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.gold;""")

# COMMAND ----------

SRC=f"/Volumes/{CATALOG}/{SCHEMA}/datalake"
BRONZE = f"/Volumes/{CATALOG}/{SCHEMA}/bronze"
SILVER = f"/Volumes/{CATALOG}/{SCHEMA}/silver"
GOLD   = f"/Volumes/{CATALOG}/{SCHEMA}/gold"
SILVERDB= f"{CATALOG}.{SCHEMA}"
GOLDDB   = f"{CATALOG}.{SCHEMA}"
print("source datalake location",SRC)
print("bronze datalake location",BRONZE)
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