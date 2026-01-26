# Databricks notebook source
# MAGIC %md
# MAGIC ### Notebook to create and load data into databricks volume

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.mobile_metrics;

# COMMAND ----------

import requests
response = requests.get("https://public.tableau.com/app/sample-data/mobile_os_usage.csv")
dbutils.fs.put("/Volumes/workspace/default/mobile_metrics/mobile_os_usage.csv", response.text, overwrite=True)


# COMMAND ----------

# MAGIC %fs
# MAGIC ls /Volumes/workspace/default/volume1/mobile_os_usage.csv

# COMMAND ----------

# MAGIC %fs head /Volumes/workspace/default/volume1/mobile_os_usage.csv

# COMMAND ----------



# COMMAND ----------

spark.read.csv("/Volumes/workspace/default/volume1/mobile_os_usage.csv").write.saveAsTable("mobile_os_usage")

# COMMAND ----------

# MAGIC %matplotlib inline
# MAGIC import pandas as pd
# MAGIC df = pd.read_csv("/Volumes/workspace/default/volume1/mobile_os_usage.csv")
# MAGIC df.plot(kind="bar", x=df.columns[0])
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC https://web.s-cdn.boostkit.dev/webaction-files/5ac62a0f22728e050851fc87_our_faculty/face-67f16daa4404199c78d2e38b.jpg
# MAGIC ![](https://fpimages.withfloats.com/actual/6929d1ac956d0a744b5c9822.jpeg)