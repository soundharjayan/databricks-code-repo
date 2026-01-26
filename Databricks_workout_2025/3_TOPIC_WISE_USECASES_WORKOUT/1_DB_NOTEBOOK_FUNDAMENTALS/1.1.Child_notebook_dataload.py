# Databricks notebook source
# MAGIC %md
# MAGIC # Reading the data by calling API
# MAGIC - Create a child notebook "4_child_nb_dataload" and write code to load data, Using the requests library, perform api call to pull data from "https://public.tableau.com/app/sample-data/mobile_os_usage.csv" into a python variable using the magic command %py

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC import requests
# MAGIC
# MAGIC url = "https://public.tableau.com/app/sample-data/mobile_os_usage.csv"
# MAGIC
# MAGIC response = requests.get(url)
# MAGIC
# MAGIC # Validate the request succeeded
# MAGIC if response.status_code != 200:
# MAGIC     raise Exception(f"Failed to download file. Status Code: {response.status_code}")
# MAGIC
# MAGIC csv_text = response.text
# MAGIC print("Data downloaded successfully!")

# COMMAND ----------

# MAGIC %py
# MAGIC
# MAGIC volume_path = "/Volumes/workspace/default/usage_metrics/mobile_os_usage.csv"
# MAGIC
# MAGIC dbutils.fs.put(volume_path, csv_text, overwrite=True)
# MAGIC
# MAGIC print(f"File written to: {volume_path}")