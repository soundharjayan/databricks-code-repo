# Databricks notebook source
# MAGIC %md
# MAGIC # Healthcare Data Utilities Usecase2
# MAGIC
# MAGIC ## Objective
# MAGIC This notebook demonstrates how to design Databricks notebooks using Markdown
# MAGIC and how to work with Databricks utilities such as dbutils.fs, dbutils.widgets,
# MAGIC and dbutils.notebook using Volumes.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Project Workflow
# MAGIC 1. Create folder structure using Volumes
# MAGIC 2. Create sample healthcare data
# MAGIC 3. Perform file operations using dbutils.fs
# MAGIC 4. Parameterize execution using widgets
# MAGIC 5. Exit notebook with execution status

# COMMAND ----------

# MAGIC %md
# MAGIC ## Folder Structure
# MAGIC
# MAGIC | Folder | Purpose |
# MAGIC |------|---------|
# MAGIC | raw | Incoming healthcare files |
# MAGIC | processed | Validated healthcare data |
# MAGIC | archive | Historical data |
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Learning Outcome
# MAGIC Our Aspirants will understand notebook design, parameterization, and fs, notebook, widgets using Databricks utilities.

# COMMAND ----------

# MAGIC %md
# MAGIC #1. Define Base Paths using python variable <br>
# MAGIC base_path = "/Volumes/workspace/default/volumewd36" <br>
# MAGIC Create raw_path, processed_path and archive_path as given below... <br>
# MAGIC raw_path = f"{base_path}/raw" <br>
# MAGIC processed_path = f"{base_path}/processed" <br>
# MAGIC archive_path = f"{base_path}/archive"

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating base path  "/Volumes/workspace/default/volumewd36"

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.volumewd36;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Creating raw path,processed path and archive path

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /Volumes/workspace/default/volumewd36/raw

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /Volumes/workspace/default/volumewd36/raw

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /Volumes/workspace/default/volumewd36/processed

# COMMAND ----------

# MAGIC %fs
# MAGIC mkdirs /Volumes/workspace/default/volumewd36/archive

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. dbutils Usecase – Create Directories using the above path variables..

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/default/volumewd36/raw")
dbutils.fs.mkdirs("/Volumes/workspace/default/volumewd36/processed")
dbutils.fs.mkdirs("/Volumes/workspace/default/volumewd36/archive")

# COMMAND ----------

# MAGIC %md
# MAGIC # 3. dbutils Usecase – Create Sample Healthcare File <br>
# MAGIC sample_data = """patient_id,patient_name,age,gender
# MAGIC 1,John Doe,68,M
# MAGIC 2,Jane Smith,54,F
# MAGIC """
# MAGIC TODO: Write this file content into raw folder created earlier... using dbutils.fs.......

# COMMAND ----------

sample_data = """patient_id,patient_name,age,gender
1,John Doe,68,M
2,Jane Smith,54,F
"""
dbutils.fs.put("/Volumes/workspace/default/volumewd36/raw/sample_healthcare.txt",sample_data,overwrite=True)

# COMMAND ----------

# MAGIC %md
# MAGIC #4. dbutils Usecase - list the file created <br>
# MAGIC TODO: List all files available in raw folder using the dbutils command <br>
# MAGIC dbutils.fs......

# COMMAND ----------

dbutils.fs.ls("dbfs:/Volumes/workspace/default/volumewd36/raw")

# COMMAND ----------

# MAGIC %md
# MAGIC #5. dbutils Usecase – Copy File (raw → processed)

# COMMAND ----------

dbutils.fs.cp("dbfs:/Volumes/workspace/default/volumewd36/raw/sample_healthcare.txt","dbfs:/Volumes/workspace/default/volumewd36/processed/sample_healthcare.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC #6. dbutils widget usecase - Create dropdown and text widgets... <br>
# MAGIC TODO: Create a dropdown widget for environment (dev, qa, prod) using <br>
# MAGIC TODO: Create a text widget for owner name
# MAGIC
# MAGIC

# COMMAND ----------

#Dropdown widget for environment

dbutils.widgets.dropdown(
  name ='Environment',
  defaultValue='dev',
  choices=['dev','prod','qa'],
  label='Select Environment'
  )

#Text widget for owner name
dbutils.widgets.text(
  name = 'owner_name',
  defaultValue = 'Soundhar',
  label = 'Enter your name'
)

# COMMAND ----------

# MAGIC %md
# MAGIC #7. dbutils widget Usecase – Read Widget Values environment and owner and print in the screen
# MAGIC

# COMMAND ----------

#Read widget values
env = dbutils.widgets.get('department')
owner = dbutils.widgets.get('owner_name')

#print values on the screen
print(f"Selected Department: {env}")
print (f"Owner Name: {owner}")

# COMMAND ----------

# MAGIC %md
# MAGIC #8. dbutils widget Usecase – Move the above processed File to Archive

# COMMAND ----------

dbutils.fs.mv("dbfs:/Volumes/workspace/default/volumewd36/processed/sample_healthcare.txt","dbfs:/Volumes/workspace/default/volumewd36/archive/sample_healthcare.txt")

# COMMAND ----------

# MAGIC %md
# MAGIC #9. dbutils notebook usecase - Run the notebook4 using the dbutils command
# MAGIC /Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/databricks_workouts_2025/1_USECASES_NB_FUNDAMENTALS/4_child_nb_dataload

# COMMAND ----------

dbutils.notebook.run("/Workspace/Users/soundharjayan@gmail.com/databricks-code-repo/Databricks_workout_2025/USECASES_WORKOUT/2.Child_notebook_dataload",60)

# COMMAND ----------

# MAGIC %md
# MAGIC #10. dbutils notebook usecase - exit this notebook 
# MAGIC TODO: Exit notebook with a success message
# MAGIC dbutils.notebook._____("Pipeline completed successfully")
# MAGIC

# COMMAND ----------

dbutils.notebook.exit("Finished Successfully")