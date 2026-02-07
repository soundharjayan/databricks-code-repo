# Databricks notebook source
# MAGIC %md
# MAGIC #Welcome to Inceptez Technologies
# MAGIC Let us understand about creating notebooks & magical commands
# MAGIC https://fplogoimages.withfloats.com/actual/68009c3a43430aff8a30419d.png
# MAGIC ![](https://fplogoimages.withfloats.com/actual/68009c3a43430aff8a30419d.png)

# COMMAND ----------

# MAGIC %md
# MAGIC ##Let us learn first about Magical Commands
# MAGIC **Important Magic Commands**
# MAGIC - %md: allows you to write markdown text to design the notebook.
# MAGIC - %run: runs a Python file or a notebook.
# MAGIC - %sh: executes shell commands on the cluster edge/client node.
# MAGIC - %fs: allows you to interact with the Databricks file system (Datalake command (cloud storage s3/adls/gcs))
# MAGIC - %sql: allows you to run Spark SQL/HQL queries.
# MAGIC - %python: switches the notebook context to Python.
# MAGIC - %pip: allows you to install Python packages.
# MAGIC
# MAGIC **Not Important Magic Commands or We learn few of these where we have Cloud(Azure) dependency**
# MAGIC - %scala: switches the notebook context to Scala.
# MAGIC - %r: switches the notebook context to R.
# MAGIC - %lsmagic: lists all the available magic commands.
# MAGIC - %config: allows you to set configuration options for the notebook.
# MAGIC - %load: loads the contents of a file into a cell.
# MAGIC - %who: lists all the variables in the current scope.

# COMMAND ----------

# MAGIC %md
# MAGIC ####How to call a notebook from the current notebook using %run magic command

# COMMAND ----------

# MAGIC %run "/Workspace/Users/infoblisstech@gmail.com/databricks-code-repo/databricks_workouts_2025/1_DATABRICKS_NOTEBOOK_FUNDAMENTALS/4_child_notebook"

# COMMAND ----------

# MAGIC %md
# MAGIC ####How to run a linux commands inside a notebook using %sh magic command

# COMMAND ----------

# MAGIC %sh
# MAGIC ls -l /databricks-datasets/airlines
# MAGIC head -1 /databricks-datasets/airlines/part-01902

# COMMAND ----------

# MAGIC %md
# MAGIC We are going to use Databricks Unity Catalog (We don't know about it yet)
# MAGIC to create tables and files under the volume (catalog/schema/volume/folder/files)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS workspace.default.volumewe47_datalake;

# COMMAND ----------

# MAGIC %md
# MAGIC ####Upload some sample data going into (Catalog -> My Organization -> Workspace -> Default -> Volumes) <br> How to run a DBFS (like Hadoop) FS commands inside a notebook using %fs magic command to copy the uploaded data into some other volume from the uploaded volume

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %fs ls "dbfs:///Volumes/workspace/default/volumewe47_datalake"

# COMMAND ----------

# MAGIC %fs cp "dbfs:/Volumes/workspace/default/volumewe47_datalake/patients.csv" "dbfs:/Volumes/workspace/default/volumewe47_datalake/patients_copy.csv"

# COMMAND ----------

# MAGIC %md
# MAGIC _Learning for the first time the dbutils, we learn in detail later
# MAGIC Rather using fs command, we can use databricks utility command (comprehensive) to copy the data/any other filesystem operations in the DBFS_

# COMMAND ----------

dbutils.fs.cp("dbfs:/Volumes/workspace/default/volumewe47_datalake/patients.csv","dbfs:/Volumes/workspace/default/volumewe47_datalake/patients_copy2.csv")
dbutils.fs.rm("dbfs:/Volumes/workspace/default/volumewe47_datalake/patients_copy.csv")

# COMMAND ----------

# MAGIC %md
# MAGIC ####How to run a Spark SQL/HQL Queries inside a notebook using %sql magic command

# COMMAND ----------

# MAGIC %sql
# MAGIC create table if not exists default.cities2(id int,city string);
# MAGIC insert into default.cities2 values(3,'Mumbai'),(4,'Lucknow');
# MAGIC select * from cities2;

# COMMAND ----------

spark.sql("select * from cities2").explain(True)

# COMMAND ----------

# MAGIC %sql
# MAGIC update cities1 set city='Kolkata' where id=4;

# COMMAND ----------

# MAGIC %sql
# MAGIC show create table cities1;

# COMMAND ----------

# MAGIC %sql
# MAGIC from cities1 select *;

# COMMAND ----------

# MAGIC %md
# MAGIC ####How to run a Python Program inside a notebook using %python  magic command or by default the cell will be enabled with python interpretter only

# COMMAND ----------

def sqrt(a):
    return a*a

# COMMAND ----------

print("square root function call ",sqrt(10))

# COMMAND ----------

# MAGIC %md
# MAGIC In the python magic cell itself, we already have spark session object instantiated, <br>
# MAGIC so we can lavishly write spark programs

# COMMAND ----------

# MAGIC %md
# MAGIC ####How to install additional libraries in this current Python Interpreter using %pip magic command

# COMMAND ----------

# MAGIC %pip install pypi
