# Databricks notebook source
# MAGIC %md
# MAGIC #Telecom Domain ReadOps Assignment
# MAGIC This notebook contains assignments to practice Spark read options and Databricks volumes. <br>
# MAGIC Sections: Sample data creation, Catalog & Volume creation, Copying data into Volumes, Path glob/recursive reads, toDF() column renaming variants, inferSchema/header/separator experiments, and exercises.<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ![](https://fplogoimages.withfloats.com/actual/68009c3a43430aff8a30419d.png)
# MAGIC ![](https://theciotimes.com/wp-content/uploads/2021/03/TELECOM1.jpg)

# COMMAND ----------

# MAGIC %md
# MAGIC ##First Import all required libraries & Create spark session object

# COMMAND ----------

# MAGIC %md
# MAGIC ##1. Write SQL statements to create:
# MAGIC 1. A catalog named telecom_catalog_assign
# MAGIC 2. A schema landing_zone
# MAGIC 3. A volume landing_vol
# MAGIC 4. Using dbutils.fs.mkdirs, create folders:<br>
# MAGIC /Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/
# MAGIC /Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/
# MAGIC /Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/
# MAGIC 5. Explain the difference between (Just google and understand why we are going for volume concept for prod ready systems):<br>
# MAGIC a. Volume vs DBFS/FileStore<br>
# MAGIC b. Why production teams prefer Volumes for regulated data<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ##Data files to use in this usecase:
# MAGIC customer_csv = '''
# MAGIC 101,Arun,31,Chennai,PREPAID
# MAGIC 102,Meera,45,Bangalore,POSTPAID
# MAGIC 103,Irfan,29,Hyderabad,PREPAID
# MAGIC 104,Raj,52,Mumbai,POSTPAID
# MAGIC 105,,27,Delhi,PREPAID
# MAGIC 106,Sneha,abc,Pune,PREPAID
# MAGIC '''
# MAGIC
# MAGIC usage_tsv = '''customer_id\tvoice_mins\tdata_mb\tsms_count
# MAGIC 101\t320\t1500\t20
# MAGIC 102\t120\t4000\t5
# MAGIC 103\t540\t600\t52
# MAGIC 104\t45\t200\t2
# MAGIC 105\t0\t0\t0
# MAGIC '''
# MAGIC
# MAGIC tower_logs_region1 = '''event_id|customer_id|tower_id|signal_strength|timestamp
# MAGIC 5001|101|TWR01|-80|2025-01-10 10:21:54
# MAGIC 5004|104|TWR05|-75|2025-01-10 11:01:12
# MAGIC '''

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Filesystem operations
# MAGIC 1. Write code to copy the above datasets into your created Volume folders:
# MAGIC Customer → /Volumes/.../customer/
# MAGIC Usage → /Volumes/.../usage/
# MAGIC Tower (region-based) → /Volumes/.../tower/region1/ and /Volumes/.../tower/region2/
# MAGIC
# MAGIC 2. Write a command to validate whether files were successfully copied

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Directory Read Use Cases
# MAGIC 1. Read all tower logs using:
# MAGIC Path glob filter (example: *.csv)
# MAGIC Multiple paths input
# MAGIC Recursive lookup
# MAGIC
# MAGIC 2. Demonstrate these 3 reads separately:
# MAGIC Using pathGlobFilter
# MAGIC Using list of paths in spark.read.csv([path1, path2])
# MAGIC Using .option("recursiveFileLookup","true")
# MAGIC
# MAGIC 3. Compare the outputs and understand when each should be used.

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Schema Inference, Header, and Separator
# MAGIC 1. Try the Customer, Usage files with the option and options using read.csv and format function:<br>
# MAGIC header=false, inferSchema=false<br>
# MAGIC or<br>
# MAGIC header=true, inferSchema=true<br>
# MAGIC 2. Write a note on What changed when we use header or inferSchema  with true/false?<br>
# MAGIC 3. How schema inference handled “abc” in age?<br>

# COMMAND ----------

# MAGIC %md
# MAGIC ##5. Column Renaming Usecases
# MAGIC 1. Apply column names using string using toDF function for customer data
# MAGIC 2. Apply column names and datatype using the schema function for usage data
# MAGIC 3. Apply column names and datatype using the StructType with IntegerType, StringType, TimestampType and other classes for towers data 

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. More to come (stay motivated)....