# Databricks notebook source
# MAGIC %md
# MAGIC #Telecom Domain Read & Write Ops Hackathon - Building Datalake & Lakehouse
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
# MAGIC 1. Write dbutils.fs code to copy the above datasets into your created Volume folders:
# MAGIC Customer → /Volumes/.../customer/
# MAGIC Usage → /Volumes/.../usage/
# MAGIC Tower (region-based) → /Volumes/.../tower/region1/ and /Volumes/.../tower/region2/
# MAGIC
# MAGIC 2. Write a command to validate whether files were successfully copied

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Spark Directory Read Use Cases
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
# MAGIC ## Spark Write Operations using 
# MAGIC - csv, json, orc, parquet, delta, saveAsTable, insertInto, xml with different write mode, header and sep options

# COMMAND ----------

# MAGIC %md
# MAGIC ##6. Write Operations (Data Conversion/Schema migration) – CSV Format Usecases
# MAGIC 1. Write customer data into CSV format using overwrite mode
# MAGIC 2. Write usage data into CSV format using append mode
# MAGIC 3. Write tower data into CSV format with header enabled and custom separator (|)
# MAGIC 4. Read the tower data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

# MAGIC %md
# MAGIC ##7. Write Operations (Data Conversion/Schema migration)– JSON Format Usecases
# MAGIC 1. Write customer data into JSON format using overwrite mode
# MAGIC 2. Write usage data into JSON format using append mode and snappy compression format
# MAGIC 3. Write tower data into JSON format using ignore mode and observe the behavior of this mode
# MAGIC 4. Read the tower data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

# MAGIC %md
# MAGIC ##8. Write Operations (Data Conversion/Schema migration) – Parquet Format Usecases
# MAGIC 1. Write customer data into Parquet format using overwrite mode and in a gzip format
# MAGIC 2. Write usage data into Parquet format using error mode
# MAGIC 3. Write tower data into Parquet format with gzip compression option
# MAGIC 4. Read the usage data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

# MAGIC %md
# MAGIC ##9. Write Operations (Data Conversion/Schema migration) – Orc Format Usecases
# MAGIC 1. Write customer data into ORC format using overwrite mode
# MAGIC 2. Write usage data into ORC format using append mode
# MAGIC 3. Write tower data into ORC format and see the output file structure
# MAGIC 4. Read the usage data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

# MAGIC %md
# MAGIC ##10. Write Operations (Data Conversion/Schema migration) – Delta Format Usecases
# MAGIC 1. Write customer data into Delta format using overwrite mode
# MAGIC 2. Write usage data into Delta format using append mode
# MAGIC 3. Write tower data into Delta format and see the output file structure
# MAGIC 4. Read the usage data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.
# MAGIC 6. Compare the parquet location and delta location and try to understand what is the differentiating factor, as both are parquet files only.

# COMMAND ----------

# MAGIC %md
# MAGIC ##11. Write Operations (Lakehouse Usecases) – Delta table Usecases
# MAGIC 1. Write customer data using saveAsTable() as a managed table
# MAGIC 2. Write usage data using saveAsTable() with overwrite mode
# MAGIC 3. Drop the managed table and verify data removal
# MAGIC 4. Go and check the table overview and realize it is in delta format in the Catalog.
# MAGIC 5. Use spark.read.sql to write some simple queries on the above tables created.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##12. Write Operations (Lakehouse Usecases) – Delta table Usecases
# MAGIC 1. Write customer data using insertInto() in a new table and find the behavior
# MAGIC 2. Write usage data using insertTable() with overwrite mode

# COMMAND ----------

# MAGIC %md
# MAGIC ##13. Write Operations (Lakehouse Usecases) – Delta table Usecases
# MAGIC 1. Write customer data into XML format using rowTag as cust
# MAGIC 2. Write usage data into XML format using overwrite mode with the rowTag as usage
# MAGIC 3. Download the xml data and open the file in notepad++ and see how the xml file looks like.

# COMMAND ----------

# MAGIC %md
# MAGIC ##14. Compare all the downloaded files (csv, json, orc, parquet, delta and xml) 
# MAGIC 1. Capture the size occupied between all of these file formats and list the formats below based on the order of size from small to big.

# COMMAND ----------

# MAGIC %md
# MAGIC ###15. Try to do permutation and combination of performing Schema Migration & Data Conversion operations like...
# MAGIC 1. Read any one of the above orc data in a dataframe and write it to dbfs in a parquet format
# MAGIC 2. Read any one of the above parquet data in a dataframe and write it to dbfs in a delta format
# MAGIC 3. Read any one of the above delta data in a dataframe and write it to dbfs in a xml format
# MAGIC 4. Read any one of the above delta table in a dataframe and write it to dbfs in a json format
# MAGIC 5. Read any one of the above delta table in a dataframe and write it to another table

# COMMAND ----------

# MAGIC %md
# MAGIC ##16. Do a final exercise of defining one/two liner of... 
# MAGIC 1. When to use/benifits csv
# MAGIC 2. When to use/benifits json
# MAGIC 3. When to use/benifit orc
# MAGIC 4. When to use/benifit parquet
# MAGIC 5. When to use/benifit delta
# MAGIC 6. When to use/benifit xml
# MAGIC 7. When to use/benifit delta tables
# MAGIC