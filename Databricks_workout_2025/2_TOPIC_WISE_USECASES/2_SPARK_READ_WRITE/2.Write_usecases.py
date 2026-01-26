# Databricks notebook source
# MAGIC %md
# MAGIC ## Spark Write Operations using 
# MAGIC - csv, json, orc, parquet, delta, saveAsTable, insertInto, xml with different write mode, header and sep options

# COMMAND ----------

# MAGIC %md
# MAGIC ##1. Write Operations (Data Conversion/Schema migration) – CSV Format Usecases
# MAGIC 1. Write customer data into CSV format using overwrite mode
# MAGIC 2. Write usage data into CSV format using append mode
# MAGIC 3. Write tower data into CSV format with header enabled and custom separator (|)
# MAGIC 4. Read the tower data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Write Operations (Data Conversion/Schema migration)– JSON Format Usecases
# MAGIC 1. Write customer data into JSON format using overwrite mode
# MAGIC 2. Write usage data into JSON format using append mode and snappy compression format
# MAGIC 3. Write tower data into JSON format using ignore mode and observe the behavior of this mode
# MAGIC 4. Read the tower data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Write Operations (Data Conversion/Schema migration) – Parquet Format Usecases
# MAGIC 1. Write customer data into Parquet format using overwrite mode and in a gzip format
# MAGIC 2. Write usage data into Parquet format using error mode
# MAGIC 3. Write tower data into Parquet format with gzip compression option
# MAGIC 4. Read the usage data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Write Operations (Data Conversion/Schema migration) – Orc Format Usecases
# MAGIC 1. Write customer data into ORC format using overwrite mode
# MAGIC 2. Write usage data into ORC format using append mode
# MAGIC 3. Write tower data into ORC format and see the output file structure
# MAGIC 4. Read the usage data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

# MAGIC %md
# MAGIC ##5. Write Operations (Data Conversion/Schema migration) – Delta Format Usecases
# MAGIC 1. Write customer data into Delta format using overwrite mode
# MAGIC 2. Write usage data into Delta format using append mode
# MAGIC 3. Write tower data into Delta format and see the output file structure
# MAGIC 4. Read the usage data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.
# MAGIC 6. Compare the parquet location and delta location and try to understand what is the differentiating factor, as both are parquet files only.

# COMMAND ----------

# MAGIC %md
# MAGIC ##6. Write Operations (Lakehouse Usecases) – Delta table Usecases
# MAGIC 1. Write customer data using saveAsTable() as a managed table
# MAGIC 2. Write usage data using saveAsTable() with overwrite mode
# MAGIC 3. Drop the managed table and verify data removal
# MAGIC 4. Go and check the table overview and realize it is in delta format in the Catalog.
# MAGIC 5. Use spark.read.sql to write some simple queries on the above tables created.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##7. Write Operations (Lakehouse Usecases) – Delta table Usecases
# MAGIC 1. Write customer data using insertInto() in a new table and find the behavior
# MAGIC 2. Write usage data using insertTable() with overwrite mode

# COMMAND ----------

# MAGIC %md
# MAGIC ##8. Write Operations (Lakehouse Usecases) – Delta table Usecases
# MAGIC 1. Write customer data into XML format using rowTag as cust
# MAGIC 2. Write usage data into XML format using overwrite mode with the rowTag as usage
# MAGIC 3. Download the xml data and open the file in notepad++ and see how the xml file looks like.

# COMMAND ----------

# MAGIC %md
# MAGIC ##9. Compare all the downloaded files (csv, json, orc, parquet, delta and xml) 
# MAGIC 1. Capture the size occupied between all of these file formats and list the formats below based on the order of size from small to big.

# COMMAND ----------

# MAGIC %md
# MAGIC ##10. Do a final exercise of defining one/two liner of... 
# MAGIC 1. When to use/benifits csv
# MAGIC 2. When to use/benifits json
# MAGIC 3. When to use/benifit orc
# MAGIC 4. When to use/benifit parquet
# MAGIC 5. When to use/benifit delta
# MAGIC 6. When to use/benifit xml
# MAGIC 7. When to use/benifit delta tables
# MAGIC