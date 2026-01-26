# Databricks notebook source
# MAGIC %md
# MAGIC #Telecom Domain ReadOps Assignment
# MAGIC This notebook contains assignments to practice Spark read options and Databricks volumes. <br>
# MAGIC Sections: Sample data creation, Catalog & Volume creation, Copying data into Volumes, Path glob/recursive reads, toDF() column renaming variants, inferSchema/header/separator experiments, and exercises.<br>

# COMMAND ----------

# MAGIC %md
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

# MAGIC %sql
# MAGIC
# MAGIC CREATE CATALOG IF NOT EXISTS telecom_catalog_assign;
# MAGIC CREATE SCHEMA IF NOT EXISTS telecom_catalog_assign.landing_zone;
# MAGIC CREATE VOLUME telecom_catalog_assign.landing_zone.landing_vol;
# MAGIC

# COMMAND ----------

dbutils.fs.mkdirs('/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer')
dbutils.fs.mkdirs('/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage')
dbutils.fs.mkdirs('/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region1/')
dbutils.fs.mkdirs('/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region2/')


# COMMAND ----------

# We can create multiple folders in one go by using a loop, instead of writing dbutils.fs.mkdirs() one by one.

folders = ['/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer','/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage','/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region1 ','/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region2']

for folder in folders:
  dbutils.fs.mkdirs(folder)

# COMMAND ----------

# MAGIC %md
# MAGIC **1.5 Explain the difference between (Just google and understand why we are going for volume concept for prod ready systems):**
# MAGIC
# MAGIC **1. Volume Vs DBFS/filestore**
# MAGIC
# MAGIC **DBFS/FileStore** is a legacy, workspace-level storage mainly used for temporary files, testing, or demos. It does not provide governance, fine-grained access control, or auditing, so it is **not recommended for production use**.
# MAGIC
# MAGIC **Databricks Volumes**, on the other hand, are a **Unity Catalog–governed storage layer** used to store files securely in cloud storage. Volumes support **fine-grained permissions, auditing, and external locations**, making them production-ready and enterprise-grade. 
# MAGIC
# MAGIC **2. Why production teams prefer Volumes for regulated data?**
# MAGIC
# MAGIC Production teams prefer **Volumes** because they provide **better security, control, and tracking** for sensitive or regulated data.
# MAGIC - **Only authorized people can access the data** (user/group permissions)
# MAGIC - **Every access is tracked and audited**
# MAGIC -** Data is stored securely in cloud storage**
# MAGIC - **Access rules are centrally managed using Unity Catalog**
# MAGIC
# MAGIC Because of this, Volumes help organizations **follow compliance rules** (like GDPR, HIPAA, etc.) and **avoid data misuse or leaks**.

# COMMAND ----------

# MAGIC %md
# MAGIC ##2.Data files to use in this usecase:
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

customer_csv = '''
101,Arun,31,Chennai,PREPAID
102,Meera,45,Bangalore,POSTPAID
103,Irfan,29,Hyderabad,PREPAID
104,Raj,52,Mumbai,POSTPAID
105,,27,Delhi,PREPAID
106,Sneha,abc,Pune,PREPAID
'''
usage_tsv = '''customer_id\tvoice_mins\tdata_mb\tsms_count
101\t320\t1500\t20
102\t120\t4000\t5
103\t540\t600\t52
104\t45\t200\t2
105\t0\t0\t0
'''
tower_logs_region1 = '''event_id|customer_id|tower_id|signal_strength|timestamp
5001|101|TWR01|-80|2025-01-10 10:21:54
5004|104|TWR05|-75|2025-01-10 11:01:12
'''



# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Filesystem operations
# MAGIC 1. Write code to copy the above datasets into your created Volume folders:
# MAGIC Customer → /Volumes/.../customer/
# MAGIC Usage → /Volumes/.../usage/
# MAGIC Tower (region-based) → /Volumes/.../tower/region1/ and /Volumes/.../tower/region2/
# MAGIC
# MAGIC 2. Write a command to validate whether files were successfully copied

# COMMAND ----------

# 3.1. Write code to copy the above datasets into your created Volume folders:
'''Customer → /Volumes/.../customer/
Usage → /Volumes/.../usage/
Tower (region-based) → /Volumes/.../tower/region1/ and /Volumes/.../tower/region2/'''

dbutils.fs.put('/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv', customer_csv, True)
dbutils.fs.put('/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv', usage_tsv, True)
dbutils.fs.put('/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region1/tower_logs_region1.csv',tower_logs_region1,True)
dbutils.fs.put('/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region2/tower_logs_region1.csv',tower_logs_region1,True)



# COMMAND ----------

# 3.2. Write a command to validate whether files were successfully copied

print(dbutils.fs.ls('/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/'))
print(dbutils.fs.ls('/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/'))
print(dbutils.fs.ls('/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region1'))
print(dbutils.fs.ls('/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region2'))

# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Directory Read Use Cases
# MAGIC 1. Read all tower logs using: <br>
# MAGIC Path glob filter (example: *.csv) <br>
# MAGIC Multiple paths input <br>
# MAGIC Recursive lookup <br>
# MAGIC
# MAGIC 2. Demonstrate these 3 reads separately:<br>
# MAGIC Using pathGlobFilter<br>
# MAGIC Using list of paths in spark.read.csv([path1, path2])<br>
# MAGIC Using .option("recursiveFileLookup","true")<br>
# MAGIC
# MAGIC 3. Compare the outputs and understand when each should be used.

# COMMAND ----------

# 4.1. Read all tower logs using Path glob filter (example: *.csv),Multiple paths input,Recursive lookup:
tower_df1 = spark.read.csv([
  "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region1",
  "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region2"], 
  header=True, 
  inferSchema=True, 
  sep="|", 
  pathGlobFilter="*.csv", 
  recursiveFileLookup=True
  )
display(tower_df1)

# 4.2. Demonstrate these 3 reads separately:

# 4.2.1. Using pathGlobFilter only
tower_df2 = spark.read.csv(
  "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region*",  # We can use a wild card (*) to load data from all the files or folders that match a path pattern.
  header=True, 
  inferSchema=True, 
  sep="|", 
  pathGlobFilter="*.csv"  # We are using pathGlobFilter is to filter out only the csv files from the folder and also we can load multiple files at the same time.
  )
display(tower_df2)

# 4.2.2. Using list of paths in spark.read.csv([path1, path2])
tower_df3 = spark.read.csv([
  "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region1/","/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region2/"],
  header=True, 
  inferSchema=True, 
  sep="|", 
  pathGlobFilter="*.csv"
  )                     # We can mention the different paths in the list of csv and we can load the data from different folders
display(tower_df3)

# 4.2.3. .option("recursiveFileLookup", "true") with multiple paths
tower_df4 = spark.read.option(
    "header","True").option(
      "inferSchema","True").option(
        "sep","|").option(
          "pathGlobFilter","*.csv").option(
            "recursiveFileLookup","true").csv(
              "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/")
       # if we mention main folder name in the path and recursiveFileLookup=True, it will load all the data from the subfolders
display(tower_df4)   



# COMMAND ----------

# MAGIC %md
# MAGIC ##5. Schema Inference, Header, and Separator
# MAGIC 1. Try the Customer, Usage files with the option and options using read.csv and format function:<br>
# MAGIC header=false, inferSchema=false<br>
# MAGIC or<br>
# MAGIC header=true, inferSchema=true<br>
# MAGIC 2. Write a note on What changed when we use header or inferSchema  with true/false?<br>
# MAGIC 3. How schema inference handled “abc” in age?<br>

# COMMAND ----------

# 5.1. Try the Customer, Usage files with the option and options using read.csv and format function:

# 5.1.1. header=false, inferSchema=false [Using Customer file]

#Reading the file using combination of option with format function
customer_df1 = spark.read.option(
    "header","false").option(
      "inferSchema","false").format('csv').load(
                  "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv")
display(customer_df1)

#Reading the file using combination of options with format function
customer_df2 = spark.read.options(
    header="false",inferSchema="false").format('csv').load(
                  "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv")
display(customer_df2)

# Reading the file using combination of option with read.csv function
customer_df3 = spark.read.option(
    "header","false").option(
      "inferSchema","false").csv(
                  "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv")
display(customer_df2)

#Reading the file using combination of options with read.csv function
customer_df4 = spark.read.options(
    header="true",inferSchema="true").csv(
                  "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv")
display(customer_df4)


# 5.1.2. header=true, inferSchema=true [Using Usage file]

#Reading the file using combination of option with format function
usage_df1 = spark.read.option(
    "header","True").option(
      "inferSchema","True").option(
        "sep","\t").format('csv').load(
                  "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv")
display(usage_df1)

#Reading the file using combination of options with format function
usage_df2 = spark.read.options(
    header="True",inferSchema="True",sep="\t").format('csv').load(
                  "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv")
display(usage_df2)

# Reading the file using combination of option with read.csv function 
usage_df3 = spark.read.option(
    "header","True").option(
      "inferSchema","True").option(
        "sep","\t").csv(
                  "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv")
display(usage_df3)

#Reading the file using combination of options with read.csv function
usage_df4 = spark.read.options(
    header="True",inferSchema="True",sep="\t").csv(
                  "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv")
display(usage_df4)



# COMMAND ----------

# MAGIC %md
# MAGIC **5.2. Write a note on What changed when we use header or inferSchema  with true/false?**
# MAGIC
# MAGIC 1.**header = True** ==> If the first row of the file contains column names, setting header = True tells Spark to treat the first row as the header and the remaining rows as data.
# MAGIC
# MAGIC 2.**header = False** ==> If the file does not contain column names in the first row, setting header = False makes Spark automatically assign default column names such as c0, c1, c2, and so on.
# MAGIC
# MAGIC 3.**inferSchema = True** ==> If we want Spark to automatically detect data types based on the column values, we can set inferSchema = True. Spark scans the data and assigns data types accordingly. This should be used cautiously because it requires scanning the data, which can impact performance.
# MAGIC
# MAGIC 4.**inferSchema = False** ==> If inferSchema is set to False, Spark treats all column values as string data types by default.

# COMMAND ----------

# MAGIC %md
# MAGIC **5.3. How schema inference handled “abc” in age?** <b>
# MAGIC
# MAGIC The age column should always contain numeric values. If even one value like 'abc' appears in the age column, Spark will infer the entire column as StringType, because it cannot safely assign an IntegerType.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ##6. Column Renaming Usecases
# MAGIC 1. Apply column names using string using toDF function for customer data
# MAGIC 2. Apply column names and datatype using the schema function for usage data
# MAGIC 3. Apply column names and datatype using the StructType with IntegerType, StringType, TimestampType and other classes for towers data 

# COMMAND ----------

# 6.1. Apply column names using string using toDF function for customer data

customer_df1 = spark.read.options(
    header="false",
    inferSchema="false"
    ).format('csv').load("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv").toDF("customer_id","first_name","last_name","city","plan_type")
display(customer_df1)

# 6.2. Apply column names and datatype using the schema function for usage data

data_type= "customer_id int,voice_mins int,data_mb int,sms_count int"
usage_df1 = spark.read.schema(data_type).options(
  header="true",
  sep="\t"
  ).csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv").toDF("customer_id","voice_mins","data_mb","sms_count")
display(usage_df1)

# 6.3. Apply column names and datatype using the StructType with IntegerType, StringType, TimestampType and other classes for towers data

from pyspark.sql.types import StructType,StructField,IntegerType,StringType,TimestampType

schema = StructType([
  StructField("event_id",IntegerType(),True),
  StructField("customer_id",IntegerType(),True),
  StructField("tower_id",StringType(),True),
  StructField("signal_strength",IntegerType(),True),
  StructField("timestamp",TimestampType(),True)
])
tower_df1 = spark.read.schema(schema).options(
  header="true",sep="|",pathGlobFilter="*.csv", recursiveFileLookup="true").format('csv').load("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region*")
display(tower_df1)

    

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. More to come (stay motivated)....