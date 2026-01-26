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

# Reading the CSV file and storing it in a datframe 

from pyspark.sql.types import StructType,StructField, StringType, IntegerType

custom_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_age", IntegerType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_plan_type", StringType(), True)])

read_customer_df = spark.read.schema(custom_schema).csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv")

# 1.Write customer data into CSV format using overwrite mode
write_customer_csv_df = read_customer_df.write.options(header='true').mode("overwrite").csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/csvout/")
display(write_customer_csv_df)

# 2.Write usage data into CSV format using append mode
write_customer_csv_df = read_customer_df.write.options(header='true').mode("append").csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/csvout/")
display(write_customer_csv_df)

# 3.Write tower data into CSV format with header enabled and custom separator (|)
read_tower_df = spark.read.options(header='true',sep='|',inferSchema='true',pathGlobeFilter='.csv',recursiveFileLookup='true').format('csv').load("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region*")
display(read_tower_df)

write_tower_csv_df = read_tower_df.write.options(header='true',sep='|').mode("overwrite").csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/csvout/")
display(write_tower_csv_df)

# 4.Read the tower data in a dataframe and show only 5 rows.
display(read_tower_df.limit(5))

# 5.Download the file into local from the catalog volume location and see the data of any of the above files opening in a notepad++.
'''Yes, I could download the file into local from the catalog volume location and see the data of above files opening in a notepad++.'''


# COMMAND ----------

# MAGIC %md
# MAGIC ##2. Write Operations (Data Conversion/Schema migration)– JSON Format Usecases
# MAGIC 1. Write customer data into JSON format using overwrite mode
# MAGIC 2. Write usage data into JSON format using append mode and snappy compression format
# MAGIC 3. Write tower data into JSON format using ignore mode and observe the behavior of this mode
# MAGIC 4. Read the tower data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

#1.Write customer data into JSON format using overwrite mode
write_customer_json_df = read_customer_df.write.mode("overwrite").json("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/jsonout/")

#2.Write usage data into JSON format using append mode and snappy compression format
read_usage_csv_df = spark.read.options(header= 'true',inferSchema="True",sep ='\t').csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv")
display(read_usage_csv_df)

write_usage_json_df = read_usage_csv_df.write.mode("append").option("compression","snappy").json("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/jsonout/")

#3.Write tower data into JSON format using ignore mode and observe the behavior of this mode
write_tower_json_df = read_tower_df.write.mode("ignore").json("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/jsonout/")

#4.Read the tower data in a dataframe and show only 5 rows
display(read_tower_df.limit(5))

#5.Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.
'''Yes, I was able to download the files locally from the catalog volume location and view the data of all three files using Notepad++. 
But, out of 3 files, only two were in readable JSON format. The 'usage' file was compressed, so I was unable to view the data in a clear format.'''


# COMMAND ----------

# MAGIC %md
# MAGIC ##3. Write Operations (Data Conversion/Schema migration) – Parquet Format Usecases
# MAGIC 1. Write customer data into Parquet format using overwrite mode and in a gzip format
# MAGIC 2. Write usage data into Parquet format using error mode
# MAGIC 3. Write tower data into Parquet format with gzip compression option
# MAGIC 4. Read the usage data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

#1.Write customer data into Parquet format using overwrite mode and in a gzip format
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

custom_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_age", IntegerType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_plan_type", StringType(), True)])

read_customer_df = spark.read.schema(custom_schema).csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv")
display(read_customer_df)

write_customer_parquet_df = read_customer_df.write.mode("overwrite").option("compression","gzip").parquet("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/parquetout/")

#2.Write usage data into Parquet format using error mode
read_usage_csv_df = spark.read.options(header= 'true',inferSchema="True",sep ='\t').csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv")
display(read_usage_csv_df)

write_usage_parquet_df = read_usage_csv_df.write.mode("error").parquet("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/parquetout/")

#3.Write tower data into Parquet format with gzip compression option
read_tower_df = spark.read.options(header='true',sep='|',inferSchema='true',pathGlobeFilter='.csv',recursiveFileLookup='true').format('csv').load("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region*")
display(read_tower_df)

write_tower_parquet_df = read_tower_df.write.mode("overwrite").option("compression","gzip").parquet("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/parquetout/")

#4.Read the usage data in a dataframe and show only 5 rows.

display(read_usage_csv_df.limit(5))

#5.Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.
'''Yes, I was able to download the file to my local machine from the catalog volume location, but I couldn’t view the data because it is compressed and stored in parquet format.'''


# COMMAND ----------

# MAGIC %md
# MAGIC ##4. Write Operations (Data Conversion/Schema migration) – Orc Format Usecases
# MAGIC 1. Write customer data into ORC format using overwrite mode
# MAGIC 2. Write usage data into ORC format using append mode
# MAGIC 3. Write tower data into ORC format and see the output file structure
# MAGIC 4. Read the usage data in a dataframe and show only 5 rows.
# MAGIC 5. Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.

# COMMAND ----------

# 1.Write customer data into ORC format using overwrite mode
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

custom_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_age", IntegerType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_plan_type", StringType(), True)])

read_customer_df = spark.read.schema(custom_schema).csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv")
display(read_customer_df)

write_customer_orc_df = read_customer_df.write.mode("overwrite").format('orc').save("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/orcout/")

#2.Write usage data into ORC format using append mode
read_usage_csv_df = spark.read.options(header= 'true',inferSchema="True",sep ='\t').csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv")
display(read_usage_csv_df)

write_usage_orc_df = read_usage_csv_df.write.mode("append").format('orc').save("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/orcout/")

#3.Write tower data into ORC format and see the output file structure
read_tower_csv_df = spark.read.options(header='true',sep='|',inferSchema='true',pathGlobeFilter='.csv',recursiveFileLookup='true').format('csv').load("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region*")
display(read_tower_df)

write_tower_orc_df = read_tower_csv_df.write.mode("overwrite").orc("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/orcout/")

#4.Read the usage data in a dataframe and show only 5 row
display(read_usage_csv_df.limit(5))

#5.Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++
'''Yes, I was able to download the file to my local machine from the catalog volume location, but I couldn’t view the data because it is compressed and stored in ORC format.'''


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

#1.Write customer data into Delta format using overwrite mode
from pyspark.sql.types import StructType,StructField, StringType, IntegerType

custom_schema = StructType([
    StructField("customer_id", IntegerType(), True),
    StructField("customer_name", StringType(), True),
    StructField("customer_age", IntegerType(), True),
    StructField("customer_city", StringType(), True),
    StructField("customer_plan_type", StringType(), True)])

read_customer_df = spark.read.schema(custom_schema).csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv")
display(read_customer_df)

write_customer_delta_df = read_customer_df.write.mode("overwrite").format('delta').save("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/deltaout/")


#2.Write usage data into Delta format using append mode
read_usage_csv_df = spark.read.options(header= 'true',inferSchema="True",sep ='\t').csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv")
display(read_usage_csv_df)

write_usage_delta_df = read_usage_csv_df.write.mode("append").format('delta').save("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/deltaout/")

#3.Write tower data into Delta format and see the output file structure
read_tower_csv_df = spark.read.options(header='true',sep='|',inferSchema='true',pathGlobeFilter='.csv',recursiveFileLookup='true').format('csv').load("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/region*")
display(read_tower_df)

write_tower_delta_df = read_tower_csv_df.write.mode("overwrite").option("compression","gzip").format('delta').save("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/tower/deltaout/")

#4.Read the usage data in a dataframe and show only 5 rows.
display(read_usage_csv_df.limit(5))

#5.Download the file into local harddisk from the catalog volume location and see the data of any of the above files opening in a notepad++.
'''I downloaded all the files into local machine from the catolog volume location and I was unable to read the data because it is compressed and stored in delta format (Internally as Parquet format).
But I could see the transaction logs in the delta, which are not available in ORC and parquet formats.'''

#6.Compare the parquet location and delta location and try to understand what is the differentiating factor, as both are parquet files only.
'''The main difference is that the transcation logs are stored in delta format and not in parquet format.
The delta format is stored as a parquet format behind the scenes.
The delta format is a file format that is optimized for data lakes and is designed to provide efficient
We can do the ACID (DML) and Write-many-read-many WMRM activities in delta formats
we can't do the above activites in ORC or parquet file formats.We can perform only write-once-read-many WORM activities in ORC'''


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

# 1.Write customer data using saveAsTable() as a managed table

read_customer_df= spark.read.csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv").toDF("customer_id","customer_name","customer_age","customer_city","customer_plan_type")

write_customer_df = read_customer_df.write.mode('overwrite').format('delta').saveAsTable("telecom_catalog_assign.landing_zone.customer_table")

# 2.Write usage data using saveAsTable()

read_usage_df = spark.read.csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv",header=True,inferSchema=True,sep='\t')

write_usage_df = read_usage_df.write.format('delta').mode('overwrite').saveAsTable("telecom_catalog_assign.landing_zone.usage_table")

# 3.Drop the managed table and verify data removal
# %sql
# DROP TABLE telecom_catalog_assign.landing_zone.customer_table;

# 4.Go and check the table overview and realize it is in delta format in the Catalog.
"1. I've verified, the table has stored as a delta format by default."

# 5.Use spark.sql to write some simple queries on the above tables created.
write_sql= spark.sql("select * from telecom_catalog_assign.landing_zone.usage_table")
display(write_sql)



# COMMAND ----------

# MAGIC %md
# MAGIC ##7. Write Operations (Lakehouse Usecases) – Delta table Usecases
# MAGIC 1. Write customer data using insertInto() in a new table and find the behavior
# MAGIC 2. Write usage data using insertTable() with append mode

# COMMAND ----------

# 1. Write customer data using insertInto() in a new table and find the behavior

# Read the customer data
read_customer_df = spark.read.csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv").toDF("customer_id","customer_name","customer_age","customer_city","customer_plan_type")

# Insert into existing table ( it won't write the data into a new target table, because it is doesn't exist in the catalog/schema.It will load the data only if the table is exist)
# write_customer_df = read_customer_df.write.insertInto("telecom_catalog_assign.landing_zone.cust_table",overwrite= True)  
write_customer_df = read_customer_df.write.insertInto("telecom_catalog_assign.landing_zone.customer_table",overwrite= False)  

write_sql= spark.sql("select * from telecom_catalog_assign.landing_zone.customer_table")
display(write_sql)

# 2. Write usage data using insertInto() with append mode
 
# Read the usage data
read_usage_data = spark.read.csv(
    "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv",
    header=True,
    inferSchema=True,
    sep='\t')

# Insert into existing table
write_usage_data = read_usage_data.write.mode('append').insertInto("telecom_catalog_assign.landing_zone.usage_table")

display(spark.sql("select * from telecom_catalog_assign.landing_zone.usage_table"))









# COMMAND ----------

# MAGIC %md
# MAGIC ##8. Write Operations (Lakehouse Usecases) – Delta table Usecases
# MAGIC 1. Write customer data into XML format using rowTag as cust
# MAGIC 2. Write usage data into XML format using overwrite mode with the rowTag as usage
# MAGIC 3. Download the xml data and open the file in notepad++ and see how the xml file looks like.

# COMMAND ----------

# 1. Write customer data into XML format using rowTag as cust

# Read the customer data
read_customer_df = spark.read.csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customer.csv").toDF("customer_id","customer_name","customer_age","customer_city","customer_plan_type")

# Write the data into xml format

write_xml_format = read_customer_df.write.format("xml").mode("overwrite").options(rowTag = 'cust').save("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/xmlout/")

# 2.Write usage data into XML format using overwrite mode with the rowTag as usage

# Read the usage data
read_usage_data = spark.read.csv(
    "/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/usage.csv",
    header=True,
    inferSchema=True,
    sep='\t')

# Write the data into xml format
write_usage_data = read_usage_data.write.mode('overwrite').options(rowTag = 'usage').format('xml').save("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/usage/xmlout/")

# 3. Download the xml data and open the file in notepad++ and see how the xml file looks like.

"""I have downloaded the both files and verified in notepad++, it has stored as a xml format with rowtag"""



# COMMAND ----------

# MAGIC %md
# MAGIC ##9. Compare all the downloaded files (csv, json, orc, parquet, delta and xml) 
# MAGIC 1. Capture the size occupied between all of these file formats and list the formats below based on the order of size from small to big.

# COMMAND ----------

# I've just downloaded a 16mb of csv file and write it as all formats to compare the file sizes. Because we cannot compare and understand the file size difference for small data.

read_df = spark.read.csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customers-100000.csv",header=True,inferSchema=True)

write_df = read_df.write.mode('overwrite').format('csv').save("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/cout")
write_df1 = read_df.write.mode('overwrite').format('orc').save("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/oout")
write_df2 = read_df.write.mode('overwrite').format('parquet').save("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/paout")
write_df3 = read_df.write.mode('overwrite').format('json').save("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/jout")

read_df2 = spark.read.csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customers-100000.csv").toDF("Index","Customer_Id", "First_Name","Company","City","Country",
 "Last_Name", "Phone_1", "Phone_2","Email","Subscription_Date","Website")
write_df5 = read_df2.write.mode('overwrite').options(rowTag='cust').format('xml').save("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/xout")

read_df3 = spark.read.csv("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/customers-100000.csv").toDF("Index","Customer_Id", "First_Name","Company","City","Country",
 "Last_Name", "Phone_1", "Phone_2","Email","Subscription_Date","Website")
write_df4 = read_df3.write.mode('overwrite').format('delta').save("/Volumes/telecom_catalog_assign/landing_zone/landing_vol/customer/dout")




# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **1. Capture the size occupied between all of these file formats and list the formats below based on the order of size from small to big.**
# MAGIC
# MAGIC **_Let me compare customers-100000 data file_**
# MAGIC
# MAGIC Raw file    => csv     => size - 16.5 MB   <br>
# MAGIC format file => csv     => size - 16.44 MB <br>
# MAGIC format file => orc     => size - 8.01 MB <br>
# MAGIC format file => delta   => size - 9.62 MB <br>
# MAGIC format file => parquet => size - 10.13 MB <br>
# MAGIC format file => json    => size - 27.09 MB <br>
# MAGIC format file => xml     => size - 51.75 MB 

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

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC **1. CSV – When to use / Benefits**
# MAGIC
# MAGIC Use CSV for **_simple data exchange_** and quick viewing because it is human-readable, lightweight, and supported everywhere, but it is not suitable for large-scale processing.
# MAGIC
# MAGIC **2. JSON – When to use / Benefits**
# MAGIC
# MAGIC Use JSON for **_semi-structured_** data and **_API integrations_** because it supports nested and flexible schemas, making it ideal for event and application data.
# MAGIC
# MAGIC **3. ORC – When to use / Benefit**s
# MAGIC
# MAGIC Use ORC for **_large data_**, read-heavy analytical workloads because it provides **_excellent compression, fast reads, and efficient predicate pushdown._**
# MAGIC
# MAGIC **4. Parquet – When to use / Benefi**ts
# MAGIC
# MAGIC Use Parquet for **_big-data analytics_** across multiple platforms because it is a **_columnar, compressed, and widely supported format_** that improves query performance.
# MAGIC
# MAGIC **5. Delta – When to use / Benefits**
# MAGIC
# MAGIC Use Delta for **_reliable data lake processing_** because it adds **_ACID transactions, schema enforcement, time travel, and scalable incremental loads_** on top of Parquet.
# MAGIC
# MAGIC **6. XML – When to use / Benefits**
# MAGIC
# MAGIC Use XML for **_legacy systems and structured data exchange_** where strict schemas and hierarchical data representation are required.
# MAGIC
# MAGIC **7. Delta Tables – When to use / Benefits**
# MAGIC
# MAGIC Use Delta tables for production-grade lakehouse architectures because they **_ensure data consistency, versioning, and optimized performance_** for large datasets.
# MAGIC