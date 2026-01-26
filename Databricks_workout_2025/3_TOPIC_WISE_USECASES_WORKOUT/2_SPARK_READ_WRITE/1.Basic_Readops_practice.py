# Databricks notebook source
# MAGIC %md
# MAGIC # 1.Creating a SCHEMA(DATABASE),VOLUME in Catalog
# MAGIC

# COMMAND ----------

# MAGIC %sql 
# MAGIC CREATE CATALOG IF NOT EXISTS soundhar_catalog;
# MAGIC CREATE SCHEMA IF NOT EXISTS soundhar_catalog.soundhar_schema;
# MAGIC CREATE VOLUME soundhar_catalog.soundhar_schema.soundhar_volume;

# COMMAND ----------

dbutils.fs.mkdirs ("/Volumes/soundhar_catalog/soundhar_schema/soundhar_volume/Volumes")

# COMMAND ----------

'''How to read the data using pyspark sql'''

# Navigate to the pyspark main package → then to the sql sub-package → then to the session module → and import the SparkSession class.
from pyspark.sql.session import SparkSession # We import the SparkSession class from the pyspark.sql.session module.

spark1 = SparkSession.builder.getOrCreate() # We instantiate (create) the SparkSession class using the builder.getOrCreate() method.
# The created SparkSession instance is stored in the 'spark' object
print(spark1)  # We manually instantiated the sparksession
print(spark)  # already instantiated the sparksession by databricks and we no need instantiate it. But both are stored in same memory 0xff2dda429eb0

# COMMAND ----------

# MAGIC %md
# MAGIC ####How to Read/Extract the data from the filesytem and load it into the distributed memory for further processing/load - using diffent methodologies/options from different sources(fs & db) and different builtin formats (csv/json/orc/parquet/delta/tables)

# COMMAND ----------

# If I don't use any options in this csv function, what is that default functionality?
# 1. By default it will consider ',' as a delimeter in csv file data. if we have '~' as a delimeter in csv file, we have to specify that in options.
# 2. By default it will use _c0,_c1,_c2,..._cn it will apply as column headers ( header=True or toDF("col1","col2","col3") or we have more options to see further)
# 3. By default it will treat all columns as string datatype. ( inferschema = True or we have more options to see further)

csv_df1 = spark.read.csv("dbfs:///Volumes/workspace/default/mobile_metrics/mobile_os_usage.csv") # it will read and show the data which is stored in the file.
csv_df1.show()   # if we use show, it will display only 20 rows by default and it will product the output in a dataframe format
display(csv_df1) # if we use display, it will display only 15 rows by default and it will product the output in a table format.

print(csv_df1.printSchema())   # if we use printschema, it will describe the table structure.
#or
display(csv_df1.printSchema())


# COMMAND ----------

# 1.Header concepts (Either we have define the column names or we have to use the column names from the data)
# if the file consist of column names in first row, we can use the first row as headers (Column names) by using Header = True
csv_df1 = spark.read.csv("dbfs:///Volumes/workspace/default/mobile_metrics/mobile_os_usage.csv",header=True)
display(csv_df1)

# if the file does not consist of column names in first row, we can define the column names by using toDF("col1","col2","col3")
csv_df2 = spark.read.csv("dbfs:///Volumes/workspace/default/mobile_metrics/mobile_os_usage_1_without_header.csv").show()

csv_df2 = spark.read.csv("dbfs:///Volumes/workspace/default/mobile_metrics/mobile_os_usage_1_without_header.csv").toDF("Date","Mobile_Operating_System","Percent_of_Usage")
display(csv_df2)



# COMMAND ----------

#2. Printing Schemav (equivalent to describe table)
csv_df1.printSchema()
csv_df2.printSchema()


# COMMAND ----------

# 3. InferSchema (By default it will consider all columns as string datatype) 
# ( performance consideration : Use this function causiously because it scans the entire data by immediately evaluating and executing the plan)
# Hence, not good for large data or not good to use on the predefined schema dataset.
# Basically Inferschema scans all the columns row by row and it will try to find the best data type for each column. It is constly process. Because we have to pay the cost of scanning the entire data.

csv_df1 = spark.read.csv("dbfs:///Volumes/workspace/default/mobile_metrics/mobile_os_usage_1_without_header.csv", inferSchema=True).toDF("Date","Mobile_Operating_System","Percent_of_Usage")
csv_df1.printSchema()


# COMMAND ----------

# 4. Delimeter (By default it will consider ',' as a delimeter in csv file data. 
# if we have '~' as a delimeter in csv file and automatically it consider as one single column instead of multiple columns, we have to specify that in options.

csv_df1 = spark.read.csv("dbfs:///Volumes/workspace/default/mobile_metrics/mobile_os_usage_without_header_delimiter~.csv", inferSchema=True,sep='~').toDF("Date","Mobile_Operating_System","Percent_of_Usage")
display(csv_df1)




# COMMAND ----------

# 5. Using different options to create dataframe with csv and other module...( 2 methodologies with 3 ways of creating dataframes)
csv_df1 = spark.read.csv("dbfs:///Volumes/workspace/default/mobile_metrics/mobile_os_usage_without_header_delimiter~.csv", header=True, inferSchema=True,sep= '~').toDF("Date","Mobile_Operating_System","Percent_of_Usage")
csv_df1.show(2)

# or another way of creating dataframe 9 from any sources whether builin or external)..
# option can be used for 1 or option..
csv_df2 = spark.read.option("header","true").option("inferSchema","true").option("sep","~").format("csv").load("dbfs:///Volumes/workspace/default/mobile_metrics/mobile_os_usage_without_header_delimiter~.csv").toDF("Date","Mobile_Operating_System","Percent_of_Usage")
csv_df2.show(2)

# or options can be used for multiple options..
csv_df3 = spark.read.options(header="true",inferSchema="true",sep="~").format("csv").load("dbfs:///Volumes/workspace/default/mobile_metrics/mobile_os_usage_without_header_delimiter~.csv").toDF("Date","Mobile_Operating_System","Percent_of_Usage")
csv_df3.show(2)


# COMMAND ----------

# MAGIC %md
# MAGIC ## Generic way of read and load data into dataframe using fundamental options from built in sources (csv/orc/parquet/xml/json/table) (inferschema, header, sep)

# COMMAND ----------

csv_df1 = spark.read.csv("dbfs:///Volumes/workspace/default/mobile_metrics/mobile_os_usage_without_header_delimiter~.csv", header=True, inferSchema=True,sep= '~').toDF("Date","Mobile_Operating_System","Percent_of_Usage")
csv_df1.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Generic way of read and load data into dataframe using extended options from external sources (bigquery/redshift/athena/synapse) (tmpfolder, access controls)

# COMMAND ----------

# Options can be used for multiple options in one function as a parameter...

csv_df3 = spark.read.options(header="true",inferSchema="true",sep="~").format("csv").load("dbfs:///Volumes/workspace/default/mobile_metrics/mobile_os_usage_without_header_delimiter~.csv").toDF("Date","Mobile_Operating_System","Percent_of_Usage")
csv_df3.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Reading data from multiple files

# COMMAND ----------

# if we have file name in same format,We can use wild card of file names like mention mobile_* intead of mentioning the full file name.
csv_df1 = spark.read.csv("dbfs:///Volumes/workspace/default/mobile_metrics/mobile_os_usage_without_*", header=True, inferSchema=True,sep= '~').toDF("Date","Mobile_Operating_System","Percent_of_Usage")
print(csv_df1.count())


# COMMAND ----------

# if we have different file names in same folder. We can mention the paths and read the data
csv_df1 = spark.read.csv(path=["dbfs:///Volumes/workspace/default/mobile_metrics/mobile_os_usage_without_header_delimiter~*","/Volumes/workspace/default/mobile_metrics/mobile_os_usage_1_without_header.csv"], header=True, inferSchema=True,sep= '~').toDF("Date","Mobile_Operating_System","Percent_of_Usage")
print(csv_df1.count())