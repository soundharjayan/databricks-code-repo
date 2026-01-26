# Databricks notebook source
# MAGIC %md
# MAGIC #I am becoming an eligible "DATA INGESTION DEVELOPER/ENGINEER"

# COMMAND ----------

# MAGIC %md
# MAGIC #### First Let's understand the basic Catalog + Volume Feature of Databricks (We are learning how to build & use Databricks DATALAKE)
# MAGIC Filesystem Hierarchy of Volume in Databricks (DBFS)?<br>
# MAGIC Catalog -> /OurWorkspace/catalog/schema(database)/volume/folder/data files<br>
# MAGIC Tables Hierarchy of Databricks?<br>
# MAGIC Catalog -> /OurWorkspace/catalog/schema(database)/tables/data(dbfs filesystem/some other filesystems)<br>

# COMMAND ----------

# MAGIC %sql
# MAGIC create catalog if not exists catalog1_dropme;
# MAGIC create database if not exists catalog1_dropme.schema1_dropme;
# MAGIC create volume if not exists catalog1_dropme.schema1_dropme.volume1_dropme;

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/directory_dropme")
#Upload the drive data into this location...

# COMMAND ----------

# MAGIC %md
# MAGIC ####If we need to create schema/volume/folder programatically, follow the below steps

# COMMAND ----------

# MAGIC %md
# MAGIC #Spark SQL<br>
# MAGIC ###1.E(Extract) 
# MAGIC L(Load)<br>
# MAGIC Inbuilt libraries sources/targets & Inbuilt data Formats<br>
# MAGIC 2. Bread & Butter (T(Transformation) A(Analytical))<br>

# COMMAND ----------

dbutils.fs.mkdirs("/Volumes/workspace/wd36schema2/volume1/folder1")

# COMMAND ----------

# MAGIC %md
# MAGIC #Learn How to Create Dataframes from filesystem using different options
# MAGIC Download the data from the below drive url <br>
# MAGIC https://drive.google.com/drive/folders/1Tw7V9eBtUxy0xQMW38z3-bzWI_ewzLm6?usp=drive_link

# COMMAND ----------

# MAGIC %md
# MAGIC ###How Do We Write a Typical Spark Application (Core(Obsolute),SQL(Important),Streaming(Mid level important))
# MAGIC ####Before we Create Dataframe/RDD, what is the prerequisite? We need to create spark session object by instantiating sparksession class (by default databricks did that if you create a notebook)

# COMMAND ----------

from pyspark.sql.session import SparkSession
print(spark)#already instantiated by databricks
spark1=SparkSession.builder.getOrCreate()
print(spark1)#we instantiated

# COMMAND ----------

# MAGIC %md
# MAGIC Create a DBFS volume namely commondata and upload the above data in that volume
# MAGIC What are other FS uri's available? file:///, hdfs:///, dbfs:///, gs:///, s3:///, adls:///, blob:///

# COMMAND ----------

# MAGIC %md
# MAGIC ####How to Read/Extract the data from the filesytem and load it into the distributed memory for further processing/load - using diffent methodologies/options from different sources(fs & db) and different builtin formats (csv/json/orc/parquet/delta/tables)

# COMMAND ----------


#If I don't use any options in this csv function, what is the default functionality?
#1. By default it will consider ',' as a delimiter (sep='~')
#2. By default it will use _c0,_c1..._cn it will apply as column headers (header=True or toDF("","","") or we have more options to see further)
#3. By default it will treat all columns as string (inferSchema=True or we have more options to see further)
csv_df1=spark.read.csv("dbfs:///Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/directory_dropme/custs_1")
#csv_df1.show(2)#display with produce output in a dataframe format
print(csv_df1.printSchema())
display(csv_df1)#display with produce output in a beautified table format, specific to databricks


# COMMAND ----------

# MAGIC %md
# MAGIC csv_df1=spark.read.csv
# MAGIC (path: PathOrPaths, schema: Optional[Union[StructType, str]]=None, sep: Optional[str]=None, encoding: Optional[str]=None, quote: Optional[str]=None, escape: Optional[str]=None, comment: Optional[str]=None, header: Optional[Union[bool, str]]=None, inferSchema: Optional[Union[bool, str]]=None, ignoreLeadingWhiteSpace: Optional[Union[bool, str]]=None, ignoreTrailingWhiteSpace: Optional[Union[bool, str]]=None, nullValue: Optional[str]=None, nanValue: Optional[str]=None, positiveInf: Optional[str]=None, negativeInf: Optional[str]=None, dateFormat: Optional[str]=None, timestampFormat: Optional[str]=None, maxColumns: Optional[Union[int, str]]=None, maxCharsPerColumn: Optional[Union[int, str]]=None, maxMalformedLogPerPartition: Optional[Union[int, str]]=None, mode: Optional[str]=None, columnNameOfCorruptRecord: Optional[str]=None, multiLine: Optional[Union[bool, str]]=None, charToEscapeQuoteEscaping: Optional[str]=None, samplingRatio: Optional[Union[float, str]]=None, enforceSchema: Optional[Union[bool, str]]=None, emptyValue: Optional[str]=None, locale: Optional[str]=None, lineSep: Optional[str]=None, pathGlobFilter: Optional[Union[bool, str]]=None, recursiveFileLookup: Optional[Union[bool, str]]=None, modifiedBefore: Optional[Union[bool, str]]=None, modifiedAfter: Optional[Union[bool, str]]=None, unescapedQuoteHandling: Optional[str]=None) -> "DataFrame"

# COMMAND ----------

# MAGIC %md
# MAGIC Sample data of custs_1
# MAGIC 4000001,Kristina,Chung,55,Pilot<br>
# MAGIC 4000002,Paige,Chen,77,Teacher
# MAGIC
# MAGIC Sample data of custs_header<br>
# MAGIC custid,fname,lname,age,profession<br>
# MAGIC 4000001,Kristina,Chung,55,Pilot<br>
# MAGIC 4000002,Paige,Chen,77,Teacher

# COMMAND ----------

#1. Header Concepts (Either we have define the column names or we have to use the column names from the data)
#Important option is...
#By default it will use _c0,_c1..._cn it will apply as column headers, but we are asking spark to take the first row as header and not as a data?
csv_df1=spark.read.csv("/Volumes/workspace/wd36schema/ingestion_volume/source/custs_header",header=True)
print(csv_df1.printSchema())
csv_df1.show(2)
#csv_df1.write.csv("/Volumes/workspace/wd36schema2/volume1/folder1/outputdata")
#By default it will use _c0,_c1..._cn it will apply as column headers, if we use toDF(colnames) we can define our own headers..
csv_df2=spark.read.csv("/Volumes/workspace/wd36schema/ingestion_volume/source/custs_1").toDF("id","fname","lname","age","prof")
#csv_df1.show(2)#display with produce output in a dataframe format
print(csv_df2.printSchema())
csv_df2.show(2)

# COMMAND ----------

#2. Printing Schema (equivalent to describe table)
csv_df1.printSchema()
csv_df2.printSchema()

# COMMAND ----------

#3. Inferring Schema 
# (Performance Consideration: Use this function causiously because it scans the entire data by immediately evaluating and executing
# hence, not good for large data or not good to use on the predefined schema dataset)
#sample data
#4004979,Tara,Drake,32,
#4004980,Earl,Hahn,34,Human resources assistant
#4004981,Don,Jones,THIRTY SIX,Lawyer
csv_df1=spark.read.csv("/Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/directory_dropme/custs_2.txt",inferSchema=True).toDF("id","fname","lname","age","prof")
csv_df1.where("id in (4004979,4004981)").show(2)
csv_df1.printSchema()

# COMMAND ----------

# MAGIC %md
# MAGIC Performance Importance: Though inferSchema has to be used causiously, we can improve performance by using an option to reduce the data scanned for large data..

# COMMAND ----------

csv_df1=spark.read.csv("/Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/directory_dropme/custs_2.txt",inferSchema=True,samplingRatio=.10).toDF("id","fname","lname","age","prof")
csv_df1.where("id in (4004979,4004981)").show(2)
csv_df1.printSchema()

# COMMAND ----------

#4. Using delimiter or seperator option
csv_df1=spark.read.csv("dbfs:///Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/directory_dropme/cust_other_del.txt",header=True,sep='~')
csv_df1.show(2)
csv_df1.printSchema()

# COMMAND ----------

#5. Using different options to create dataframe with csv and other module... (2 methodologies (spark.read.inbuiltfunction or spark.read.format(anyfunction).load("path")) with 3 ways of creating dataframes (pass parameters to the csv()/option/options))
csv_df1=spark.read.csv("dbfs:///Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/directory_dropme/cust_other_del.txt",inferSchema=True,header=True,sep='~')
csv_df1.show(2)
#or another way of creating dataframe (from any sources whether builtin or external)...
#option can be used for 1 or 2 option...
csv_df2=spark.read.option("header","True").option("inferSchema","true").option("sep","~").format("csv").load("dbfs:///Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/directory_dropme/cust_other_del.txt")
csv_df2.show(2)
#options can be used for multiple options in one function as a parameter...
csv_df3=spark.read.options(header="True",inferSchema="true",sep="~").format("csv").load("dbfs:///Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/directory_dropme/cust_other_del.txt")
csv_df3.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generic way of read and load data into dataframe using fundamental options from built in sources (csv/orc/parquet/xml/json/table) (inferschema, header, sep)

# COMMAND ----------

csv_df1=spark.read.csv("dbfs:///Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/directory_dropme/cust_other_del.txt",inferSchema=True,header=True,sep='~')
csv_df1.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Generic way of read and load data into dataframe using extended options from external sources (bigquery/redshift/athena/synapse) (tmpfolder, access controls)

# COMMAND ----------

#options can be used for multiple options in one function as a parameter...
csv_df3=spark.read.options(header="True",inferSchema="true",sep="~").format("csv").load("dbfs:///Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/directory_dropme/cust_other_del.txt")
csv_df3.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading data from multiple files & Multiple Path (We still have few more options)

# COMMAND ----------

csv_df1=spark.read.csv("dbfs:///Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/directory_dropme/cust*",inferSchema=True,header=True,sep='~')
print(csv_df1.count())

# COMMAND ----------

csv_df1=spark.read.csv(path=["dbfs:///Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/directory_dropme/cust*","/Volumes/workspace/wd36schema/ingestion_volume/source/custs_1"],inferSchema=True,header=True,sep=',')
print(csv_df1.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ####Requirement: I am getting data from different source systems of different regions (NY, TX, CA) into different landing pad (locations), how to access this data?

# COMMAND ----------

df_multiple_sources=spark.read.csv(path=["/Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/sourcedata/NY","dbfs:///Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/sourcedata/TX"],inferSchema=True,header=True,sep=',',pathGlobFilter="custs_header_*",recursiveFileLookup=True)
#.toDF("cid","fn","ln","a","p")
print(df_multiple_sources.count())
df_multiple_sources.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Provide schema with SQL String or programatically (very very important)
# MAGIC [PySpark SQL Datatypes](https://spark.apache.org/docs/latest/sql-ref-datatypes.html) <br>
# MAGIC [Data Types](https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/data_types.html)<br>
# MAGIC ###To provide schema (columname & datatype), what are the 2 basic options available that we learned so far ? inferSchema/toDF<br>
# MAGIC ###We are going to learn additionally 2 more options to handle schema (colname & datatype)?<br>
# MAGIC ###1. Using simple string format of define schema.<br>
# MAGIC ###IMPORTANT: 2. Using structure type to define schema.<br>

# COMMAND ----------

#By default it will use _c0,_c1..._cn it will apply as column headers, if we use toDF(colnames) we can define our own headers..

csv_df1=spark.read.csv("/Volumes/workspace/wd36schema/ingestion_volume/source/custs_1").toDF("id","fname","lname","age","prof")
print(csv_df1.printSchema())
csv_df1=spark.read.csv("/Volumes/workspace/wd36schema/ingestion_volume/source/custs_1",inferSchema=True).toDF("id","fname","lname","age","prof")
print(csv_df1.printSchema())

#1. Using simple string format of define custom simple schema.
str_struct="id integer,fname string,lname string,age integer,prof string"
csv_df1=spark.read.schema(str_struct).csv("/Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/sourcedata/custs_header_1")
print(csv_df1.printSchema())
csv_df1.show(2)

#2. Important part - Using structure type to define custom complex schema.
#4000001,Kristina,Chung,55,Pilot
#pattern - 
#import the types library based classes..
# define_structure=StructType([StructField("colname",DataType(),True),StructField("colname",DataType(),True)...])
from pyspark.sql.types import StructType,StructField,StringType,IntegerType
custom_schema=StructType([StructField("id",IntegerType(),False),StructField("fname",StringType(),True),StructField("lname",StringType(),True),StructField("age",IntegerType(),True),StructField("prof",StringType())])
csv_df1=spark.read.schema(custom_schema).csv("/Volumes/catalog1_dropme/schema1_dropme/volume1_dropme/sourcedata/custs_header_1")
print(csv_df1.printSchema())
csv_df1.show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC what will happen if one of the file have an extra column, rest all the columns are same?<br>
# MAGIC Is union operation performing here in dataframe? not directly, but we can do to answer above question.. unionByName (Schema evolution)
# MAGIC
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Reading data from other formats (Try the below usecases after completing the 3-Basic-WriteOps)

# COMMAND ----------

df1=spark.read.options(header="True",inferSchema="true",sep="~").csv("/Volumes/workspace/wd36schema/ingestion_volume/target/csvout")
df1.show(2)
display(df1.take(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Reading json data

# COMMAND ----------

dfjson1=spark.read.json("/Volumes/workspace/wd36schema/ingestion_volume/target/jsonout")
dfjson1.show(2)
display(dfjson1.take(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Reading xml data

# COMMAND ----------

dfxml1=spark.read.xml("/Volumes/workspace/wd36schema/ingestion_volume/target/xmlout",rowTag="cust")
dfxml1.show(2)
display(dfxml1.take(3))

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Reading serialized data (orc/parquet/delta)

# COMMAND ----------

print("orcdata")
dforc1=spark.read.orc("/Volumes/workspace/wd36schema/ingestion_volume/target/orcout")
dforc1.show(2)
display(dforc1.take(2))
print("parquetdata")
dfparquet1=spark.read.parquet("/Volumes/workspace/wd36schema/ingestion_volume/target/parquetout")
dfparquet1.show(2)
display(dfparquet1.take(2))
print("deltadata")
dfdelta1=spark.read.format("delta").load("/Volumes/workspace/wd36schema/ingestion_volume/target/deltaout")
dfdelta1.show(2)
display(dfdelta1.take(2))

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Reading delta (hive) table data (Lakehouse)

# COMMAND ----------

dftable1=spark.read.table("workspace.wd36schema.lh_custtbl")
dftable1.show(2)
display(dftable1.take(2))

# COMMAND ----------

# MAGIC %md
# MAGIC What are all the overall options we used in this notebook, for learning fundamental spark csv read operations?
# MAGIC 1. How to create/manage Catalog & Volume
# MAGIC 2. spark session
# MAGIC 3. How to create some sample data and push it to the volume/folder/file
# MAGIC 4. spark.read.csv operations & spark.read.format().load()
# MAGIC 5. Few of the important read options under csv such as header, sep, inferSchema, toDF.
# MAGIC 6. How to define custom schema/structure using mere string with colname & datatype or by using StructType([StructField(colname,DataType()...)]) (very important).
# MAGIC 7. Few additional options such as samplingRatio, recursiveFileLookup & pathGlobFilter for accessing folders and subfolders with some pattern of filenames
# MAGIC 8. How to Read data from different sources and different file formats
# MAGIC