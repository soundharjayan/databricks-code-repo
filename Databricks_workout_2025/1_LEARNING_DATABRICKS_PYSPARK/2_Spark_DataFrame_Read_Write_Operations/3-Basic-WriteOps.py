# Databricks notebook source
# MAGIC %md
# MAGIC #By Knowing this notebook, we can become an eligible "Data Egress Developer/Engineer"
# MAGIC ###We are writing data in Structured(csv), Semi Structured(JSON/XML), Serialized files (orc/parquet/delta) (Datalake)
# MAGIC ###Table (delta/hive) (Lakehouse) format

# COMMAND ----------

# MAGIC %md
# MAGIC ### Let's get some data we have already...

# COMMAND ----------

from spark.sql.session import SparkSession
spark=SparkSession.builder.appName("Spark DataFrames").getOrCreate()

# COMMAND ----------

#Extract
ingest_df1=spark.read.csv("/Volumes/workspace/wd36schema/ingestion_volume/source/custs_header",header=True,sep=',',inferSchema=True,samplingRatio=0.10)

#ingest_df1.write.format("delta").save("/Volumes/workspace/wd36schema/ingestion_volume/deltadata")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Writing the data in Builtin - different file formats & different targets (all targets in this world we can write the data also...)

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Writing in csv (structured data (2D data Table/Frames with rows and columns)) format with few basic options listed below (Schema (structure) Migration)
# MAGIC custid,fname,lname,age,profession -> custid~fname~lname~prof~age
# MAGIC - header
# MAGIC - sep
# MAGIC - mode

# COMMAND ----------

#We are performing schema migration from comma to tilde delimiter
ingest_df1.write.csv(path="/Volumes/workspace/wd36schema/ingestion_volume/target/csvout",sep='~',header=True,mode='overwrite')
#4 modes of writing - append,overwrite,ignore,error

# COMMAND ----------

#We are performing schema migration by applying some transformations (this is our bread and butter that we learn exclusively further)
#Transform
transformed_df=ingest_df1.select("custid","fname","lname","profession","age").withColumnRenamed("profession","prof")#DSL transformation (not for now...)
#Load
transformed_df.write.csv(path="/Volumes/workspace/wd36schema/ingestion_volume/target/csvout",sep='~',header=True,mode='overwrite',compression='gzip')

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Writing in json format with few basic options listed below
# MAGIC path<br>
# MAGIC mode
# MAGIC - We did a schema migration and data conversion from csv to json format (ie structued to semi structured format)
# MAGIC - json - we learn a lot subsequently (nested/hierarchical/complex/multiline...), 
# MAGIC - what is json - fundamentally it is a dictionary of dictionaries
# MAGIC - json - java script object notation
# MAGIC - Standard json format (can't be changed) - {"k1":"string value","k2":numbervalue,"k3":v2} where key has to be unique & enclosed in double quotes and value can be anything
# MAGIC - **when to go with json or benifits** - 
# MAGIC - a. If we have data in a semistructure format (with variable data format with dynamic schema)
# MAGIC - eg. {"custid":4000001,"profession":"Pilot","age":55,"city":"NY"}
# MAGIC -     {"custid":4000001,"fname":"Kristina","lname":"Chung","prof":"Pilot","age":"55"}
# MAGIC - b. columns/column names or the types or the order can be different
# MAGIC - c. json will be provided by the sources if the data is dynamic in nature (not sure about number or order of columns) or if the data is api response in nature.
# MAGIC - d. json is a efficient data format (serialized/encoded) for performing data exchange between applications via network & good for parsing also & good for object by object operations (row by row operation in realtime fashion eg. amazon click stream operations)
# MAGIC - e. json can be used to group or create hierarchy of data in a complex or in a nested format eg. https://randomuser.me/api/

# COMMAND ----------

#Data Conversion/Schema Migration from Structured to SemiStructured format..
ingest_df1.write.json(path="/Volumes/workspace/wd36schema/ingestion_volume/target/jsonout",mode='append')
#Structured -> SemiStruct...
#custid,fname,lname,age,profession -> {"custid":4000001,"fname":"Kristina","lname":"Chung","prof":"Pilot","age":55}

# COMMAND ----------

# MAGIC %md
# MAGIC ####3.Serialization (encoding in a more optimized fashion) & Deserialization File formats (Binary/Brainy File formats)
# MAGIC **Data Mechanics:**
# MAGIC 1. encoding/decoding(machine format) - converting the data from human readable format to machine understandable format for performant data transfer (eg. Network transfer of data will be encoded)
# MAGIC 2. *compression/uncompression(encoding+space+time) - shrinking the data in some format using some libraries (tradeoff between time and size) (eg. Compress before store or transfer) - snappy is a good compression tech used in bigdata platform
# MAGIC 3. encryption (encoding+security) - Addition to encoding, encryption add security hence data is (performant+secured) (using some algos - SHA/MD5/AES/DES/RSA/DSA..)
# MAGIC 4. *Serialization (applicable more for bigdata) - Serialization is encoding + performant by saving space + processing intelligent bigdata format - Fast, Compact, Interoperable, Extensible (additional configs), Scalable (cluster compute operations), Secured (binary format)..
# MAGIC 5. *masking - Encoding of data (in some other format not supposed to be machine format) which should not be allowed to decode (used for security purpose)
# MAGIC
# MAGIC What are the (builtin) serialized file formats we are going to learn?
# MAGIC orc
# MAGIC parquet
# MAGIC delta(databricks properatory)
# MAGIC
# MAGIC - We did a schema migration and data conversion from csv/json to serialized data format (ie structued to sturctured(internall binary unstructured) format)
# MAGIC - We learn/use a lot/heavily subsequently
# MAGIC - what is serialized - fundamentally they are intelligent/encoded/serialized/binary data formats applied with lot of optimization & space reduction strategies.. (encoded/compressed/intelligent)
# MAGIC - orc - optimized row column format (Columnar formats)
# MAGIC - parquet - tiled data format (Columnar formats)
# MAGIC - delta(databricks properatory) enriched parquet format - Delta (modified/changes) operations can be performed (ACID property (DML))
# MAGIC - format - serialized/encoded , we can't see with mere eyes, only some library is used deserialized/decoded data can be accessed as structured data
# MAGIC - **when to go with serialized or benifits** - 
# MAGIC - a. For storage benifits for eg. orc will save 65+% of space for eg. if i store 1gb data it occupy 350mb space, with compression (snappy) it can improved more...
# MAGIC - b. For processing optimization. Orc/parquet/delta will provide the required data alone if you query using Pushdown optimization .
# MAGIC - c. Interoperability feature - this data format can be understandable in multiple environments for eg. bigquery can parse this data.
# MAGIC - d. Secured
# MAGIC - **In the projects/environments when to use what fileformats - we learn in detail later...
# MAGIC | Format  | Schema Type              | Storage Efficiency | Analytics/Transformation Performance | Updates Supported |
# MAGIC |--------|--------------------------|--------------------|-----------------------|------------------|
# MAGIC | CSV    | Structured               | Low                | Slow                  | No               |
# MAGIC | JSON   | Semi-structured           | Low                | Slow                  | No               |
# MAGIC | ORC    | Structured / Striped      | High               | Fast                  | Limited          |
# MAGIC | Parquet| Structured / Nested       | High               | Very Fast             | Limited          |
# MAGIC | Delta  | Structured / Evolving     | High               | Very Fast             | Highly           |
# MAGIC | XML    | Semi-structured           | Low                | Slow                  | No               |

# COMMAND ----------

ingest_df1.write.orc(path="/Volumes/workspace/wd36schema/ingestion_volume/target/orcout",mode='overwrite',compression='zlib')#by default orc/parquet uses snappy compression
spark.read.orc("/Volumes/workspace/wd36schema/ingestion_volume/target/orcout").show(2)#uncompression + deserialization

# COMMAND ----------

#Orc/Parquet follows WORM feature (Write Once Read Many)
ingest_df1.write.mode("overwrite").option("compression","gzip").option("compression","snappy").parquet(path="/Volumes/workspace/wd36schema/ingestion_volume/target/parquetout")#by default orc/parquet uses snappy compression
spark.read.parquet("/Volumes/workspace/wd36schema/ingestion_volume/target/parquetout").show(2)#uncompression + deserialization

# COMMAND ----------

#Delta follows WMRM feature (Write Many Read Many) - We did Delta Lake creation (Datalake + Delta file format)
ingest_df1.write.format("delta").save("/Volumes/workspace/wd36schema/ingestion_volume/target/deltaout",mode='overwrite')
spark.read.format("delta").load("/Volumes/workspace/wd36schema/ingestion_volume/target/deltaout").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####4.Table Load Operations - Building LAKEHOUSE ON TOP OF DATALAKE
# MAGIC Can we do SQL operations directly on the tables like a database or datawarehouse? or Can we build a Lakehouse in Databricks?
# MAGIC - We learn/use a lot/heavily subsequently, 
# MAGIC - what is Lakehouse - A SQL/Datawarehouse/Query layer on top of the Datalake is called Lakehouse
# MAGIC - We have different lakehouses which we are going to learn further - 
# MAGIC 1. delta tables (lakehouse) in databricks
# MAGIC 2. hive in onprem
# MAGIC 3. bigquery in GCP
# MAGIC 4. synapse in azure
# MAGIC 5. athena in aws
# MAGIC - **when to go with lakehouse** - 
# MAGIC - a. Transformation
# MAGIC - b. Analysis/Analytics
# MAGIC - c. AI/BI
# MAGIC - d. Literally we are going to learn SQL & Advanced SQL

# COMMAND ----------

#We are building delta tables in databricks (we are building hive tables in onprem/we are building bq tables in gcp...)
#saveastable (named notation/named arguments)
#Table
#cid,prof,age,fname,lname
#mapping
#cid,prof,age,fname,lname
ingest_df1.write.saveAsTable("workspace.wd36schema.lh_custtbl",mode='overwrite')
#display(spark.sql("show create table workspace.wd36schema.lh_custtbl"))

# COMMAND ----------

#1. insertinto function can be used as like saveAstable with few differences
#a. it works only if the target table exist
#b. it works by creating insert statements in the behind(not bulk load), hence it is slow, hence we have use for small dataset (safely only if table exists)
#c. it will load the data from the dataframe by using position, not by using name..
#insertInto (positional notation/positional arguments)
#Table
#cid,prof,age,fname,lname
#mapping.
#cid,fname,lname,age,prof
ingest_df1.write.insertInto("workspace.wd36schema.lh_custtbl",overwrite=True)

# COMMAND ----------

ingest_df1.write.format("delta").save("location")

# COMMAND ----------

#I am using spark engine to pull the data from the lakehouse table backed by dbfs (s3) (datalake) where data in delta format(deltalake) 
display(spark.sql("select * from workspace.wd36schema.lh_custtbl"))#sparkengine+lakehouse+datalake(deltalake)

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. XML Format - Semi structured data format (most of the json features can be applied in xml also, but in DE world not so famous like json)
# MAGIC - Used rarely on demand (by certain target/source systems eg. mainframes)
# MAGIC - Can be related with json, but not so much efficient like json
# MAGIC - Databricks provides xml as a inbuild function

# COMMAND ----------

ingest_df1.write.xml("/Volumes/workspace/wd36schema/ingestion_volume/target/xmlout",mode="ignore",rowTag="cust")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Modes in Writing
# MAGIC 1. **Append** - Adds the new data to the existing data. It does not overwrite anything.
# MAGIC 2. **Overwrite** - Replaces the existing data entirely at the destination.
# MAGIC 3. **ErrorIfexist**(default) - Throws an error if data already exists at the destination.
# MAGIC 4. **Ignore** - Skips the write operation if data already exists at the destination.

# COMMAND ----------

# MAGIC %md
# MAGIC ####What are all the overall functions/options we used in this notebook, for learning fundamental spark dataframe WRITE operations in different formats and targets?
# MAGIC 1. We learned dozen of functions (out of 18 functions) in the write module with minimum options...
# MAGIC 2. Functions we learned are (Datalake functions - csv/json/xml/orc/parquet+delta), (Lakehouse functions - saveAsTable/insertInto), (additional options - format/save/option/options/mode).
# MAGIC 3. We have few more performance optimization/advanced options available (jdbc (we learn this soon in the name of foreign catalog), partitionBy,ClusterBy,BucketBy,SortBy,text)
# MAGIC 4. Few of the important read options under csv such as header, sep, mode(append/overwrite/error/ignore), toDF.
# MAGIC 5. Few additional options such as compression, different file formats...