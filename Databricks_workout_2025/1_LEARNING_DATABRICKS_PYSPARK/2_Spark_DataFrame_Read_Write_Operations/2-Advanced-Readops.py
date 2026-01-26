# Databricks notebook source
# MAGIC %md
# MAGIC Try to use this sales data with atleast very important and important options...
# MAGIC https://drive.google.com/file/d/1MZI4XIofL-0QpMIr9sFODSKVkexrSZ1A/view?usp=drive_link

# COMMAND ----------

# MAGIC %md
# MAGIC ###1. CSV Advanced Features - 
# MAGIC ######Very Important - path: PathOrPaths, schema: Optional[Union[StructType, str]]=None, sep: Optional[str]=None,header: Optional[Union[bool, str]]=None, inferSchema: Optional[Union[bool, str]]=None, 
# MAGIC ######Important - mode: Optional[str]=None, columnNameOfCorruptRecord: Optional[str]=None,  quote: Optional[str]=None, escape: Optional[str]=None, 
# MAGIC Not Important but good to know once - encoding: Optional[str]=None, comment: Optional[str]=None,ignoreLeadingWhiteSpace: Optional[Union[bool, str]]=None, ignoreTrailingWhiteSpace: Optional[Union[bool, str]]=None, nullValue: Optional[str]=None, nanValue: Optional[str]=None, positiveInf: Optional[str]=None, negativeInf: Optional[str]=None, dateFormat: Optional[str]=None, timestampFormat: Optional[str]=None, maxColumns: Optional[Union[int, str]]=None, maxCharsPerColumn: Optional[Union[int, str]]=None, maxMalformedLogPerPartition: Optional[Union[int, str]]=None,   multiLine: Optional[Union[bool, str]]=None, charToEscapeQuoteEscaping: Optional[str]=None, samplingRatio: Optional[Union[float, str]]=None, enforceSchema: Optional[Union[bool, str]]=None, emptyValue: Optional[str]=None, locale: Optional[str]=None, lineSep: Optional[str]=None, pathGlobFilter: Optional[Union[bool, str]]=None, recursiveFileLookup: Optional[Union[bool, str]]=None, modifiedBefore: Optional[Union[bool, str]]=None, modifiedAfter: Optional[Union[bool, str]]=None, unescapedQuoteHandling: Optional[str]=None) -> "DataFrame"

# COMMAND ----------

# MAGIC %md
# MAGIC #### A. Options for handling quotes & Escape
# MAGIC
# MAGIC id,name,remarks
# MAGIC 1,'Ramesh, K.P','Good performer'
# MAGIC 2,'Manoj','Needs ~'special~' attention'

# COMMAND ----------

#When to go for quote: If the data is having delimiter in it..
#When to go for escape: If the data is having quote in it...
struct1="custid int,name string,age int,corrupt_record string"
df1=spark.read.schema(struct1).csv("/Volumes/workspace/default/volumewd36/malformeddata1.txt",header=False,sep=',',mode='permissive',comment='#',columnNameOfCorruptRecord="corrupt_record",quote="'",escape="|")
df1.show(10,False)

# COMMAND ----------

# MAGIC %md
# MAGIC #### B. Comments, Multi line, leading and trailing whitespace handling, null and nan handling

# COMMAND ----------

from pyspark.sql.functions import *
struct1="custid int,name string,height float,joindt date,age string"
df1=spark.read.schema(struct1).csv("/Volumes/workspace/default/volumewd36/malformeddata2.txt",header=False,mode='permissive'
                                   ,multiLine=True,quote="'",ignoreLeadingWhiteSpace=True,ignoreTrailingWhiteSpace=True,
                                   nullValue='na',nanValue=-1,maxCharsPerColumn='100',modifiedAfter='2025-12-19',dateFormat="yyyy-dd-MM")
display(df1)

# COMMAND ----------

# MAGIC %md
# MAGIC #### C. Read modes in csv (Important feature)
# MAGIC If any data challenges (malformed data) such as format issue/column numbers (lesser/more than expected) issue etc.,
# MAGIC ### There are 3 typical read modes and the default read mode is permissive.
# MAGIC ##### 1. permissive — All fields are set to null and corrupted records are placed in a string column called _corrupt_record
# MAGIC ##### 	2. dropMalformed — Drops all rows containing corrupt records.
# MAGIC ##### 3. failFast — Fails when corrupt records are encountered.

# COMMAND ----------

#We learned about few important features mode, columnNameOfCorruptRecord, Quote, Comment
#Question - Corrupt_record column consume more memory because it capturing all the column values(incorrect) in one column. ? Useful for doing RCA (Root Cause Analysis/Debugging)
struct1="custid int,name string,age int,corrupt_record string"
df1=spark.read.schema(struct1).csv("/Volumes/workspace/default/volumewd36/malformeddata.txt",header=False,sep=',',mode='permissive',comment='#',columnNameOfCorruptRecord="corrupt_record",quote="'")
df1.show(10)
df1=spark.read.schema(struct1).csv("/Volumes/workspace/default/volumewd36/malformeddata.txt",header=False,sep=',',mode='dropMalformed',comment='#',columnNameOfCorruptRecord="corrupt_record",quote="'")
df1.show(10)
df1=spark.read.schema(struct1).csv("/Volumes/workspace/default/volumewd36/malformeddata.txt",header=False,sep=',',mode='failFast',comment='#',columnNameOfCorruptRecord="corrupt_record",quote="'")
df1.show(10)
#df1.filter("corrupt_record is not null").write.csv("/Volumes/workspace/default/volumewd36/rejecteddata")
#spark.read.csv("/Volumes/workspace/default/volumewd36/rejecteddata").show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###2. JSON Advanced Features - 
# MAGIC **Very Important** - path,schema,columnNameOfCorruptRecord,dateFormat,timestampFormat,multiLine,pathGlobFilter,recursiveFileLookup<br>
# MAGIC No header, No inferSchema, No sep in json...<br>
# MAGIC **Important** - primitivesAsString(don't do inferSchema), prefersDecimal, allowComments, allowUnquotedFieldNames, `allowSingleQuotes`, lineSep, samplingRatio, dropFieldIfAllNull, modifiedBefore, modifiedAfter, useUnsafeRow(This is performance optimization when the data is loaded into spark memory) <br>
# MAGIC **Not Important** (just try to know once for all) - allowNumericLeadingZero, allowBackslashEscapingAnyCharacter, allowUnquotedControlChars, encoding, locale, allowNonNumericNumbers<br>

# COMMAND ----------

#https://spark.apache.org/docs/latest/sql-data-sources-json.html#data-source-option
#primitivesAsString = inferSchema=False
dfjson1=spark.read.json("/Volumes/workspace/default/volumewd36/",primitivesAsString=True)
dfjson1.printSchema()
#prefersDecimal
dfjson1=spark.read.json("/Volumes/workspace/default/volumewd36/",prefersDecimal=True)
dfjson1.printSchema()

# COMMAND ----------

#If we don't define structure, it will infer the schema
str1="id int,name string,amt float,dop date,corruptrecord string"
dfjson1=spark.read.schema(str1).json("/Volumes/workspace/default/volumewd36/",samplingRatio=1,allowUnquotedFieldNames=True,allowSingleQuotes=True,modifiedAfter='2025-12-22',dateFormat='yyyy-dd-MM',columnNameOfCorruptRecord="corruptrecord",pathGlobFilter="simple_json.tx*",recursiveFileLookup=True)
dfjson1.printSchema()
dfjson1.show(10,False)

# COMMAND ----------

str1="id int,name string,amt float,dop date,corruptrecord string"
dfjson1=spark.read.schema(str1).json("/Volumes/workspace/default/volumewd36/",allowUnquotedFieldNames=True,allowSingleQuotes=True,modifiedAfter='2025-12-22',dateFormat='yyyy-dd-MM',columnNameOfCorruptRecord="corruptrecord",pathGlobFilter="simple_json2.tx*",recursiveFileLookup=True,allowComments=True,lineSep='~',useUnsafeRow=True)
dfjson1.printSchema()
dfjson1.show(10,False)

# COMMAND ----------

str1="id int,name string,amt float,dop date,corruptrecord string"
dfjson1=spark.read.schema(str1).json("/Volumes/workspace/default/volumewd36/simple_json_multiline3.txt",dateFormat='yyyy-dd-MM',columnNameOfCorruptRecord="corruptrecord",
                                     allowComments=True,multiLine=True)
dfjson1.printSchema()
dfjson1.show(10,False)

# COMMAND ----------

# MAGIC %md
# MAGIC ###3. Serialized data Advanced Feature - orc, parquet/delta (very very important & we learn indepth)
# MAGIC - PathOrPaths
# MAGIC - **mergeSchema** - Important interview property (make it proactive/make it driven in the interview) SCHEMA EVOLUTION
# MAGIC - pathGlobFilter
# MAGIC - recursiveFileLookup
# MAGIC - modifiedBefore
# MAGIC - modifiedAfter
# MAGIC Problem statement:
# MAGIC Source is sending data in any way they want...
# MAGIC Day1/source1- 5 cols
# MAGIC Day2/source2 - 7 Cols
# MAGIC
# MAGIC 1. I am reading the dataframe in csv/json...
# MAGIC 2. Writing into a orc/parquet format in a single location.
# MAGIC 3. Reading data in a orc/parquet format using mergeSchema option.

# COMMAND ----------

#Case study: If the source (external) is sending data to us, if our consumer (Datascience/Dataanalytics) is directly communicated with our source system and asked them to propogate more/less attributes/features without the knowledge of the Dataengineering team? Here we have implement the strategy of Schema Evolution using option mergeSchema
#Story building for interview: We get product data from source which get evolved on a frequent basis for eg. product data originally sent without gendra, costprice, purchaseprice, profit/loss metrics, demant information...
#Steps to follow:
#1. Collect the data as it is from the source
#2. Convert into orc/parquet format and write to the target by appending the data on a regular interval
#3. Read the data from the target and do the schema evolution and get the evolved dataframe created...

# COMMAND ----------

#1. Collect the data as it is from the source
#2. Convert into orc/parquet format and write to the target by appending the data on a regular interval
day1df=spark.read.csv("/Volumes/workspace/default/volumewd36/day1.txt",header=True,inferSchema=True)
day1df.write.orc("/Volumes/workspace/wd36schema/ingestion_volume/target/orcoutput",mode='append')
day2df=spark.read.csv("/Volumes/workspace/default/volumewd36/day2.txt",header=True,inferSchema=True)
day2df.write.orc("/Volumes/workspace/wd36schema/ingestion_volume/target/orcoutput",mode='append')
day3df=spark.read.csv("/Volumes/workspace/default/volumewd36/day3.txt",header=True,inferSchema=True)
day3df.write.orc("/Volumes/workspace/wd36schema/ingestion_volume/target/orcoutput",mode='append')

# COMMAND ----------

#3. Read the data from the target and do the schema evolution and get the evolved dataframe created...
post_day3=spark.read.orc("/Volumes/workspace/wd36schema/ingestion_volume/target/orcoutput/",mergeSchema=True)
display(post_day3)

# COMMAND ----------

#1. Collect the data as it is from the source
#2. Convert into orc/parquet format and write to the target by appending the data on a regular interval
#Here we use inferSchema without an option and We can't use structuretype, because schema is evolving...
day1df=spark.read.csv("/Volumes/workspace/default/volumewd36/day1.txt",header=True,inferSchema=True)
day1df.write.parquet("/Volumes/workspace/wd36schema/ingestion_volume/target/parquetoutput",mode='append')
day2df=spark.read.csv("/Volumes/workspace/default/volumewd36/day2.txt",header=True,inferSchema=True)
day2df.write.parquet("/Volumes/workspace/wd36schema/ingestion_volume/target/parquetoutput",mode='append')
day3df=spark.read.csv("/Volumes/workspace/default/volumewd36/day3.txt",header=True,inferSchema=True)
day3df.write.parquet("/Volumes/workspace/wd36schema/ingestion_volume/target/parquetoutput",mode='append')

# COMMAND ----------

#3. Read the data from the target and do the schema evolution and get the evolved dataframe created...
post_day3=spark.read.parquet("/Volumes/workspace/wd36schema/ingestion_volume/target/parquetoutput/",mergeSchema=True)
display(post_day3)

# COMMAND ----------

# MAGIC %md
# MAGIC ###4. Reading data from other formats 

# COMMAND ----------

# MAGIC %md
# MAGIC ####1. Reading csv data

# COMMAND ----------

spark.read.csv("/Volumes/workspace/wd36schema/ingestion_volume/target/csvout").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####2. Reading json data

# COMMAND ----------

spark.read.json("/Volumes/workspace/wd36schema/ingestion_volume/target/jsonout").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####3. Reading xml data

# COMMAND ----------

spark.read.xml("/Volumes/workspace/wd36schema/ingestion_volume/target/xmlout",rowTag="cust").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####4. Reading serialized data (orc/parquet/delta)

# COMMAND ----------

spark.read.orc("/Volumes/workspace/wd36schema/ingestion_volume/target/orcout").show(2)
spark.read.parquet("/Volumes/workspace/wd36schema/ingestion_volume/target/parquetout").show(2)
spark.read.format("delta").load("/Volumes/workspace/wd36schema/ingestion_volume/target/deltaout").show(2)

# COMMAND ----------

# MAGIC %md
# MAGIC ####5. Reading delta/hive table data - We will heavily learn this under Databricks (Spark)